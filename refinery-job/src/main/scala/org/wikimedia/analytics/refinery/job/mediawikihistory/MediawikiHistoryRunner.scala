package org.wikimedia.analytics.refinery.job.mediawikihistory

import org.apache.spark.sql.SaveMode
import org.wikimedia.analytics.refinery.job.mediawikihistory.sql.AllViewsRegistrar
import org.wikimedia.analytics.refinery.spark.utils.{StatsHelper, MapAccumulator}


/**
  * Entry point for the Mediawiki History spark job(s).
  * It allows to run 3 sub-jobs (separately or jointly):
  *  - users history reconstruction
  *    [[org.wikimedia.analytics.refinery.job.mediawikihistory.user.UserHistoryRunner.run]]
  *  - pages history reconstruction
  *    [[org.wikimedia.analytics.refinery.job.mediawikihistory.page.PageHistoryRunner.run]]
  *  - revisions and denormalization
  *    [[org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized.DenormalizedRunner.run]]
  *
  *  Users and pages history only need sqooped data,
  *  while revisions and denormalization needs sqooped
  *  data AND users and pages history.
  *
  * You should configure no less than 2Gb of RAM per core
  * for user and page jobs, and 8Gb for denormalize job
  * Extra memory overhead is also to be allocated
  * when running denormalize to cover for huge joins
  * (spark.yarn.executor.memoryOverhead=2048)
  *
  * Example launch command (customize to your own user):
  *
  * sudo -u analytics spark2-submit \
  *     --master yarn \
  *     --deploy-mode cluster \
  *     --executor-memory 32G \
  *     --driver-memory 32G \
  *     --executor-cores 4 \
  *     --conf spark.dynamicAllocation.maxExecutors=32 \
  *     --conf spark.executor.memoryOverhead=8192 \
  *     --class org.wikimedia.analytics.refinery.job.mediawikihistory.MediawikiHistoryRunner \
  *     /home/milimetric/refinery-source/refinery-job/target/refinery-job-0.0.91-SNAPSHOT.jar \
  *     -i /wmf/data/raw/mediawiki \
  *     -p /wmf/data/raw/mediawiki_private \
  *     -o /user/milimetric/wmf/data/wmf/mediawiki \
  *     -s 2019-04 \
  *     -w simplewiki,fawiki \
  *     -n 8
  */

object MediawikiHistoryRunner {

  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.SparkConf
  import org.apache.spark.sql.SparkSession
  import scopt.OptionParser
  import org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized._
  import org.wikimedia.analytics.refinery.job.mediawikihistory.page.{PageHistoryRunner, PageState, PageEvent}
  import org.wikimedia.analytics.refinery.job.mediawikihistory.user.{UserHistoryRunner, UserState, UserEvent}


  @transient
  lazy val log: Logger = Logger.getLogger(this.getClass)

  /**
    * Case class handling job parameters
    */
  case class Params(mediawikiBasePath: String = "hdfs://analytics-hadoop/wmf/data/raw/mediawiki",
                    mediawikiPrivateBasePath: String = "hdfs://analytics-hadoop/wmf/data/raw/mediawiki_private",
                    outputBasePath: String = "hdfs://analytics-hadoop/wmf/data/wmf/mediawiki",
                    wikiConstraint: Seq[String] = Seq.empty[String],
                    snapshot: Option[String] = None,
                    tmpPath: String = "hdfs://analytics-hadoop/tmp/mediawiki/history/checkpoints",
                    baseNumPartitions: Int = 64,
                    readerFormat: String = "avro",
                    debug: Boolean = false,
                    noStats: Boolean = false,
                    writeErrors: Boolean = false,
                    runUsersHistory: Boolean = false,
                    runPagesHistory: Boolean = false,
                    runDenormalize: Boolean = false,
                    runAll: Boolean = true
                   )

  /**
    * CLI Option Parser for job parameters (fill-in Params case class)
    */
  val argsParser = new OptionParser[Params]("") {
    head("Mediawiki History Runner", "")
    note("""Builds user history, page history, and join them with revision
            |history into a fully denormalized parquet table by default.
            |You can specify which sub-jobs you wish to run if you think
            |that `you gotta keep'em separated`.""".stripMargin)
    help("help") text "Prints this usage text"

    opt[String]('i', "mediawiki-base-path") optional() valueName "<path>" action { (x, p) =>
      p.copy(mediawikiBasePath = if (x.endsWith("/")) x.dropRight(1) else x)
    } text "Base path to mediawiki extracted data on hadoop.\n\tDefaults to hdfs://analytics-hadoop/wmf/data/raw/mediawiki"

    opt[String]('p', "mediawiki-private-base-path") optional() valueName "<path>" action { (x, p) =>
      p.copy(mediawikiPrivateBasePath = if (x.endsWith("/")) x.dropRight(1) else x)
    } text "Base path to mediawiki-private extracted data on hadoop.\n\tDefaults to hdfs://analytics-hadoop/wmf/data/raw/mediawiki_private"

    opt[String]('o', "output-base-path") optional() valueName "<path>" action { (x, p) =>
      p.copy(outputBasePath = if (x.endsWith("/")) x else x + "/")
    } text "Path to output directory base.\n\tDefaults to hdfs://analytics-hadoop/wmf/data/wmf/mediawiki"

    opt[String]('w', "wikis") optional() valueName "<wiki_db_1>,<wiki_db_2>..." action { (x, p) =>
      p.copy(wikiConstraint = x.split(",").map(_.toLowerCase))
    } validate { x =>
      val dbs = x.split(",").map(_.toLowerCase)
      if (dbs.exists(db => db.isEmpty || (! db.contains("wik"))))
        failure("Invalid wikis list")
      else
        success
      } text "wiki dbs to compute"

    opt[String]('s', "snapshot") optional() valueName "<snapshot>" action { (x, p) =>
      p.copy(snapshot = Some(x))
    } text "Snapshot partition to add to output directory (no partition if None).\n\tDefaults to None"

    opt[String]('t', "temporary-path") optional() valueName "<path>" action { (x, p) =>
      p.copy(tmpPath = if (x.endsWith("/")) x else x + "/")
    } text "Path to use as checkpoint directory for Spark (temporary data).\n\t" +
      "Defaults to hdfs://analytics-hadoop/tmp/mediawiki/history/checkpoints"

    opt[Int]('n', "base-num-partitions") optional() action { (x, p) =>
      p.copy(baseNumPartitions = x)
    } text "Base number of partitions to split jobs.\n\t" +
      "users history uses base-num-partitions\n\t" +
      "pages history uses base-num-partitions * 4.\n\t" +
      "Revisions denormalization uses base-num-partitions * 16.\n\tDefaults to 64"

    opt[String]('f', "table-format") optional() valueName "<format>" action { (x, p) =>
      // Use external avro reader as not yet defined by default in spark
      // Should be removed with spark 2.4 and up
      p.copy(readerFormat = "avro")
    } validate { x =>
      if (!Seq("avro", "parquet").contains(x))
        failure(s"Invalid format $x - Should be avro or parquet.")
      else
        success
    } text "Format of sqooped-table files (avro or parquet). Defaults to avro."

    opt[Unit]("debug").action( (_, c) =>
      c.copy(debug = true) ).text("debug mode -- spark logs added to applicative logs (VERY verbose)")

    opt[Unit]("no-stats").action( (_, c) =>
      c.copy(noStats = true) ).text("no-stats mode -- No statistics gathering along the process (lighter in memory)")

    opt[Unit]("write-errors").action( (_, c) =>
      c.copy(writeErrors = true) ).text("Write error rows in historyType_errors folders as csv files")

    opt[Unit]("users-history").action( (_, c) =>
      c.copy(runUsersHistory = true, runAll = false)).text("Run specific step(s) -- Users History")

    opt[Unit]("pages-history").action( (_, c) =>
      c.copy(runPagesHistory = true, runAll = false) ).text("Run specific step(s) -- Pages History")

    opt[Unit]("revisions-denormalize").action( (_, c) =>
      c.copy(runDenormalize = true, runAll = false) ).text("Run specific step(s) -- Revisions & denormalization")

  }

  /**
    * Does the actual job launch
    * @param args The command lines args to be parsed
    */
  def main(args: Array[String]) {
    argsParser.parse(args, Params()) match {
      case Some(params) =>

        // Parameter extraction for clarity
        val mediawikiBasePath = params.mediawikiBasePath
        val mediawikiPrivateBasePath = params.mediawikiPrivateBasePath
        val outputBasePath = params.outputBasePath

        val wikiConstraint = params.wikiConstraint
        val snapshot = params.snapshot
        val snapshotPartition = if (snapshot.isDefined) s"/snapshot=${snapshot.get}" else ""

        val baseNumPartitions = params.baseNumPartitions
        val usersNumPartitions = baseNumPartitions
        val pagesNumPartitions = baseNumPartitions * 4
        val revisionsNumPartitions = baseNumPartitions * 16

        val tmpPath = params.tmpPath
        val readerFormat = params.readerFormat
        val debug = params.debug
        val noStats = params.noStats
        val writeErrors = params.writeErrors
        val runUsersHistory = params.runAll || params.runUsersHistory
        val runPagesHistory = params.runAll || params.runPagesHistory
        val runDenormalize = params.runAll || params.runDenormalize

        // Logging levels settings
        val appLogLevel = Level.INFO
        val allLogLevel = if (debug) Level.INFO else Level.ERROR

        Logger.getRootLogger.setLevel(appLogLevel)
        Logger.getLogger("org.wikimedia").setLevel(appLogLevel)

        Logger.getLogger("akka").setLevel(allLogLevel)
        Logger.getLogger("com.databricks").setLevel(allLogLevel)
        Logger.getLogger("DataNucleus").setLevel(allLogLevel)
        Logger.getLogger("hive").setLevel(allLogLevel)
        Logger.getLogger("org.apache").setLevel(allLogLevel)
        Logger.getLogger("org.graphframes").setLevel(allLogLevel)
        Logger.getLogger("org.spark-project").setLevel(allLogLevel)

        // Paths preparation
        val namespacesPath = mediawikiBasePath + "/project_namespace_map" + snapshotPartition
        val baseDataPath = mediawikiBasePath + "/tables"
        val basePrivateDataPath = mediawikiPrivateBasePath + "/tables"

        val archiveDataPath = baseDataPath + "/archive" + snapshotPartition
        val changeTagDataPath = baseDataPath + "/change_tag" + snapshotPartition
        val changeTagDefDataPath = baseDataPath + "/change_tag_def" + snapshotPartition
        val loggingDataPath = baseDataPath + "/logging" + snapshotPartition
        val pageDataPath = baseDataPath +  "/page" + snapshotPartition
        val revisionDataPath = baseDataPath + "/revision" + snapshotPartition
        //TODO Uncomment the following and remove private when data will available from labs
        //val actorDataPath = baseDataPath + "/actor" + snapshotPartition
        //val commentDataPath = baseDataPath + "/comment" + snapshotPartition
        val userDataPath = baseDataPath + "/user" + snapshotPartition
        val userGroupsDataPath = baseDataPath + "/user_groups" + snapshotPartition

        val actorPrivateDataPath = basePrivateDataPath + "/actor" + snapshotPartition
        val commentPrivateDataPath = basePrivateDataPath + "/comment" + snapshotPartition

        val denormalizedHistoryPath = outputBasePath + "/history" + snapshotPartition
        val pageHistoryPath = outputBasePath + "/page_history" + snapshotPartition
        val userHistoryPath = outputBasePath + "/user_history" + snapshotPartition

        val denormalizedHistoryErrorsPath = if (writeErrors) Some(outputBasePath + "/history_errors" + snapshotPartition) else None
        val pageHistoryErrorsPath = if (writeErrors) Some(outputBasePath + "/page_history_errors" + snapshotPartition) else None
        val userHistoryErrorsPath = if (writeErrors) Some(outputBasePath + "/user_history_errors" + snapshotPartition) else None

        val statsPath = outputBasePath + "/history_stats" + snapshotPartition

        log.info(
          s"""
             |Starting MediawikiHistoryRunner with params:
             |  mediawiki-base-path:    $mediawikiBasePath
             |  output-base-path:       $outputBasePath
             |  wikis:                  $wikiConstraint
             |  snapshot:               $snapshot
             |  temporary-path:         $tmpPath
             |  num-partitions:         $baseNumPartitions
             |  table-format:           $readerFormat
             |  debug:                  $debug
             |  write-errors:           $writeErrors
             |  users-history:          $runUsersHistory
             |  pages-history:          $runPagesHistory
             |  revisions-denormalize:  $runDenormalize
               """.stripMargin
        )

        // Spark setup
        val conf = new SparkConf()
          .setAppName(s"MediawikiHistoryRunner-${params.snapshot.getOrElse("NoSnapshot")}")
          .set("spark.sql.parquet.compression.codec", "snappy")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryoserializer.buffer", "512k")
          .set("spark.ui.killEnabled", "false")  // Prevent errors in UI
          // MapAccumulator is memory-heavy for the driver and moves 'a lot' of data between
          // tasks and driver. Two things need to be confirgured:
          //  - number of stages and tasks kept in the driver for UI inspection
          //    Keeping a lot of them means keeping a lot of memory-heavy details
          //  - Maximum data exchanged between task and driver, as the maps can be big
          .set("spark.ui.retainedStage", "5")
          .set("spark.ui.retainedTasks", "100")
          .set("spark.driver.maxResultSize", "4g")
          // Bump default numbers for various settings to better fit the job (big scale)
          // See https://wikitech.wikimedia.org/wiki/Analytics/Systems/Cluster/Spark#Spark_tuning_for_big_jobs
          .set("spark.stage.maxConsecutiveAttempts", "10")
          .set("spark.rpc.io.serverTreads", "64")
          .set("spark.shuffle.file.buffer", "1MB")
          .set("spark.unsafe.sorter.spill.reader.buffer.size", "1MB")
          .set("spark.file.transferTo", "false")
          .set("spark.shuffle.unsafe.file.output.buffer", "5MB")
          .set("spark.io.compression.lz4.blockSize", "512KB")
          .set("spark.shuffle.service.index.cache.size", "2048")
          .set("spark.shuffle.registration.timeout", "2m")
          .set("spark.shuffle.registration.maxAttempts", "5")
          // Set kryo serialization registering classes
          .registerKryoClasses(Array(
            // Keys
            classOf[PartitionKey],
            classOf[StateKey],
            classOf[MediawikiEventKey],
            // MediawikiEvent
            classOf[MediawikiEventPageDetails],
            classOf[MediawikiEventUserDetails],
            classOf[MediawikiEventRevisionDetails],
            classOf[MediawikiEvent],
            // Page and user Event and State
            classOf[PageEvent],
            classOf[PageState],
            classOf[UserEvent],
            classOf[UserState]))
        val spark = SparkSession.builder().config(conf).getOrCreate()
        spark.sparkContext.setCheckpointDir(tmpPath)

        implicit def sumLongs(a: Long, b: Long) = a + b

        val statsAccumulator: Option[MapAccumulator[String, Long]] = {
          if (noStats) None
          else Some(new MapAccumulator[String, Long]())
        }
        statsAccumulator.foreach(statAcc => spark.sparkContext.register(statAcc, "statistics"))

        // Register precomputed complex views for namespaces, archive and revision
        new AllViewsRegistrar(
          spark,
          statsAccumulator,
          revisionsNumPartitions,
          wikiConstraint,
          readerFormat
        ).run(
          namespacesPath,
          actorPrivateDataPath,
          archiveDataPath,
          changeTagDataPath,
          changeTagDefDataPath,
          commentPrivateDataPath,
          loggingDataPath,
          pageDataPath,
          revisionDataPath,
          userDataPath,
          userGroupsDataPath
        )

        // Launch jobs as needed

        // User History
        if (runUsersHistory)
          new UserHistoryRunner(spark, statsAccumulator, usersNumPartitions).run(
            userHistoryPath,
            userHistoryErrorsPath
          )

        spark.sqlContext.clearCache()

        // Page history
        if (runPagesHistory)
          new PageHistoryRunner(spark, statsAccumulator, pagesNumPartitions).run(
            pageHistoryPath,
            pageHistoryErrorsPath
          )

        spark.sqlContext.clearCache()

        // Revisions and denormalization
        if (runDenormalize)
          new DenormalizedRunner(spark, statsAccumulator, revisionsNumPartitions, wikiConstraint).run(
            userHistoryPath,
            pageHistoryPath,
            denormalizedHistoryPath,
            denormalizedHistoryErrorsPath
          )

        //***********************************
        // Write stats
        //***********************************
        // Rename variables to prevent recursive binding
        val (sp, statsAcc) = (spark, statsAccumulator)

        new StatsHelper {
          override val spark: SparkSession = sp
          override val statsAccumulator: Option[MapAccumulator[String, Long]] = statsAcc
        }.statsDataframe.foreach(
          _.repartition(1).write
            .mode(SaveMode.Overwrite)
            .format("csv")
            .option("sep", "\t")
            .option("codec", "none") // Enforce no compression for stats
            .save(statsPath))
        log.info(s"Denormalized MW Events stats written")

        log.info("MediawikiHistoryRunner job done.")

      case None => sys.exit(1) // If args parsing fail (parser prints nice error)
    }
  }

}
