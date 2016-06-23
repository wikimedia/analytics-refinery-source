package org.wikimedia.analytics.refinery.job.mediawikihistory

import org.apache.spark.sql.SQLContext

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
  * Example launch command (using joal settings):
  *
  * sudo -u hdfs /home/joal/code/spark-1.6.3-bin-hadoop2.6/bin/spark-submit \
  *     --master yarn \
  *     --deploy-mode cluster \
  *     --executor-memory 8G \
  *     --driver-memory 4G \
  *     --executor-cores 1 \
  *     --conf spark.dynamicAllocation.enabled=true \
  *     --conf spark.shuffle.service.enabled=true \
  *     --conf spark.dynamicAllocation.maxExecutors=64 \
  *     --conf spark.yarn.executor.memoryOverhead=2048
  *     --class org.wikimedia.analytics.refinery.job.mediawikihistory.MediawikiHistoryRunner \
  *     /home/joal/code/refinery-source/refinery-job/target/refinery-job-0.0.39-SNAPSHOT.jar \
  *     -i /wmf/data/raw/mediawiki \
  *     -o /wmf/data/wmf/mediawiki \
  *     -s 2017-03
  */
object MediawikiHistoryRunner {

  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.{SparkConf, SparkContext}
  import scopt.OptionParser
  import org.wikimedia.analytics.refinery.job.mediawikihistory.page.PageHistoryRunner
  import org.wikimedia.analytics.refinery.job.mediawikihistory.user.UserHistoryRunner
  import org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized.DenormalizedRunner

  /**
    * Case class handling job parameters
    */
  case class Params(mediawikiBasePath: String = "hdfs://analytics-hadoop/wmf/data/raw/mediawiki",
                    outputBasePath: String = "hdfs://analytics-hadoop/wmf/data/wmf/mediawiki",
                    wikiConstraint: Seq[String] = Seq.empty[String],
                    snapshot: Option[String] = None,
                    tmpPath: String = "hdfs://analytics-hadoop/tmp/mediawiki/history/checkpoints",
                    numPartitions: Int = 128,
                    debug: Boolean = false,
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

    opt[String]('o', "output-base-path") optional() valueName "<path>" action { (x, p) =>
      p.copy(outputBasePath = if (x.endsWith("/")) x else x + "/")
    } text "Path to output directory base.\n\tDefaults to hdfs://analytics-hadoop/wmf/data/wmf/mediawiki"

    opt[String]('w', "wikis") optional() valueName "<wiki_db_1>,<wiki_db_2>..." action { (x, p) =>
      p.copy(wikiConstraint = x.split(",").map(_.toLowerCase))
    } validate { x =>
      val dbs = x.split(",").map(_.toLowerCase)
      if (dbs.filter(db => db.isEmpty || (! db.contains("wik"))).length > 0)
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

    opt[Int]('n', "num-partitions") optional() action { (x, p) =>
      p.copy(numPartitions = x)
    } text "Number of partitions to split users and pages datasets by.\n\t" +
      "Revisions dataset is split by 8 * num-partitions. Defaults to 64"

    opt[Unit]("debug").action( (_, c) =>
      c.copy(debug = true) ).text("debug mode -- spark logs added to applicative logs (VERY verbose)")

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
        val outputBasePath = params.outputBasePath

        val wikiConstraint = params.wikiConstraint
        val snapshotPartition = if (params.snapshot.isDefined) s"/snapshot=${params.snapshot.get}" else ""

        val numPartitions = params.numPartitions
        val tmpPath = params.tmpPath
        val debug = params.debug
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

        val archiveDataPath = baseDataPath + "/archive" + snapshotPartition
        val loggingDataPath = baseDataPath + "/logging" + snapshotPartition
        val pageDataPath = baseDataPath +  "/page" + snapshotPartition
        val revisionDataPath = baseDataPath + "/revision" + snapshotPartition
        val userDataPath = baseDataPath + "/user" + snapshotPartition
        val userGroupsDataPath = baseDataPath + "/user_groups" + snapshotPartition

        val denormalizedHistoryPath = outputBasePath + "/history" + snapshotPartition
        val pageHistoryPath = outputBasePath + "/page_history" + snapshotPartition
        val userHistoryPath = outputBasePath + "/user_history" + snapshotPartition


        // Spark setup
        val conf = new SparkConf().setAppName(s"MediawikiHistoryRunner-${params.snapshot.getOrElse("NoSnapshot")}")
        val sqlContext = new SQLContext(new SparkContext(conf))
        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
        sqlContext.sparkContext.setCheckpointDir(tmpPath)


        // Launch jobs as needed

        // User History
        if (runUsersHistory)
          new UserHistoryRunner(sqlContext).run(
            wikiConstraint,
            loggingDataPath,
            userDataPath,
            userGroupsDataPath,
            revisionDataPath,
            userHistoryPath,
            numPartitions
          )

        // Page history
        if (runPagesHistory)
          new PageHistoryRunner(sqlContext).run(
            wikiConstraint,
            loggingDataPath,
            pageDataPath,
            revisionDataPath,
            namespacesPath,
            pageHistoryPath,
            numPartitions
          )

        // Revisions and denormalization
        if (runDenormalize)
          new DenormalizedRunner(sqlContext).run(
            wikiConstraint,
            revisionDataPath,
            archiveDataPath,
            userHistoryPath,
            pageHistoryPath,
            denormalizedHistoryPath,
            numPartitions * 8
          )

      case None => sys.exit(1) // If args parsing fail (parser prints nice error)
    }
  }

}
