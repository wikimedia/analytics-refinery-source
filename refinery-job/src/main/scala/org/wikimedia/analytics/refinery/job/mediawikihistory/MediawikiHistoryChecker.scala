package org.wikimedia.analytics.refinery.job.mediawikihistory

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized.DenormalizedHistoryChecker
import org.wikimedia.analytics.refinery.job.mediawikihistory.page.PageHistoryChecker
import org.wikimedia.analytics.refinery.job.mediawikihistory.reduced.ReducedHistoryChecker
import org.wikimedia.analytics.refinery.job.mediawikihistory.user.UserHistoryChecker
import scopt.OptionParser


/**
  * Job checking a mediawiki-history snapshot versus a previously generated one (expected correct).
  * This job can check either user_history, page_history, denormalized-history or reduced-history.
  *
  * There is one difference about the reduced-history run: instead of appending results to the output
  * folders as user,page or denormalized checkers do (since they are run altogether in the same oozie
  * job), the reduced-checkers overwrite its result folder (as it is run alone).
  *
  * The datasets are grouped by wiki and event entity and type (user create, user rename, page create,
  * page move, revision create, etc) and metrics are computed against the groups:
  *  - user_history:   Number of events, distinct number of user_ids, distinct number of user_texts
  *                    distinct number of bots, number of bot events, number of anonymous events
  *  - page_history:   Number of events, distinct number of page_ids and artificial page ids
  *                    (pages deleted), distinct number of page titles, number of redirects
  *  - denorm_history: Number of events, same metrics as for user_history and page_history (for user
  *                    and page event entities, as the dataset is denormalized), number of deleted
  *                    revisions, number reverted revisions, and number of reverts revisions.
  *  - reduced_history: Same as denorm_history, with additition of specific fields for digests events.
  *
  *  Each of those metrics is expected to grow from the previous snapshot to the next. Since we know
  *  there are unexpected cases (real data deletion, and page-id/page-artificial-id artifact because
  *  of page deletion), the lower accepted threshold for metrics-growth is set to -0.01%. The higher
  *  bound for metrics-growth is set to 100%, as small wikis can have a lot of variability in number
  *  of events. Another check is made on how many wiki/metrics have been reported in error: less than
  *  5% of the dataset rows in error is not reported.
  *  In order to better differenciate signal from noise, we concentrate on the wikis with he most
  *  edit activity, taking the top 50 by default.
  *
  *  usage example (using joal settings):
  *
  * sudo -u hdfs spark2-submit \
  *     --master yarn \
  *     --deploy-mode cluster \
  *     --executor-memory 8G \
  *     --driver-memory 4G \
  *     --executor-cores 4 \
  *     --conf spark.dynamicAllocation.maxExecutors=64 \
  *     --class org.wikimedia.analytics.refinery.job.mediawikihistory.MediawikiHistoryChecker \
  *     /home/joal/code/refinery-source/refinery-job/target/refinery-job-0.0.66-SNAPSHOT.jar \
  *     -i /wmf/data/wmf/mediawiki \
  *     -p 2018-04 \
  *     -n 2018-05 \
  *     --check-denormalized-history
  *
  */
object MediawikiHistoryChecker {

  @transient
  lazy val log: Logger = Logger.getLogger(this.getClass)

  /**
    * Case class handling job parameters
    */
  case class Params(mediawikiHistoryBasePath: String = "hdfs://analytics-hadoop/wmf/data/wmf/mediawiki",
                    previousSnapshot: String = "",  // Mandatory parameter, will be overwritten
                    newSnapshot: String = "",       // Mandatory parameter, will be overwritten
                    wikisToCheck: Int = 50,                      // Number of wikis to check
                    minEventsGrowthThreshold: Double = -0.01d,   // Set to -1% by default
                    maxEventsGrowthThreshold: Double = 1.0d,     // Set to 100% by default
                    wrongRowsRatioThreshold: Double = 0.05d,     // Set to 5% by default
                    checkUserHistory: Boolean = false,
                    checkPageHistory: Boolean = false,
                    checkDenormHistory: Boolean = false,
                    checkReducedHistory: Boolean = false,
                    debug: Boolean = false
                   )

  /**
    * CLI Option Parser for job parameters (fill-in Params case class)
    */
  val argsParser = new OptionParser[Params]("") {
    head("Mediawiki History Checker", "")
    note(
      """Check rebuilt history of a new snapshot against the previous one.
        | The previous one is assumed correct, and this job checks that the new snapshot has more
        | events than the previous one for user and revision, and that the number of page event
        | is coherent in term of creation, move and deletion. It also ensures that no wiki is missing
        | from the new snapshot.
        | Problematic rows are output at `mediawikiHistoryBasePath/history_check`
      """.stripMargin)
    help("help") text "Prints this usage text"

    opt[String]('i', "mediawiki-history-base-path") optional() valueName "<path>" action { (x, p) =>
      p.copy(mediawikiHistoryBasePath = if (x.endsWith("/")) x.dropRight(1) else x)
    } text "Base path to mediawiki history on hadoop.\n\tDefaults to hdfs://analytics-hadoop/wmf/data/wmf/mediawiki"

    opt[String]('p', "previous-snapshot") required() valueName "<snapshot>" action { (x, p) =>
      p.copy(previousSnapshot = x)
    } text "Previous snapshot partition to use as comparison base."

    opt[String]('n', "new-snapshot") required() valueName "<snapshot>" action { (x, p) =>
      p.copy(newSnapshot = x)
    } text "New snapshot partition to compare to previous."

    opt[Int]('c', "wikis-to-check") optional() valueName "<int>" action { (x, p) =>
      p.copy(wikisToCheck = x)
    } text ("The number of wikis to include in the list of checked metrics (by decreasing edit activity)."
           + "(Default to 100)")

    opt[Double]('m', "min-events-growth-threshold") optional() valueName "<double>" action { (x, p) =>
      p.copy(minEventsGrowthThreshold = x)
    } text ("The minimum growth below which events-difference-ratio raise an error (negative, as growth is expected)."
           + "(default to -0.01)")

    opt[Double]('x', "max-events-growth-threshold") optional() valueName "<double>" action { (x, p) =>
      p.copy(maxEventsGrowthThreshold = x)
    } text ("The maximum growth above which events-difference-ratio raise an error."
      + "(default to 1.0)")

    opt[Double]('r', "wrong-rows-ratio-threshold") optional() valueName "<double>" action { (x, p) =>
      p.copy(wrongRowsRatioThreshold = x)
    } text "The ratio above which error-rows-ratio raise an error (default to 0.05)"

    opt[Unit]("check-user-history").action( (_, c) =>
      c.copy(checkUserHistory = true)).text("Check User History")

    opt[Unit]("check-page-history").action( (_, c) =>
      c.copy(checkPageHistory = true)).text("Check Pages History")

    opt[Unit]("check-denormalized-history").action( (_, c) =>
      c.copy(checkDenormHistory = true)).text("Check Denormalized history")

    opt[Unit]("check-reduced-history").action( (_, c) =>
      c.copy(checkReducedHistory = true)).text("Check Reduced history")

    opt[Unit]("debug").action( (_, c) =>
      c.copy(debug = true) ).text("debug mode -- spark logs added to applicative logs (VERY verbose)")

  }

  /**
    * Instantiate spark session and MediawikiHistoryCheckerInternals
    * and launches the job through the created objects.
    *
    * @param args The command lines args to be parsed
    */
  def main(args: Array[String]) {
    argsParser.parse(args, Params()) match {
      case Some(params) =>

        // Logging levels settings
        val appLogLevel = Level.INFO
        val allLogLevel = if (params.debug) Level.INFO else Level.ERROR

        Logger.getRootLogger.setLevel(appLogLevel)
        Logger.getLogger("org.wikimedia").setLevel(appLogLevel)

        Logger.getLogger("akka").setLevel(allLogLevel)
        Logger.getLogger("com.databricks").setLevel(allLogLevel)
        Logger.getLogger("DataNucleus").setLevel(allLogLevel)
        Logger.getLogger("hive").setLevel(allLogLevel)
        Logger.getLogger("org.apache").setLevel(allLogLevel)
        Logger.getLogger("org.graphframes").setLevel(allLogLevel)
        Logger.getLogger("org.spark-project").setLevel(allLogLevel)


        log.info(
          s"""Starting MediawikiHistoryChecker with parameters:
            | mediawikiHistoryBasePath = ${params.mediawikiHistoryBasePath}
            | previousSnapshot = ${params.previousSnapshot}
            | newSnapshot = ${params.newSnapshot}
            | wikisToCheck = ${params.wikisToCheck}
            | minEventsGrowthThreshold = ${params.minEventsGrowthThreshold}
            | maxEventsGrowthThreshold = ${params.maxEventsGrowthThreshold}
            | wrongRowsRatioThreshold = ${params.wrongRowsRatioThreshold}
            | checkUserHistory = ${params.checkUserHistory}
            | checkPageHistory = ${params.checkPageHistory}
            | checkDenormHistory = ${params.checkDenormHistory}
            | checkReducedHistory = ${params.checkReducedHistory}
          """.stripMargin)

        val previousSnapshot = params.previousSnapshot
        val newSnapshot = params.newSnapshot

        // Spark setup
        val conf = new SparkConf()
          .setAppName(s"MediawikiHistoryChecker-$previousSnapshot-$newSnapshot")
          .set("spark.sql.parquet.compression.codec", "snappy")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.ui.killEnabled", "false") // Prevent errors in UI

        val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

        if (params.checkUserHistory) {
          new UserHistoryChecker(spark,
            params.mediawikiHistoryBasePath,
            previousSnapshot,
            newSnapshot,
            params.wikisToCheck,
            params.minEventsGrowthThreshold,
            params.maxEventsGrowthThreshold,
            params.wrongRowsRatioThreshold
          ).checkUserHistory()
        }

        if (params.checkPageHistory) {
          new PageHistoryChecker(spark,
            params.mediawikiHistoryBasePath,
            previousSnapshot,
            newSnapshot,
            params.wikisToCheck,
            params.minEventsGrowthThreshold,
            params.maxEventsGrowthThreshold,
            params.wrongRowsRatioThreshold
          ).checkPageHistory()
        }

        if (params.checkDenormHistory) {
          new DenormalizedHistoryChecker(spark,
            params.mediawikiHistoryBasePath,
            previousSnapshot,
            newSnapshot,
            params.wikisToCheck,
            params.minEventsGrowthThreshold,
            params.maxEventsGrowthThreshold,
            params.wrongRowsRatioThreshold
          ).checkDenormHistory()
        }

        if (params.checkReducedHistory) {
          new ReducedHistoryChecker(spark,
            params.mediawikiHistoryBasePath,
            previousSnapshot,
            newSnapshot,
            params.wikisToCheck,
            params.minEventsGrowthThreshold,
            params.maxEventsGrowthThreshold,
            params.wrongRowsRatioThreshold
          ).checkReducedHistory()
        }

      case None => sys.exit(1) // If args parsing fail (parser prints nice error)
    }
  }

}