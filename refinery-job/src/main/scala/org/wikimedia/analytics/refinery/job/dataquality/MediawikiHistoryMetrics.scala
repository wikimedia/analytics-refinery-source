package org.wikimedia.analytics.refinery.job.dataquality

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.amazon.deequ.analyzers.{ApproxCountDistinct, Size}
import com.amazon.deequ.checks.{Check, CheckLevel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.wikimedia.analytics.refinery.core.HivePartition
import org.wikimedia.analytics.refinery.spark.deequ.{DeequAnalyzersToDataQualityMetrics, DeequVerificationSuiteToDataQualityAlerts}
import org.wikimedia.analytics.refinery.tools.LogHelper
import org.wikimedia.analytics.refinery.tools.config._

import scala.collection.immutable.ListMap
import scala.util.{Failure, Success, Try}

object MediawikiHistoryMetrics extends LogHelper with ConfigHelper{
    case class Config(
        source_table: String = "wmf.mediawiki_history",
        wiki_table: String = "wmf.ingestion_wikis",
        partition_map: ListMap[String, String],
        previous_partition_map: ListMap[String, String],
        run_id: String,
        metric_table: Option[String] = None,
        alerts_table: Option[String] = None,
        alerts_output_path: Option[String] = None
    ) {
        val hive_table_partition: HivePartition = HivePartition(source_table, partition_map)
        val previous_hive_table_partition: HivePartition =
            HivePartition(source_table, previous_partition_map)
        val event_growth_ratio_threshold: Double = 0.05
    }

    object Config {
        val propertiesDoc: ListMap[String, String] = ListMap(
            "config_file <file1.properties,files2.properties>" ->
                """Comma separated list of paths to properties files.  These files parsed for
                  |for matching config parameters as defined here.""",
            "source_table" ->
                """Hive table to compute metrics on.""",
            "wiki_table" ->
              """Hive table to get list of ingestion wikis""",
            "partition_map" ->
                """Map of partition keys to values.
                  |These will be used to select the Hive partition on which to compute metrics.
                  |Should be provided as e.g. k1:v1,k2:v2,snapshot:2023-8
                  |""".stripMargin,
            "previous_partition_map" ->
                """Map of partition keys to values.
                  |This will be used to select the previous Hive partition on which to
                  |perform comparison.
                  |""".stripMargin,
            "run_id" ->
                """Id used to keep track of the run that compute specific metrics.""",
            "metrics_table" ->
                """Hive table name in which to write computed metrics.""",
            "alerts_table" ->
                """Hive table name in which to write computed alerts.""",
            "alerts_output_path" ->
                """HDFS path in which to write computed alerts."""
        )
        val usage: String =
            """
            Compute data quality metrics for MediaWiki-History snapshot.

            Example:
              spark3-submit \
              --class org.wikimedia.analytics.refinery.job.refine.MediawikiHistoryMetrics refinery-job.jar \
               # Configs can be read out of a config file
               --config_file                     /etc/refinery/mediawiki_history_metrics.properties \
               # Override and/or set other configs on the CLI
               --source_table           "wmf.mediawiki_history" \
               --partition_map                   "snapshot:2023-8" \
               --run_id          "mediawiki_history20210824"
            """

        def apply(args: Array[String]): Config = {
            val config = try{
                configureArgs[Config](args)
            } catch {
                case e: ConfigHelperException =>
                    log.fatal(e.getMessage + ". Aborting.")
                    sys.exit(1)
            }
            log.info("Loaded Mediawiki History Metrics config:\n" + prettyPrint(config))
            config
        }
    }

    private def onYarn(spark: SparkSession): Boolean = spark.conf.get("spark.master") == "yarn"

    private val repository: MetricsRepository =
        new InMemoryMetricsRepository

    def main(args: Array[String]): Unit = {
        if (args.contains("--help")) {
            println(help(Config.usage, Config.propertiesDoc))
            sys.exit(0)
        }
        val config = Config(args)
        // log in console when launched from airflow
        addConsoleLogAppender()
        val spark = SparkSession
          .builder()
          .enableHiveSupport()
          .getOrCreate()

        // if not running in yarn, make spark log level quieter.
        if (!onYarn(spark)) {
            spark.sparkContext.setLogLevel("WARN")
        }
        sys.exit(
            apply(spark)(config) match {
                case Success(_) => 0
                case Failure(_) => 1
            }
        )
    }

    private def generateAndStoreMWHistorySummaryMetrics(config: Config)
                                                       (denormalizedHistoryDf: DataFrame): Unit = {
        val spark = denormalizedHistoryDf.sparkSession

        import spark.implicits._
        val historyCountsMetricKey = ResultKey(System.currentTimeMillis(), Map("metric_type" -> "DenormalizedHistorySummaryStats"))

        //Generate summary stats for denormalized history
        AnalysisRunner
          .onData(denormalizedHistoryDf)
          .addAnalyzer(Size()) //count of mw history event
          .useRepository(repository)
          .saveOrAppendResult(historyCountsMetricKey)
          .run()

        val metrics = DeequAnalyzersToDataQualityMetrics(spark)(
            repository.load.get,
            config.hive_table_partition,
            config.run_id
        )

        val metricWriter = config.metric_table match {
            case Some(value) => metrics.write.iceberg.output(value)
            case None => metrics.write.iceberg
        }

        metricWriter.save()
    }

    private def verifyAndGenerateMediawikiHistoryAlerts(config: Config)
                                                       (newDenormalizedHistoryDf: DataFrame,
                                                        previousNormalizedHistoryDf: DataFrame,
                                                        wikiDf: DataFrame,
                                                       ): Unit = {
        val spark = newDenormalizedHistoryDf.sparkSession

        if (previousNormalizedHistoryDf.isEmpty) {
            log.warn(
                "Previous mediawiki_history partition data is empty. Skipping data quality comparisons."
            )
            return
        }
        // get growth threshold given a percentage growth allowed
        // TODO: We will need to optimize the anomaly check approach
        // TODO: by implementing Deequ FileSystem repo + anomaly detection capabilities.
        val previousSnapshotCount = previousNormalizedHistoryDf.count()
        val growth_threshold = previousSnapshotCount * config.event_growth_ratio_threshold
        //get count of wikis that should be ingested into the source table
        val wikisCount = wikiDf.count()
        // Data validation.
        val mwHistoryPageVerificationSuite = VerificationSuite()
          .onData(newDenormalizedHistoryDf)
          .addCheck(
              Check(CheckLevel.Error, "Data Integrity Checks")
                .hasSize(_ < (previousSnapshotCount + growth_threshold))
                .hasSize(_ > (previousSnapshotCount - growth_threshold))
                .isComplete("page_id")
                .satisfies("page_id > 0", "page_id is not 0")
                .hasUniqueness(List(
                    "wiki_db",
                    "event_entity",
                    "event_type",
                    "event_timestamp",
                    "event_user_text_historical",
                    "user_text_historical",
                    "page_id",
                    "revision_id"),
                    _ == 1.0
                )
                .hasNumberOfDistinctValues("wiki_db", _ == wikisCount)
          )
          .run()
          .checkResults

        val dqAlerts = DeequVerificationSuiteToDataQualityAlerts(spark)(mwHistoryPageVerificationSuite,
            config.hive_table_partition,
            config.run_id)

        // Alerts will be persisted only when the user
        // provides an output table or output path.
        if (config.alerts_table.nonEmpty) {
            dqAlerts.write.iceberg.output(config.alerts_table.get).save()
        }

        if (config.alerts_output_path.nonEmpty) {
            dqAlerts.write.text.output(config.alerts_output_path.get).save()
        }
    }

    def apply(spark: SparkSession)(config: Config): Try[Unit] = Try {
        try {
            import spark.implicits._
            //Extract mediawiki history data for snapshot
            val mWHistoryDf = spark.sql(
                s"""select *
                   |from ${config.hive_table_partition.tableName}
                   |where ${config.hive_table_partition.sqlPredicate}
                   |""".stripMargin
            )

            // TODO: This should be removed when Deequ anomaly check capabilities is implemented
            val previousMWHistoryDf = spark.sql(
                s"""select *
                   |from ${config.hive_table_partition.tableName}
                   |where ${config.previous_hive_table_partition.sqlPredicate}
                   |""".stripMargin
            )
            //Extract complete wiki list dataframe
            val wikiDf = spark.sql(
                s"""select distinct wiki_db
                   |from ${config.wiki_table}
                   |""".stripMargin
            )
            // Generate summary stats for mediawiki history
            generateAndStoreMWHistorySummaryMetrics(config)(mWHistoryDf)
            // Perform checks quality checks and send alert if any
            verifyAndGenerateMediawikiHistoryAlerts(config)(mWHistoryDf,previousMWHistoryDf,wikiDf)

            Success
        } catch {
            case e: Exception =>
                log.error(s"Failed to perform data quality check on ${config.hive_table_partition.table}", e)
                // Rethrow error after logging to force failure
                throw e
        }
    }
}
