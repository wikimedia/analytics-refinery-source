package org.wikimedia.analytics.refinery.job.dataquality


import org.apache.spark.sql.{DataFrame, SparkSession}
import com.amazon.deequ.repository._
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import com.amazon.deequ.analyzers.{Maximum, Mean, Minimum, Size}
import com.amazon.deequ.analyzers.runners.AnalysisRunner
import org.wikimedia.analytics.refinery.core.HivePartition
import org.wikimedia.analytics.refinery.spark.deequ.{DeequAnalyzersToDataQualityMetrics, DeequVerificationSuiteToDataQualityAlerts}
import org.wikimedia.analytics.refinery.tools.config._
import org.wikimedia.analytics.refinery.tools.LogHelper

import scala.collection.immutable.ListMap
import scala.util.{Failure, Success, Try}

/**
 * Generate data quality metrics for wmf.webrequest_actor_metrics_hourly.
 */
object WebrequestActorMetrics extends DataQualityHelper with LogHelper with ConfigHelper {
    private final val SummaryStats: String =
        "ActorMetrics"


    case class Config(
                       table: String,
                       partition_map: ListMap[String, String],
                       run_id: String,
                       metrics_table: Option[String] = None,
                       alerts_table: Option[String] = None,
                       alerts_output_path: Option[String] = None
                     ) {
        val hive_partition: HivePartition = HivePartition(table, partition_map)
    }

    object Config {
        val propertiesDoc: ListMap[String, String] = ListMap(
            "config_file <file1.properties,files2.properties>" ->
              """Comma separated list of paths to properties files.  These files parsed for
                  |for matching config parameters as defined here.""",
            "table" ->
              """Fully qualified Hive table name on which to compute metrics""",
            "partition_map" ->
              """Map of partition keys to values.
                  |These will be used to select the Hive partition on which to compute metrics.
                  |Should be provided as e.g. k1:v1,year:2023,month:12,day:1:hour:0
                  |""".stripMargin,
            "run_id" ->
              """Used to keep track of runs that compute specific metrics.""",
            "metrics_table" ->
              """Fully qualified Hive table name in which to write computed metrics.""",
            "alerts_table" ->
              """Fully qualified Hive table name in which to write computed alerts.""",
            "alerts_output_path" ->
              """HDFS path in which to write computed alerts."""
        )

        val usage: String =
            """
                |Compute data quality metrics about a wmf.webrequest_actor_metrics_hourly  partition.
                |
                |Example:
                |  spark-submit --class org.wikimedia.analytics.refinery.job.refine.WebrequestActorMetrics refinery-job.jar \
                |   # read configs out of this file
                |   --config_file                     /etc/refinery/webrequest_actor_metrics.properties \
                |   # Override and/or set other configs on the CLI
                |   --table                           "test.dataquality_table" \
                |   --partition_map                   "year:2023,month:12,day:1,hour:0" \
                |   --run_id                          "1234567"
                |"""

        def apply(args: Array[String]): Config = {
            val config = try {
                configureArgs[Config](args)
            }
            catch {
                case e: ConfigHelperException => {
                    log.fatal(e.getMessage + ". Aborting.")
                    sys.exit(1)
                }
            }
            log.info("Loaded WebrequestActorMetrics config:\n" + prettyPrint(config))
            config
        }
    }

    private val repository: MetricsRepository =
        new InMemoryMetricsRepository // This metric is not stateful. No need to persist.

    def main(args: Array[String]): Unit = {
        if (args.contains("--help")) {
            println(help(Config.usage, Config.propertiesDoc))
            sys.exit(0)
        }

        val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

        // if not running in yarn, make spark log level quieter.
        if (!onYarn(spark)) {
            spark.sparkContext.setLogLevel("WARN")
        }

        val config = Config(args)

        // Call apply with spark and Config properties as parameters
        apply(spark)(config) match {
            case Success(_) => sys.exit(0)
            case Failure(error) => throw error // Sets exit code to 1.
        }
    }

    private def generateAndStoreMetrics(config: Config)
                                                 (webrequestDf: DataFrame): Unit = {
        val spark = webrequestDf.sparkSession

        import spark.implicits._
        val countsResultKey = mkResultKey(SummaryStats)


        // Generate summary stats (counts, sums).
        AnalysisRunner
          .onData(
              webrequestDf
                .select($"actor_signature",
                    $"pageview_count",
                    $"distinct_pages_visited_count",
                    $"user_agent_length")
          )
          .addAnalyzer(Size()) // number of actor signature
          .addAnalyzer(Maximum("pageview_count")) // TODO: add ApproxQuantile support in the refinery SerDes. See T362780.
          .addAnalyzer(Minimum("pageview_count"))
          .addAnalyzer(Mean("pageview_count"))
          .addAnalyzer(Maximum("distinct_pages_visited_count"))
          .addAnalyzer(Minimum("distinct_pages_visited_count"))
          .addAnalyzer(Mean("distinct_pages_visited_count"))
          .addAnalyzer(Maximum("user_agent_length"))
          .addAnalyzer(Minimum("user_agent_length"))
          .addAnalyzer(Mean("user_agent_length"))
          .useRepository(repository)
          .saveOrAppendResult(countsResultKey)
          .run()

        val metrics = DeequAnalyzersToDataQualityMetrics(spark)(
            repository.load.get,
            config.hive_partition,
            config.run_id
        )

        val metricsWriter = config.metrics_table match {
            case Some(t) => metrics.write.iceberg.output(t)
            case None => metrics.write.iceberg
        }

        metricsWriter.save()
    }

    /**
     * Generate and store actor signature quality metrics.
     *
     * apply() wraps data preparation, compute and storage steps.
     *
     * @param spark
     * @param config
     * @return
     */
    def apply(spark: SparkSession)(config: Config): Try[Unit] = Try {
        import spark.implicits._

        // 1. Load data for a given input partition
        val sourceDataFrame = spark
          .sql(s"""select year, month, day, hour,
                    |actor_signature, pageview_count, distinct_pages_visited_count, user_agent_length
                    |from ${config.hive_partition.tableName}
                    |where (${config.hive_partition.sqlPredicate})
                    | """.stripMargin)

        // 2. Generate and store metrics.
        generateAndStoreMetrics(config)(sourceDataFrame)

        Success
    }
}
