package org.wikimedia.analytics.refinery.job.dataquality

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.analyzers.Maximum
import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.wikimedia.analytics.refinery.tools.LogHelper
import org.wikimedia.analytics.refinery.tools.config._
import org.wikimedia.analytics.refinery.core.HivePartition
import org.wikimedia.analytics.refinery.spark.deequ.{DeequAnalyzersToDataQualityMetrics, DeequVerificationSuiteToDataQualityAlerts}

import scala.collection.immutable.ListMap
import scala.util.{Failure, Success, Try}

/**
 * Check Human-Automated ratio anomaly of PageViews
 */
object PageViewMetrics extends LogHelper with ConfigHelper{
    case class Config(
                       source_table: String = "wmf.pageview_hourly",
                       metrics_table: Option[String] = None,
                       alerts_table: Option[String] = None,
                       alerts_output_path: Option[String] = None,
                       human_bounds: ListMap[String, String],
                       auto_bounds: ListMap[String, String],
                       run_id: String,
                       year: Int,
                       month: Int,
                       day: Int
                     ) {
        val source_hive_table_partition: HivePartition = HivePartition(
            source_table,
            ListMap(
                "year" -> year.toString,
                "month" -> month.toString,
                "day" -> day.toString
            )
        )
    }

    object Config {
        val propertiesDoc: ListMap[String, String] = ListMap(
            "config_file <file1.properties,files2.properties>" ->
              """Comma separated list of paths to properties files.  These files parsed for
                |for matching config parameters as defined here.""",
            "source_table" ->
              """Hive table to compute pageview human-spider-automated ratio metrics on.""",
            "metrics_table" ->
              """Hive table name in which to write computed metrics.""",
            "alerts_table" ->
              """Hive table name in which to write computed alerts.""",
            "alerts_output_path" ->
              """Path to write failed checks on Hdfs""",
            "human_bounds" ->
              """A ListMap containing upper and lower thresholds of human share.
                |Please note the first key name is the "lower"
                |while the second key name is the "upper".
                |""".stripMargin,
            "auto_bounds" ->
              """A ListMap containing upper and lower thresholds of automated share.
                |Please note the first Key name is the "lower"
                |and the second key is name "upper".
                |""".stripMargin,
            "year" ->
              """year to check for anomaly""",
            "month" ->
              """month to check for anomaly""",
            "day" ->
              """day to check for anomaly."""
        )
        val usage: String =
            """
            Compute data quality metrics for Pageview hourly .

            Example:
              spark3-submit \
              --class org.wikimedia.analytics.refinery.job.dataquality.PageViewMetrics refinery-job.jar \
               # Configs can be read out of a config file
               --config_file                     /etc/refinery/pageview_metrics.properties \
               # Override and/or set other configs on the CLI
               --source_table           "wmf.pageview_hourly"
            """

        def apply(args: Array[String]): Config = {
            val config = try{
                configureArgs[Config](args)
            } catch {
                case e: ConfigHelperException =>
                    log.fatal(e.getMessage + ". Aborting.")
                    sys.exit(1)
            }
            log.info("Loaded Pageviews Metrics config:\n" + prettyPrint(config))
            config
        }
    }

    private def onYarn(spark: SparkSession): Boolean = spark.conf.get("spark.master") == "yarn"

    private val repository: MetricsRepository =
        new InMemoryMetricsRepository

    private val agentSharesMetricKey = ResultKey(System.currentTimeMillis(), Map("metric_type" -> "PageViewAgentRatios"))

    def main(args: Array[String]): Unit = {
        val conf = Config(args)
        val spark = SparkSession
          .builder()
          .enableHiveSupport()
          .getOrCreate()
        // Make spark log level quieter if not running on yarn.
        if (!onYarn(spark)) {
            spark.sparkContext.setLogLevel("WARN")
        }
        sys.exit(
            apply(spark)(conf) match {
                case Success(_) => 0
                case Failure(_) => 1
            }
        )
    }

    /**
     * Function to compute the ratios of all agent types.
     * It computes  the human, spider and automated ratios respectively
     *
     * @param spark SparkSession used for executing Spark SQL
     * @param conf Configuration object containing runtime parameters
     * @param currPvDf DataFrame containing the current day's page views data
     * @return Dataframe
     */
    private[dataquality] def generateAgentDailyRatioMetrics(spark: SparkSession)
                                                                  (conf: Config,
                                                                   currPvDf: DataFrame): DataFrame = {

        val currRatioDf = currPvDf.groupBy()
          .agg(
              sum(when(col("agent_type") === "user", col("view_count"))).as("user"),
              sum(when(col("agent_type") === "automated", col("view_count"))).as("automated"),
              sum(when(col("agent_type") === "spider", col("view_count"))).as("spider")
          )
          .na.fill(0)
          .withColumn("total_views", col("user") + col("automated") + col("spider"))
          .withColumn("human_ratio", col("user") / col("total_views"))
          .withColumn("automated_ratio", col("automated") / col("total_views"))
          .withColumn("spider_ratio", col("spider") / col("total_views"))

        currRatioDf
    }

    /**
     * This function uses Deequ to to get the ratios and store them
     * in the metric table
     *
     * @param conf Configuration object containing runtime parameters
     * @param currRatioDf
     */
    private[dataquality] def saveDailyRatioMetric(conf: Config,
                                                  currRatioDf: DataFrame): Unit = {

        val spark = currRatioDf.sparkSession
        // Run deequ analyzers to generate metrics
        AnalysisRunner
          .onData(currRatioDf)
          .addAnalyzer(Maximum("human_ratio"))
          .addAnalyzer(Maximum("automated_ratio"))
          .addAnalyzer(Maximum("spider_ratio"))
          .useRepository(repository)
          .saveOrAppendResult(agentSharesMetricKey)
          .run()

        // save metrics in metric table
        val ratioMetrics = DeequAnalyzersToDataQualityMetrics(spark)(
            repository.load.get,
            conf.source_hive_table_partition,
            conf.run_id
        )

        val ratioMetricWriter = conf.metrics_table match {
            case Some(value) =>
                // Delete old metrics in case of rerun
                spark.sql(
                    s"""
                       |DELETE FROM $value
                       |WHERE partition_ts = CAST('${conf.source_hive_table_partition.dt}' AS TIMESTAMP)
                       |AND source_table = '${conf.source_hive_table_partition.tableName}'
                       |AND tags['metric_type'] = 'PageViewAgentRatios'
                       |""".stripMargin
                )
                ratioMetrics.write.iceberg.output(value)
            case None => ratioMetrics.write.iceberg
        }
        ratioMetricWriter.save()

        log.info(s"Daily ratios saved for ${conf.year}-${conf.month}-${conf.day}")

    }


    /**
     * Check Human and Automated Ratios are within provided thresholds.
     *
     * Saves Ratios to alert output file path if check fails
     * for alerting purpose.
     *
     * @param conf Configuration object containing runtime parameters
     * @param todayRatioDf
     */
    private[dataquality] def verifyAgentDailyRatio(conf: Config)
                                                  (todayRatioDf: DataFrame): Unit = {
        val spark = todayRatioDf.sparkSession


        // We use Deequ here so that we can also save the checks in
        // general alert table using the deequ defined schema.
        // Define checks
        val check = Check(CheckLevel.Error, "Daily PageViews agent ratios anomaly check")
          .isContainedIn("human_ratio", conf.human_bounds("lower").toDouble, conf.human_bounds("upper").toDouble)
          .isContainedIn("automated_ratio", conf.auto_bounds("lower").toDouble, conf.auto_bounds("upper").toDouble)

        // Apply Deequ checks
        val botDetectionVerificationResult = VerificationSuite()
          .onData(todayRatioDf)
          .addCheck(check)
          .run()
          .checkResults

        val dqAlerts = DeequVerificationSuiteToDataQualityAlerts(spark)(botDetectionVerificationResult,
            conf.source_hive_table_partition,
            conf.run_id,
            Option(agentSharesMetricKey))

        // Alerts will be persisted only when the user
        // provides an output table or output path.
        if (conf.alerts_table.nonEmpty) {
            dqAlerts.write.iceberg.output(conf.alerts_table.get).save()
        }

        // if ratios are above thresholds save to alerts output file.
        if (conf.alerts_output_path.nonEmpty) {
            dqAlerts.write.text.output(conf.alerts_output_path.get).save()
        }
    }

    /**
     * Bundles data metrics computation and checks.
     *
     * @param spark SparkSession used for executing Spark SQL
     * @param config Configuration object containing runtime parameters
     * @return
     */
    def apply(spark: SparkSession)(config: Config): Try[Unit] = Try{
        try{
            //Extract Current day's data
            val currPvDf = spark.sql(
                s"""
                   |select agent_type, SUM(view_count) AS view_count
                   |from ${config.source_table}
                   |where ${config.source_hive_table_partition.sqlPredicate}
                   |group by agent_type
                   |""".stripMargin
            )
            // generate and compare agent ratios
            val currPageViewRatios = generateAgentDailyRatioMetrics(spark)(config, currPvDf)
            saveDailyRatioMetric(config, currPageViewRatios)
            verifyAgentDailyRatio(config)(currPageViewRatios)

        } catch {
            case e: Exception =>
                log.error(s"Failed to perform data quality check on ${config.source_table}", e)
                // Rethrow error after logging to force failure
                throw e
        }
    }

}
