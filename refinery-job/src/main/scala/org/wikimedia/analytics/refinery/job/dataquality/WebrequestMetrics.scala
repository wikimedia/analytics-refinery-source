package org.wikimedia.analytics.refinery.job.dataquality


import com.amazon.deequ.VerificationSuite
import org.apache.spark.sql.functions.{abs, col, count, max, min, udf}
import org.apache.spark.sql.SparkSession
import com.amazon.deequ.repository._
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import com.amazon.deequ.analyzers.{ApproxCountDistinct, Maximum, Mean, Minimum, Size, Sum}
import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.checks.{Check, CheckLevel}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.wikimedia.analytics.refinery.core.HivePartition
import org.wikimedia.analytics.refinery.spark.deequ.{DeequAnalyzersToDataQualityMetrics, DeequVerificationSuiteToDataQualityAlerts}
import org.wikimedia.analytics.refinery.tools.config._
import org.wikimedia.analytics.refinery.tools.LogHelper

import scala.collection.immutable.ListMap
import scala.util.{Failure, Try, Success}

/**
  * Generate data quality metrics for wmf.webrequest.
  */
object WebrequestMetrics extends LogHelper with ConfigHelper {
    private final val WebrequestSequenceValidation: String =
        "WebrequestSequenceValidation"

    private final val WebrequestSummaryStats: String =
        "WebrequestSummaryStats"

    private final val WebrequestDataIntegrityChecks: String = "integrity checks"

    private final val WebrequestSequenceValidationChecks : String = "sequence number validation"


    case class Config(
        table: String,
        partition_map: ListMap[String, String],
        run_id: String,
        // TODO: these should probably not be optional
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
              |Compute data quality metrics about a webrequest partition.
              |
              |Example:
              |  spark-submit --class org.wikimedia.analytics.refinery.job.refine.WebrequestMetrics refinery-job.jar \
              |   # read configs out of this file
              |   --config_file                     /etc/refinery/webrequest_metrics.properties \
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
            log.info("Loaded WebrequestMetrics config:\n" + prettyPrint(config))
            config
        }
    }

    private def onYarn(spark: SparkSession): Boolean = spark.conf.get("spark.master") == "yarn"

    private def mkResultKey(metricType: String): ResultKey = {
        ResultKey(System.currentTimeMillis(), Map("metric_type" -> metricType))
    }
    // https://phabricator.wikimedia.org/T351909
    private val countDuplicateKeys: UserDefinedFunction = udf((field: String) => {
        val keyValuePairs = field.split(";").map(_.trim)
        val uniqueKeys = keyValuePairs.map(_.split("=")(0)).toSet
        keyValuePairs.length - uniqueKeys.size
    })

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
        // Exit with proper exit val if not running in YARN.
        val exitCode = apply(spark)(config) match {
            case Success(_) => 0
            case Failure(_) => 1

        }
        if (!onYarn(spark)) {
            sys.exit(exitCode)
        }
    }

    def apply(spark: SparkSession)(config: Config): Try[Unit] = Try {
        val webrequestsDf = spark
            .sql(s"""select year, month, day, hour, hostname, user_agent, sequence, x_analytics
                    |from ${config.hive_partition.tableName}
                    |where (${config.hive_partition.sqlPredicate})
                    | """.stripMargin)
            .withColumn(
                "x_analytics_duplicate_keys",
                countDuplicateKeys(col("x_analytics")) // https://phabricator.wikimedia.org/T351909
            )

        // Calculate the difference between the number of rows and expected_sequence
        val sequenceNumberDiffDf = webrequestsDf
            .select("hour", "hostname", "sequence")
            .where(col("sequence") > 0)
            .groupBy("hour", "hostname")
            .agg(
                count("sequence").alias("num_rows"),
                max("sequence").alias("max_sequence"),
                min("sequence").alias("min_sequence")
            )
            .withColumn(
                "expected_sequence",
                col("max_sequence") - col("min_sequence")
            )
            .withColumn("difference", abs(col("num_rows") - col("expected_sequence")))

        val countsResultKey = mkResultKey(WebrequestSummaryStats)
        val sequenceValidationKey =
            mkResultKey(WebrequestSequenceValidation)

        // Generate summary stats.
        AnalysisRunner
            .onData(
                webrequestsDf
                    .select("user_agent", "hostname", "x_analytics_duplicate_keys")
            )
            .addAnalyzer(Size())
            .addAnalyzer(ApproxCountDistinct("user_agent"))
            .addAnalyzer(ApproxCountDistinct("hostname"))
            .addAnalyzer(Sum("x_analytics_duplicate_keys"))
            .useRepository(repository)
            .saveOrAppendResult(countsResultKey)
            .run()

        // Sequence number validation data prep.
        AnalysisRunner
            .onData(
                sequenceNumberDiffDf
                    .select("hostname", "difference")
            )
            .addAnalyzer(Minimum("difference"))
            .addAnalyzer(Maximum("difference"))
            .addAnalyzer(Mean("difference"))
            .useRepository(repository)
            .saveOrAppendResult(sequenceValidationKey)
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

        // Data validation.
        // TODO: threshold / constraints tuning.
        // TODO: extract magic numbers to constants?
        val webrequestConstraints = VerificationSuite()
          .onData(webrequestsDf)
          .addCheck(Check(CheckLevel.Error, WebrequestDataIntegrityChecks)
            .isComplete("user_agent")
            .isNonNegative("x_analytics_duplicate_keys")
            .hasMax("x_analytics_duplicate_keys", _ == 0))
          // TODO: we might want to store the computed constraint checks as metrics in the repository
          //.useRepository(repository)
          //.saveOrAppendResult(sequenceValidationKey)
          .run()
          .checkResults
          .flatMap { case (_, checkResult) => checkResult.constraintResults }.toSeq

        // Sequence number validation.
        val sequenceNumberConstraints = VerificationSuite()
          .onData(sequenceNumberDiffDf)
          .addCheck(Check(CheckLevel.Error, WebrequestSequenceValidationChecks)
            .isNonNegative("difference")
            .hasApproxQuantile("difference", 0.9, _ == 1) // 90% of entries have valid sequence numbers
          )

          // TODO: we might want to store the computed constraint checks as metrics in the repository
          //.useRepository(repository)
          //.saveOrAppendResult(metricsResultKey)
          .run()
          .checkResults
          .flatMap { case (_, checkResult) => checkResult.constraintResults }.toSeq


        val alerts = DeequVerificationSuiteToDataQualityAlerts(spark)(webrequestConstraints ++ sequenceNumberConstraints,
            config.hive_partition,
            config.run_id)

        val alertsWriter = alerts.write

        // Here we assume that alerts will be persisted only
        // when the user provides and output table / path.
        if (config.alerts_table.nonEmpty) {
            alerts.write.iceberg.output(config.alerts_table.get)
            alertsWriter.iceberg.save()
        }

        if (config.alerts_output_path.nonEmpty) {
            alerts.write.text.output(config.alerts_output_path.get)
            alertsWriter.text.save()
        }

        Success
    }
}
