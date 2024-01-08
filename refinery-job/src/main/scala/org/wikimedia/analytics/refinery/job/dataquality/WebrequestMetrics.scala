package org.wikimedia.analytics.refinery.job.dataquality


import com.amazon.deequ.VerificationSuite
import org.apache.spark.sql.functions.{abs, col, count, max, min, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.amazon.deequ.repository._
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import com.amazon.deequ.analyzers.{ApproxCountDistinct, Histogram, Maximum, Mean, Minimum, Size, Sum}
import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.checks.{Check, CheckLevel}
import org.wikimedia.analytics.refinery.core.HivePartition
import org.wikimedia.analytics.refinery.spark.deequ.{DeequAnalyzersToDataQualityMetrics, DeequVerificationSuiteToDataQualityAlerts}
import org.wikimedia.analytics.refinery.tools.config._
import org.wikimedia.analytics.refinery.tools.LogHelper

import scala.collection.immutable.ListMap
import scala.util.{Failure, Success, Try}

// TODO: consider moving these utility classes to a package. What is a good location?
protected case class KeyValue(key: String, value: String)

protected trait MultipartValueSplitter {
    val keyValueDelimiter = ';'
    val keyValueAssignmentOperator = '='


    /**
     * Splits a string containing ";" delimited "key=value"
     * pairs when the value can also contain the delimiter ";"
     *
     * E.g. key1=multi;part;key2=value2 will be mapped to [(key1, multi;part), (key2, value2)]
     *
     * @param string
     * @return
     */
    def splitAndTrim(string: String): Seq[KeyValue] = {

        var isKey = true
        var currentKey = "" // key accumulator
        var currentValue = "" // value accumulator
        var currentToken = "" // look-ahead
        val resultSet = scala.collection.mutable.ArrayBuffer[KeyValue]()

        for (i <- 0 until string.length) {
            val char = string(i)

            if (char == keyValueAssignmentOperator && isKey) {
                isKey = false
            } else if (char == keyValueAssignmentOperator && !isKey) {
                resultSet.append(KeyValue(currentKey, currentValue.stripSuffix(";")))
                currentKey = currentToken
                currentToken = ""
                currentValue = ""
            } else if (char == keyValueDelimiter) {
                // start of new candidate value.
                // don't start with ";"
                if (currentValue.nonEmpty) {
                    currentValue += char
                }
                currentValue += currentToken
                currentToken = ""
            } else {
                if (isKey) {
                    currentKey += char
                } else {
                    currentToken += char
                }
            }
        }
        // start of new candidate value.
        // don't start with ";"
        if (currentValue.nonEmpty) {
            currentValue += keyValueDelimiter
        }
        currentValue += currentToken
        resultSet.append(KeyValue(currentKey, currentValue))

        resultSet
    }
}

protected object CountDuplicateKeys {
    /**
     * Count the number of duplicate key/value pairs in a query string.
     *
     * Helper class for parsing x_analytics via a Spark UDF.
     * See https://phabricator.wikimedia.org/T351909
     *
     * @param keyValuePairs a map of key/value pairs
     * @param distinctValues when true consider keys with distinct values as non duplicate.
     * @return
     */
    def apply(keyValuePairs: Seq[KeyValue], distinctValues: Boolean): Int = {
        if (!distinctValues) {
            // Count duplicate keys, regardless of their value.
            val uniqueKeys = keyValuePairs.map(x => x.key).toSet

            keyValuePairs.length - uniqueKeys.size
        } else {
            // Count duplicate (key, value) pairs.
            keyValuePairs.length - keyValuePairs.toSet.size
        }
    }
}

protected object HasMultipartValue extends MultipartValueSplitter {
    /**
     * Check if a ; delimited string of key/value pairs contains multipart
     * values.
     *
     * We found instances of x_analytics `wprov`` keys containing stylesheet markup for `sfti1`
     * ios text share value.
     * This leads to multipart (nested) key/value pairs that will break str_map parsing logic,
     * and generate false positives (and negatives) alerts.
     *
     * This format does not conform to documentation, and we should consider it invalid.
     *
     * In terms of metrics, we use this function to count the number of keys with
     * invalid payloads. This could result in key/value sets that match `CountDuplicateKeys` criteria.
     *
     * In future work we might want to add constraints on which keys should be considered valid (e.g.
     * by matching an allow list).
     *
     * Helper class for parsing x_analytics via a Spark UDF.
     * See:
     *  - https://phabricator.wikimedia.org/T351909
     *  - https://phabricator.wikimedia.org/T354568
     *
     * @param string the query string.
     * @return
     */
    def apply(keyValuePairs: Seq[KeyValue]): Boolean = {
        val values = keyValuePairs.map(kv => kv.value).toList

        // Check if any value contains multiple values
        values.exists(value => value.contains(keyValueDelimiter))
    }
}

protected object QueryStringAnalyzer extends MultipartValueSplitter {
    /**
     * Parse and compute some stats on query strings (e.g. x_analytics) in one go.
     * Ordering of returned values is significant. UDF logic relies
     * on it for columns aliasing.
     *
     * Helper class for parsing x_analytics via a Spark UDF.
     * See https://phabricator.wikimedia.org/T351909
     *
     * @param string the query string.
     * @return
     */
    def apply(string: String): Tuple3[Int, Int, Int] = {
        val keyValuePairs = splitAndTrim(string)
        val distinctValues = true;

        Tuple3(if (HasMultipartValue(keyValuePairs))  1 else 0, // ._1
          CountDuplicateKeys(keyValuePairs, distinctValues), // ._2
          CountDuplicateKeys(keyValuePairs, !distinctValues)) // ._3
    }
}

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
    private val parseXAnalyticsUdf = udf((string: String) => QueryStringAnalyzer(string))

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

    /**
     * Helper function to prepare webrequest data for Deequ analysis. Projects columns
     * we want to compute summary stats for, and parse x_analytics for validation.
     *
     * @param data
     * @return
     */
    private def prepareWebrequestSummaryStatsDataFrame(data: DataFrame): DataFrame = {
        val spark = data.sparkSession
        import spark.implicits._
        // Pre-process `wmf.webrequest` for analysis and data quality checks.
        data
          .withColumn("x_analytics_parsed", parseXAnalyticsUdf($"x_analytics"))
          .select($"year",
              $"month",
              $"day",
              $"hour",
              $"hostname",
              $"user_agent",
              $"sequence",
              $"x_analytics_parsed._1".as("x_analytics_duplicate_keys"),
              $"x_analytics_parsed._2".as("x_analytics_duplicate_keys_values"),
              $"x_analytics_parsed._3".as("x_analytics_count_multipart_value")
          )
    }

    /**
     * Helper function to prepare webrequest data for Deequ analysis. Computes raw aggregate values
     * for sequence number validation.
     *
     * @param data
     * @return
     */
    private def prepareWebrequestSequenceNumberValidationDataFrame(data: DataFrame): DataFrame = {
        val spark = data.sparkSession
        import spark.implicits._

        // Calculate the difference between the number of rows and expected_sequence
        data
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


    }

    /**
     * * Generate and store webrequest metrics.
     *
     * Wraps the execution of Deequ's Analysis runner and DeequAnalyzersToDataQualityMetrics.
     *
     * Metrics are computed on both webrequest and sequence number datasets. The union of both analysis
     * is stored (in memory) in a Deequ `repository`. Both sets of metrics are stored in Iceberg.
     *
     * WARNING: this function has side effects. The generated alerts can be store to Iceberg/HDFS (depending
     * on config options).
     *
     */
    private def generateAndStoreWebRequestMetrics(config: Config)
                                         (webrequestDf: DataFrame, sequenceNumberValidationDf: DataFrame): Unit = {
        val spark = webrequestDf.sparkSession

        import spark.implicits._
        val countsResultKey = mkResultKey(WebrequestSummaryStats)
        val sequenceValidationKey =
            mkResultKey(WebrequestSequenceValidation)

        // Generate summary stats (counts, sums).
        AnalysisRunner
          .onData(
              webrequestDf
                .select($"user_agent",
                    $"hostname",
                    $"x_analytics_duplicate_keys",
                    $"x_analytics_duplicate_keys_values",
                    $"x_analytics_count_multipart_value")
          )
          .addAnalyzer(Size())
          .addAnalyzer(ApproxCountDistinct("user_agent"))
          .addAnalyzer(ApproxCountDistinct("hostname"))
          .addAnalyzer(Sum("x_analytics_duplicate_keys"))
          .addAnalyzer(Sum("x_analytics_duplicate_keys_values"))
          .addAnalyzer(Sum("x_analytics_count_multipart_value"))
          .useRepository(repository)
          .saveOrAppendResult(countsResultKey)
          .run()

        // Sequence number validation data prep.
        AnalysisRunner
          .onData(
              sequenceNumberValidationDf
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
    }

    /**
     * Generate and store webrequest alerts.
     *
     * Wraps the execution of Deequ's VerificationSuite and DeequVerificationSuiteToDataQualityAlerts.
     *
     * Alerts are computed on both webrequest and sequence number datasets. The union of both sets of
     * alerts are stored in Iceberg.
     *
     * WARNING: this function has side effects. The generated alerts can be store to
     * Iceberg/HDFS (depending
     * on config options).
     *
     * @param config
     * @param webrequestDf
     * @param sequenceNumberValidationDf
     */
    private def generateAndStoreWebrequestAlerts(config: Config)
                                        (webrequestDf: DataFrame, sequenceNumberValidationDf: DataFrame): Unit = {
        val spark = webrequestDf.sparkSession
        // Data validation.
        // TODO: threshold / constraints tuning.
        // TODO: extract magic numbers to constants?
        val webrequestVerificationSuite = VerificationSuite()
          .onData(webrequestDf)
          .addCheck(Check(CheckLevel.Warning, WebrequestSequenceValidationChecks)
            .isNonNegative("x_analytics_duplicate_keys")
            .hasMax("x_analytics_duplicate_keys", _ == 0))
          .addCheck(Check(CheckLevel.Error, WebrequestDataIntegrityChecks)
            .isComplete("user_agent")
            .isNonNegative("x_analytics_duplicate_keys_values")
            .hasMax("x_analytics_duplicate_keys_values", _ == 0))
          // TODO: we might want to store the computed constraint checks as metrics in the repository
          //.useRepository(repository)
          //.saveOrAppendResult(sequenceValidationKey)
          .run()
          .checkResults

        // Sequence number validation.
        val sequenceNumberVerificationSuite = VerificationSuite()
          .onData(sequenceNumberValidationDf)
          .addCheck(Check(CheckLevel.Error, WebrequestSequenceValidationChecks)
            .isNonNegative("difference")
            .hasApproxQuantile("difference", 0.9, _ == 1) // 90% of entries have valid sequence numbers
          )

          // TODO: we might want to store the computed constraint checks as metrics in the repository
          //.useRepository(repository)
          //.saveOrAppendResult(metricsResultKey)
          .run()
          .checkResults

        val alerts = DeequVerificationSuiteToDataQualityAlerts(spark)(webrequestVerificationSuite ++ sequenceNumberVerificationSuite,
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
    }

    /**
     * Genetate and store webrquest quality metrics and alerts.
     *
     * applu() wraps data preparation, compute and storage steps.
     *
     * @param spark
     * @param config
     * @return
     */
    def apply(spark: SparkSession)(config: Config): Try[Unit] = Try {
        import spark.implicits._

        // 1. Load webrequest data for a given input partition
        val sourceDataFrame = spark
            .sql(s"""select year, month, day, hour, hostname, user_agent, sequence, x_analytics
                    |from ${config.hive_partition.tableName}
                    |where (${config.hive_partition.sqlPredicate})
                    | """.stripMargin)

        // 2. Pre-process `wmf.webrequest` for analysis and data quality checks.
        // Project target columns, and apply a UDF to parse x_analytics
        val webrequestDf = prepareWebrequestSummaryStatsDataFrame(sourceDataFrame)
        // 3. Calculate the difference between the number of rows and expected_sequence
        val sequenceNumberValidationDf = prepareWebrequestSequenceNumberValidationDataFrame(sourceDataFrame)

        // 4. Generate and store metrics and alerts.
        generateAndStoreWebRequestMetrics(config)(webrequestDf, sequenceNumberValidationDf)
        generateAndStoreWebrequestAlerts(config)(webrequestDf, sequenceNumberValidationDf)

        Success
    }
}
