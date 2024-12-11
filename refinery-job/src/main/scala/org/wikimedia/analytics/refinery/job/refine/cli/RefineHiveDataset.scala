package org.wikimedia.analytics.refinery.job.refine.cli

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.FailFastMode
import org.wikimedia.analytics.refinery.core.HivePartition
import org.wikimedia.analytics.refinery.job.refine.RefineHelper.{TransformFunction, buildEventSchemaLoader}
import org.wikimedia.analytics.refinery.job.refine.WikimediaEventSparkSchemaLoader.BASE_SCHEMA_URIS_DEFAULT
import org.wikimedia.analytics.refinery.job.refine.{RawRefineDataReader, RefineHelper, RefineTarget, SparkEventSchemaLoader}
import org.wikimedia.analytics.refinery.spark.sql.DataFrameToTable
import org.wikimedia.analytics.refinery.spark.sql.HiveExtensions._
import org.wikimedia.analytics.refinery.tools.LogHelper
import org.wikimedia.analytics.refinery.tools.config._
import org.wikimedia.eventutilities.core.event.EventSchemaLoader

import java.net.URI
import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer
import scala.util.Success

object RefineHiveDataset
    extends LogHelper
        with ConfigHelper
        with TransformFunctionsConfigHelper {

    val SupportedInputFormats: Set[String] = Set("parquet", "json", "avro")

    /**
     * Config class for use config files and args.
     */
    case class Config(
        input_paths: Seq[String],
        schema_uri: String,
        table: String,
        partition_paths: Seq[String],
        input_format: String = "json",
        schema_base_uris: Seq[String] = BASE_SCHEMA_URIS_DEFAULT,
        transform_functions: Seq[TransformFunction] = Seq.empty,
        dataframe_reader_options: Map[String, String] = Map("mode" -> FailFastMode.name),
        corrupt_record_failure_threshold: Int = 1,
        spark_job_scale: String = "small"
    ) {
        // Call validate now so we can throw at instantiation if this Config is not valid.
        validate()

        /**
         * Validates that configs as provided make sense.
         * Throws IllegalArgumentException if not.
         */
        private def validate(): Unit = {
            val illegalArgumentMessages: ArrayBuffer[String] = ArrayBuffer()

            if (input_paths.nonEmpty) {
                if (!RefineTarget.supportedInputFormats.contains(input_format)) {
                    illegalArgumentMessages +=
                        s"Invalid input_format ${input_format}. " +
                            s"Must be one of ${SupportedInputFormats.mkString(",")}"
                }

                if (input_paths.length != partition_paths.length) {
                    illegalArgumentMessages +=
                        s"input_paths and partition_paths must have the same number of elements."
                }
            } else {
                illegalArgumentMessages += "Must provide input_paths"
            }

            if (table.isEmpty) {
                illegalArgumentMessages += "Must provide output_table."
            } else if (!table.contains(".")) {
                illegalArgumentMessages += "Invalid output_table. It should match the form: db.table"
            }

            val hivePartitionPathPattern = """(?:[a-zA-Z_]+=[^/]+/?)+""".r
            if (partition_paths.forall(hivePartitionPathPattern.findFirstIn(_).isEmpty)) {
                illegalArgumentMessages += "Invalid partition_paths. It should match the form: k1=v1/k2=v2/.../kn=vn"
            }

            if (illegalArgumentMessages.nonEmpty) {
                throw new IllegalArgumentException(
                    illegalArgumentMessages.mkString("\n")
                )
            }
        }
    }

    object Config {
        val propertiesDoc: ListMap[String, String] = ListMap(
            "input_paths" ->
                """Paths to input data. Every directory is expected to be in hourly
                  |time-partitioned format""".stripMargin,
            "input_format" ->
                s"""Format of the input data, used when reading data with Spark DataFrameReader.
                   |Defaults to 'json'.
                   |Must be one of ${SupportedInputFormats.mkString(",")}""".stripMargin,
            "schema_base_uris" ->
                """The base URIs to retrieve the input schema.""",
            "schema_uri" ->
                """The base-relative URI to retrieve the input schema.""",
            "table" ->
                """The fully qualified table name to write the data to. Must be in the form db.table
                  |e.g. event.navigationtiming""".stripMargin,
            "partition_paths" ->
                """List of output Hive partitions specified in path form k1=v1/k2=v2/.../kn=vn
                  |e.g. datacenter=eqiad/year=2024/month=2/day=15/hour=0
                  |Its order should match the order of the input_paths.
                  |Note that these are not necessarily the actual file paths, but
                  |are just used to add literal columns and values to the data before
                  |writing it.
                  |Actual file partitioning is handled by the table schema spec when writing.
                  |""".stripMargin,
            "transform_functions" ->
                s"""Comma separated list of fully qualified module.ObjectNames. The objects'
                   |apply methods should take a DataFrame and a HivePartition and return
                   |a DataFrame.  These functions can be used to map from the input DataFrames
                   |to a new ones, applying various transformations along the way.
                   |NOTE: The functions will be executed in the given order (it matters:).
                   |Default: none""".stripMargin,
            "dataframe_reader_options" ->
                """Comma separated list of key:value pairs to use as options to DataFrameReader
                  |when reading the input DataFrame.
                  |Default: mode:FAILFAST""".stripMargin,
            "spark_job_scale" ->
                """The scale of the Spark job (small, medium, large). This is used to set the number of
                  |partitions of the DataFrame. Default: small""".stripMargin
        )

        val usage: String =
            """
              |Hourly input folders to Hive table.
              |
              |Given an input path and an output partition, this will replace the data of the Hive partition.
              |
              |Example:
              |  spark3-submit --class org.wikimedia.analytics.refinery.job.refine.RefineHiveDataset refinery-job.jar \
              |   # read configs out of this file
              |   --config_file                        /etc/refinery/refine_event.properties \
              |   # Override and/or set other configs on the CLI
              |   --input_paths                       '/wmf/data/raw/eventlogging_legacy/eventlogging_NavigationTiming/year=2024/month=02/day=15/hour=00' \
              |   --input_format                      'json' \
              |   --schema_base_uris                  'https://schema.discovery.wmnet/repositories/secondary/jsonschema/' \
              |   --schema_uri                        'analytics/legacy/navigationtiming/1.6.0' \
              |   --table                             'event.navigationtiming' \
              |   --partition_paths                   'datacenter=eqiad/year=2024/month=2/day=15/hour=0' \
              |   --transform_functions               'org.wikimedia.analytics.refinery.job.refine.filter_allowed_domains,org.wikimedia.analytics.refinery.job.refine.remove_canary_events,org.wikimedia.analytics.refinery.job.refine.deduplicate,org.wikimedia.analytics.refinery.job.refine.geocode_ip,org.wikimedia.analytics.refinery.job.refine.parse_user_agent,org.wikimedia.analytics.refinery.job.refine.add_is_wmf_domain,org.wikimedia.analytics.refinery.job.refine.add_normalized_host,org.wikimedia.analytics.refinery.job.refine.normalizeFieldNamesAndWidenTypes' \
              |   --spark_job_scale                   'small'
              |""".stripMargin

        /**
         * Loads Config from args
         */
        def apply(args: Array[String]): Config = {
            try {
                val config = configureArgs[Config](args)
                log.info("Loaded Refine config:\n" + prettyPrint(config))
                config
            } catch {
                case e: ConfigHelperException => {
                    log.fatal(e.getMessage + ". Aborting.")
                    sys.exit(1)
                }
            }
        }
    }

    def main(args: Array[String]): Unit = {
        args.foreach(println)

        if (args.contains("--help")) {
            println(help(Config.usage, Config.propertiesDoc))
            sys.exit(0)
        }

        val spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
        adjustLoggingLevelToSparkMaster(spark)

        val config = Config(args)

        // Call apply with spark and Config properties as parameters
        val eventSchemaLoader: EventSchemaLoader = buildEventSchemaLoader(config.schema_base_uris)
        val sparkSchemaLoader = SparkEventSchemaLoader(eventSchemaLoader, timestampsAsStrings = true)
        val reader = RawRefineDataReader(
            spark,
            sparkSchemaLoader.load(URI.create(config.schema_uri)),
            config.input_format,
            config.dataframe_reader_options,
            config.corrupt_record_failure_threshold)

        sys.exit(if (apply(reader, config)) 0 else 1)
    }

    /**
     * Refine the input_paths for the given hour.
     *
     * @return true if the target refinement succeeded, false otherwise.
     */
    def apply(reader: RawRefineDataReader, config: Config): Boolean = {

        val inputPathsCount = config.input_paths.length
        config.input_paths.zipWithIndex.map {
            case (inputPath: String, i: Int) =>
                val Array(database, table) = config.table.split("\\.")
                val tableLocation = reader.tableLocation(config.table)
                val partitionPath = config.partition_paths(i)

                // Here we are using HivePartition:
                // - for nice toString and logging purposes.
                // - to parse the partitionPath into a Map of partition columns.
                val hivePartition = HivePartition(
                    table,
                    database,
                    tableLocation, // location not really needed here, but it is nice for logging
                    partitionPath
                )

                try {
                    log.info(s"Refining folder ${i + 1}/$inputPathsCount $inputPath into $hivePartition")

                    val inputDf = reader.readInputDataFrameWithSchemaURI(Seq(inputPath))

                    val outputFilesNumber = if (config.spark_job_scale == "small") 1 else inputDf.rdd.getNumPartitions

                    // Add partition columns with the literal values as provided.
                    val inputDfWithPartitionValues = inputDf.addPartitionColumnValues(hivePartition.partitions)

                    // Apply the configured transform functions.
                    val transformedDf = RefineHelper.applyTransforms(inputDfWithPartitionValues, config.transform_functions)

                    // Insert and overwrite the DataFrame into the Hive table.
                    val recordCount = DataFrameToTable.hiveInsertOverwrite(
                        transformedDf,
                        config.table,
                        outputFilesNumber
                    )

                    log.info(s"Successfully refined $recordCount rows from ${config.input_paths} into $hivePartition")
                    Success()
                } catch {
                    case e: Exception =>
                        log.error(s"Failed refinement of $inputPath into $hivePartition", e)
                        throw e
                }
        }.forall(_.isSuccess)
    }
}