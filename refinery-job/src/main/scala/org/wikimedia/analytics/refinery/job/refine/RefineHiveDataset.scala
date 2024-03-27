package org.wikimedia.analytics.refinery.job.refine

import org.apache.spark.sql.catalyst.util.FailFastMode
import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.job.refine.EventSparkSchemaLoader.BASE_SCHEMA_URIS_DEFAULT
import org.wikimedia.analytics.refinery.job.refine.RefineHelper.TransformFunction
import org.wikimedia.analytics.refinery.spark.sql.DataFrameToTable
import org.wikimedia.analytics.refinery.tools.LogHelper
import org.wikimedia.analytics.refinery.tools.config._

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
        input_format: String,
        input_paths: Seq[String],
        input_schema_base_uris: Seq[String] = BASE_SCHEMA_URIS_DEFAULT,
        input_schema_uri: String,
        output_table: String,
        output_hive_partition_paths: Seq[String] = Seq.empty[String],
        transform_functions: Seq[TransformFunction] = Seq.empty,
        dataframe_reader_options: Map[String, String] = Map("mode" -> FailFastMode.name),
        corrupt_record_failure_threshold: Int = 1
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

                if (input_paths.length != output_hive_partition_paths.length) {
                    illegalArgumentMessages +=
                        s"input_paths and output_hive_partition_paths must have the same number of elements."
                }
            } else {
                illegalArgumentMessages += "Must provide input_paths"
            }

            if (output_table.isEmpty) {
                illegalArgumentMessages += "Must provide output_table."
            } else if (!output_table.contains(".")) {
                illegalArgumentMessages += "Invalid output_table. It should match the form: db.table"
            }

            val hivePartitionPathPattern = """(?:[a-zA-Z_]+=[^/]+/?)+""".r
            if (output_hive_partition_paths.isEmpty) {
                illegalArgumentMessages += "Must provide output_hive_partition_paths."
            } else if (output_hive_partition_paths.forall(hivePartitionPathPattern.findFirstIn(_).isEmpty)) {
                illegalArgumentMessages += "Invalid output_hive_partition_paths. It should match the form: k1=v1/k2=v2/.../kn=vn"
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
                   |Must be one of ${SupportedInputFormats.mkString(",")}""".stripMargin,
            "input_schema_base_uris" ->
                """The base URIs to retrieve the input schema.""",
            "input_schema_uri" ->
                """The base-relative URI to retrieve the input schema.""",
            "output_table" ->
                """The fully qualified table name to write the data to. Must be in the form db.table
                  |e.g. event.navigationtiming""".stripMargin,
            "output_hive_partition_paths" ->
                """The partition paths to write the data to. Must be in the form k1=v1/k2=v2/.../kn=vn
                  |e.g. datacenter=eqiad/year=2024/month=2/day=15/hour=0
                  |Its order should match the order of the input_paths""".stripMargin,
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
                  |Default: mode:FAILFAST""".stripMargin
        )

        val usage: String =
            """
              |Hourly input folders to Hive table.
              |
              |Given an input path and an output partition, this will replace the data of the Hive partition.
              |
              |Example:
              |  spark3-submit --class org.wikimedia.analytics.refinery.job.refine.RefineHiveDataset refinery-job.jar \
              |  # read configs out of this file
              |   --config_file                     /etc/refinery/refine_event.properties \
              |   # Override and/or set other configs on the CLI
              |   --input_paths                       '/wmf/data/raw/eventlogging_legacy/eventlogging_NavigationTiming/year=2024/month=02/day=15/hour=00' \
              |   --input_format                      'json' \
              |   --input_schema_base_uris            'https://schema.discovery.wmnet/repositories/secondary/jsonschema/' \
              |   --input_schema_uri                  'analytics/legacy/navigationtiming/1.6.0' \
              |   --output_table                      'event.navigationtiming' \
              |   --output_hive_partition_paths       'datacenter=eqiad/year=2024/month=2/day=15/hour=0' \
              |   --transform_functions               'org.wikimedia.analytics.refinery.job.refine.extractDatacenterFromFolderName,org.wikimedia.analytics.refinery.job.refine.filter_allowed_domains,org.wikimedia.analytics.refinery.job.refine.remove_canary_events,org.wikimedia.analytics.refinery.job.refine.deduplicate,org.wikimedia.analytics.refinery.job.refine.geocode_ip,org.wikimedia.analytics.refinery.job.refine.parse_user_agent,org.wikimedia.analytics.refinery.job.refine.add_is_wmf_domain,org.wikimedia.analytics.refinery.job.refine.add_normalized_host,org.wikimedia.analytics.refinery.job.refine.normalizeFieldNamesAndWidenTypes'
              |""".stripMargin

        /**
         * Loads Config from args
         */
        def apply(args: Array[String]): Config = {
            val config = try {
                configureArgs[Config](args)
            } catch {
                case e: ConfigHelperException => {
                    log.fatal(e.getMessage + ". Aborting.")
                    sys.exit(1)
                }
            }
            log.info("Loaded Refine config:\n" + prettyPrint(config))
            config
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
        sys.exit(if (apply(spark)(config)) 0 else 1)
    }

    /**
     * Refine the input_paths for the given hour.
     *
     * @return true if the target refinement succeeded, false otherwise.
     */
    def apply(
        spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    )(
        config: Config,
        eventSparkSchemaLoader: Option[EventSparkSchemaLoader] = None
    ): Boolean = {
        val inputPathsCount = config.input_paths.length
        val table = config.output_table
        config.input_paths.zipWithIndex.map { case (inputPath: String, i: Int) =>
            try {
                val partitionPath = config.output_hive_partition_paths(i)
                log.info(s"Refining folder ${i + 1}/$inputPathsCount $inputPath into $table/$partitionPath")

                val inputDf = RefineHelper.readInputDataFrame(
                    spark,
                    config.input_paths,
                    config.input_schema_uri,
                    inputFormat = config.input_format,
                    inputSchemaLoader = eventSparkSchemaLoader,
                    dataframeReaderOptions = config.dataframe_reader_options,
                    inputSchemaBaseURIs = config.input_schema_base_uris,
                    corruptRecordFailureThreshold = config.corrupt_record_failure_threshold
                )

                val transformedDf = RefineHelper.applyTransforms(
                    inputDf,
                    config.output_table,
                    config.transform_functions
                )

                val recordCount = DataFrameToTable.hiveInsertOverwritePartition(transformedDf, table, partitionPath)

                log.info(s"Successfully refined $recordCount $inputPath into $table")
                Success()
            } catch {
                case e: Exception =>
                    log.error(s"Failed refinement of dataset $inputPath into $table.", e)
                    throw e
            }
        }.forall(_.isSuccess)
    }
}