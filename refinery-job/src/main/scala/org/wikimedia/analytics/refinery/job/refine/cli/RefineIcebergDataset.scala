package org.wikimedia.analytics.refinery.job.refine.cli

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.FailFastMode
import org.wikimedia.analytics.refinery.job.refine.RefineHelper.{TransformFunction, buildEventSchemaLoader}
import org.wikimedia.analytics.refinery.job.refine.WikimediaEventSparkSchemaLoader.BASE_SCHEMA_URIS_DEFAULT
import org.wikimedia.analytics.refinery.job.refine.{RawRefineDataReader, RefineHelper, RefineTarget}
import org.wikimedia.analytics.refinery.spark.sql.DataFrameToTable
import org.wikimedia.analytics.refinery.tools.LogHelper
import org.wikimedia.analytics.refinery.tools.config._
import org.wikimedia.eventutilities.core.event.EventSchemaLoader

import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

object RefineIcebergDataset extends LogHelper with ConfigHelper with TransformFunctionsConfigHelper {

    val SupportedInputFormats: Set[String] = Set("parquet", "json", "avro")

    /**
      * Config class for use config files and args.
      */
    case class Config(
        input_paths: Seq[String],
        input_format: String,
        input_schema_uri: String,
        output_table: String,
        partition_time_field: String,
        delete_after: DateTime,
        delete_before: DateTime,
        input_schema_base_uris: Seq[String] = BASE_SCHEMA_URIS_DEFAULT,
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
            } else {
                illegalArgumentMessages += "Must provide input_paths"
            }

            if (output_table.isEmpty) {
                illegalArgumentMessages += "Must provide output_table."
            }

            // Validate that delete_after and delete_before are calendar hours.
            // This is to prevent manual mistakes of partial-hours being deleted
            if (delete_after.minute.get() != 0 ||
                delete_after.second.get() != 0 ||
                delete_after.millis.get() != 0) {
                illegalArgumentMessages += "delete_after must be a calendar hour with no minute/second/millis."
            }
            if (delete_before.minute.get() != 0 ||
                delete_before.second.get() != 0 ||
                delete_before.millis.get() != 0) {
                illegalArgumentMessages += "delete_before must be a calendar hour with no minute/second/millis."
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
                """Paths to input data.  Every directory is expected to be in hourly
                  |time-partitioned format""".stripMargin,
            "input_format" ->
                s"""Format of the input data, used when reading data with Spark DataFrameReader.
                   |Must be one of ${SupportedInputFormats.mkString(",")}""".stripMargin,
            "input_schema_base_uris" ->
                """The base URIs to retrieve the input schema.""",
            "input_schema_uri" ->
                """The base-relative URI to retrieve the input schema.""",
            "output_table" ->
                "Iceberg fully qualified table name e.g. 'database.table'",
            "partition_time_field" ->
                """The field to be used to delete data in the output-table for the delete_after
                  |and delete_before parameters. This allows the job to be idempotent and not
                  |generate duplicated data in case of reruns.
                  |""".stripMargin,
            "delete_after" ->
                """The hour at which deletion starts in the destination table (included), ISO-8601 formatted date time.""",
            "delete_before" ->
                """The hour at which deletion stops in the destination table(excluded), ISO-8601 formatted date time.""",
            "transform_functions" ->
                s"""Comma separated list of fully qualified module.ObjectNames. The objects'
                   |apply methods should take a DataFrame and a HivePartition and return
                   |a DataFrame. These functions can be used to map from the input DataFrames
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
              |Hourly input folders -> Iceberg table.
              |
              |Given an input base path and an hour to process, this will clean the iceberg table from
              |existing data for that hour and insert data found in the input folders.
              |
              |Example:
              |  spark-submit --class org.wikimedia.analytics.refinery.job.refine.RefineIcebergDataset refinery-job.jar \
              |  # read configs out of this file
              |   --config_file                     /etc/refinery/refine_event.properties \
              |   # Override and/or set other configs on the CLI
              |   --input_paths                     '/wmf/data/raw/eventlogging_legacy/eventlogging_NavigationTiming/year=2024/month=02/day=15/hour=00' \
              |   --input_format                    'json' \
              |   --input_schema_base_uris          'https://schema.discovery.wmnet/repositories/secondary/jsonschema/'
              |   --input_schema_uri                'analytics/legacy/navigationtiming/1.6.0'
              |   --output_table                    'event.navigationtiming'
              |   --main_time_field                 'meta.dt'
              |   --delete_after                    '2024-02-15T00:00:00' \
              |   --delete_before                   '2024-02-16T00:00:00' \
              |   --transform_functions             'org.wikimedia.analytics.refinery.job.refine.extractDatacenterFromFolderName,org.wikimedia.analytics.refinery.job.refine.filter_allowed_domains,org.wikimedia.analytics.refinery.job.refine.remove_canary_events,org.wikimedia.analytics.refinery.job.refine.deduplicate,org.wikimedia.analytics.refinery.job.refine.geocode_ip,org.wikimedia.analytics.refinery.job.refine.parse_user_agent,org.wikimedia.analytics.refinery.job.refine.add_is_wmf_domain,org.wikimedia.analytics.refinery.job.refine.add_normalized_host,org.wikimedia.analytics.refinery.job.refine.normalizeFieldNamesAndWidenTypes'
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

        val eventSchemaLoader: EventSchemaLoader = buildEventSchemaLoader(config.input_schema_base_uris)
        val reader = RawRefineDataReader(spark,
            RefineHelper.loadSparkSchema(eventSchemaLoader, config.input_schema_uri),
            config.input_format,
            config.dataframe_reader_options,
            config.corrupt_record_failure_threshold)

        apply(reader, config) match {
            case Success(_) => sys.exit(0)
            case Failure(_) => sys.exit(1)
        }
    }

    /**
      * Refine the input_paths for the given hour.
      *
      * @return true if the target refinement succeeded, false otherwise.
      */
    def apply(
        reader: RawRefineDataReader,
        config: Config,
    ): Try[Unit] = {
        try {
            log.info(s"Refining ${config.input_paths} folder(s) into Iceberg table ${config.output_table} " +
                s"between ${config.delete_after} and ${config.delete_before}.")

            val inputDf = reader.readInputDataFrameWithSchemaURI(config.input_paths)

            val transformedDf = RefineHelper.applyTransforms(inputDf, config.transform_functions)

            val recordCount = DataFrameToTable.icebergDeleteInsert(
                transformedDf,
                config.output_table,
                config.partition_time_field,
                config.delete_before,
                config.delete_after
            )

            log.info(
                s"Successfully refined ${config.input_paths} into Iceberg table ${config.output_table} " +
                    s"between ${config.delete_after} and ${config.delete_before} " +
                    s"(total # refined records: $recordCount).")
            Success()
        } catch {
            case e: Exception => {
                log.error(s"Failed refinement of dataset ${config.input_paths} into ${config.output_table} " +
                    s"between ${config.delete_after} and ${config.delete_before}.", e)
                throw e
            }
        }
    }
}
