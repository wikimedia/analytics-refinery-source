package org.wikimedia.analytics.refinery.job.refine.cli

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.FailFastMode
import org.wikimedia.analytics.refinery.job.refine.RefineHelper.{TransformFunction, buildEventSchemaLoader}
import org.wikimedia.analytics.refinery.job.refine.cli.EvolveHiveTable.DEFAULT_PARTITIONS
import org.wikimedia.analytics.refinery.job.refine.cli.RefineHiveDataset.SupportedInputFormats
import org.wikimedia.analytics.refinery.job.refine.{RawRefineDataReader, RefineTarget, SparkEventSchemaLoader, WikimediaEventSparkSchemaLoader}
import org.wikimedia.analytics.refinery.tools.LogHelper
import org.wikimedia.analytics.refinery.tools.config._

import java.net.URI
import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer

object EvolveAndRefineToHiveTable
    extends LogHelper
        with ConfigHelper
        with TransformFunctionsConfigHelper {


    case class Config(
        // EvolveHiveTable fields
        table: String,
        schema_uri: String,
        schema_base_uris: Seq[String] = WikimediaEventSparkSchemaLoader.BASE_SCHEMA_URIS_DEFAULT,
        dry_run: Boolean = false,
        timestamps_as_strings: Boolean = true,
        location: Option[String] = None,
        partition_columns: ListMap[String, String] = DEFAULT_PARTITIONS,
        transform_functions: Seq[TransformFunction] = Seq.empty,

        // RefineHiveDataset fields (Some EvolveHiveTable fields are reused)
        input_paths: Seq[String],
        input_format: String = "json",
        partition_paths: Seq[String],
        dataframe_reader_options: Map[String, String] = Map("mode" -> FailFastMode.name),
        corrupt_record_failure_threshold: Int = 1,
        spark_job_scale: String = "small",
        ignore_missing_paths: Boolean = false,
    ) {
        // validate Refine settings
        private def validateRefine(): Unit = {
            val errs = ArrayBuffer[String]()
            if (input_paths.isEmpty) {
                errs += "Must provide input_paths"
            } else {
                if (!RefineTarget.supportedInputFormats.contains(input_format))
                    errs += s"Invalid input_format $input_format"
                if (input_paths.length != partition_paths.length)
                    errs += "input_paths and partition_paths must have same length"
            }
            if (table.isEmpty || !table.contains("."))
                errs += "Must provide output table as db.table"
            val pat = """(?:[a-zA-Z_]+=[^/]+/?)+""".r
            if (partition_paths.forall(p => pat.findFirstIn(p).isEmpty))
                errs += "Invalid partition_paths; use k1=v1/.../kn=vn"
            if (errs.nonEmpty) throw new IllegalArgumentException(errs.mkString("\n"))
        }
        // no-op for evolve (reuse default partition_columns)
        validateRefine()
    }


    object Config {
        val propertiesDoc: ListMap[String, String] = ListMap(
            "table" ->
                """The fully qualified table name to write the data to. Must be in the form db.table
                  |e.g. event.navigationtiming""".stripMargin,
            "schema_uri" ->
                """The base-relative URI to retrieve the input schema.""",
            "schema_base_uris" ->
                """The base URIs to retrieve the input schema.""",
            "dry_run" ->
                """If true, do not perform the modification on the table neither write the data to Hive.
                  |This is useful for testing.
                  |Default: false""".stripMargin,
            "timestamps_as_strings" ->
                """If true, timestamps are stored as strings in the Hive table.
                  |Default: true""".stripMargin,
            "location" ->
                """The location of the Hive table. Default: None""".stripMargin,
            "partition_columns" ->
                """The partition columns of the Hive table. Default: None""".stripMargin,
            "transform_functions" ->
                s"""Comma separated list of fully qualified module.ObjectNames. The objects'
                   |apply methods should take a DataFrame and a HivePartition and return
                   |a DataFrame.  These functions can be used to map from the input DataFrames
                   |to a new ones, applying various transformations along the way.
                   |NOTE: The functions will be executed in the given order (it matters:).
                   |Default: none""".stripMargin,
            "input_paths" ->
                """Paths to input data. Every directory is expected to be in hourly
                  |time-partitioned format""".stripMargin,
            "input_format" ->
                s"""Format of the input data, used when reading data with Spark DataFrameReader.
                   |Defaults to 'json'.
                   |Must be one of ${SupportedInputFormats.mkString(",")}""".stripMargin,
            "partition_paths" ->
                """List of output Hive partitions specified in path form k1=v1/k2=v2/.../kn=vn
                  |e.g. datacenter=eqiad/year=2024/month=2/day=15/hour=0
                  |Its order should match the order of the input_paths.
                  |Note that these are not necessarily the actual file paths, but
                  |are just used to add literal columns and values to the data before
                  |writing it.
                  |Actual file partitioning is handled by the table schema spec when writing.
                  |""".stripMargin,
            "dataframe_reader_options" ->
                """Comma separated list of key:value pairs to use as options to DataFrameReader
                  |when reading the input DataFrame.
                  |Default: mode:FAILFAST""".stripMargin,
            "spark_job_scale" ->
                """The scale of the Spark job (small, medium, large). This is used to set the number of
                  |partitions of the DataFrame. Default: small""".stripMargin,
            "ignore_missing_paths" ->
                """If true, ignore missing paths when reading input data. Default: false""",
            "corrupt_record_failure_threshold" ->
                """The maximum number of corrupt records allowed before failing the job.
                  |Default: 1""".stripMargin,
        )

        val usage: String = "Evolve Hive table according to stream schema then refine data into it"

        /**
         * Loads Config from args
         */
        def apply(args: Array[String]): Config = {
            try {
                val config = configureArgs[Config](args)
                log.info("Loaded Refine config:\n" + prettyPrint(config))
                config
            } catch {
                case e: ConfigHelperException =>
                    log.fatal(e.getMessage + ". Aborting.")
                    sys.exit(1)
            }
        }
    }

    def main(args: Array[String]): Unit = {
        if (args.isEmpty || args.contains("--help")) {
            println(help(Config.usage, Config.propertiesDoc))
            sys.exit(0)
        }

        val config  = configureArgs[Config](args)
        val spark  = SparkSession.builder().enableHiveSupport().getOrCreate()
        adjustLoggingLevelToSparkMaster(spark)

        // 1) Evolve Hive table schema
        val evolver = new EvolveHiveTable(buildEventSchemaLoader(config.schema_base_uris), spark)
        val didEvolve = evolver.evolveHiveTableWithSchema(
            config.table,
            URI.create(config.schema_uri),
            config.timestamps_as_strings,
            config.location,
            config.transform_functions,
            config.partition_columns,
            config.dry_run,
        )

        // abort if dry_run requested
        if (config.dry_run) sys.exit(if (didEvolve) 0 else 1)

        // 2) Refine data into the evolved table
        val loader = buildEventSchemaLoader(config.schema_base_uris)
        val sparkSchemaLoader = SparkEventSchemaLoader(loader, timestampsAsStrings = config.timestamps_as_strings)
        val reader = RawRefineDataReader(
            spark,
            sparkSchemaLoader.load(URI.create(config.schema_uri)),
            config.input_format,
            config.dataframe_reader_options,
            config.corrupt_record_failure_threshold,
            config.ignore_missing_paths
        )

        val refineHiveDatasetConfig: RefineHiveDataset.Config = RefineHiveDataset.Config(
            input_paths = config.input_paths,
            schema_uri = config.schema_uri,
            table = config.table,
            partition_paths = config.partition_paths,
            input_format = config.input_format,
            schema_base_uris = config.schema_base_uris,
            transform_functions = config.transform_functions,
            dataframe_reader_options = config.dataframe_reader_options,
            corrupt_record_failure_threshold = config.corrupt_record_failure_threshold,
            spark_job_scale = config.spark_job_scale,
            ignore_missing_paths = config.ignore_missing_paths
        )
        val success = RefineHiveDataset.apply(reader, refineHiveDatasetConfig)
        sys.exit(if (success) 0 else 1)
    }
}