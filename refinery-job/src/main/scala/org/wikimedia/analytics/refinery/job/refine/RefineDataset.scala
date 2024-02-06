package org.wikimedia.analytics.refinery.job.refine

import cats.syntax.either._
import com.github.nscala_time.time.Imports._
import io.circe.Decoder
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.util.FailFastMode
import org.wikimedia.analytics.refinery.core.{HivePartition, ReflectUtils}
import org.wikimedia.analytics.refinery.spark.connectors.DataFrameToHive
import org.wikimedia.analytics.refinery.spark.sql.HiveExtensions._
import org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame
import org.wikimedia.analytics.refinery.tools.LogHelper
import org.wikimedia.analytics.refinery.tools.config._

import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

object RefineDataset extends LogHelper with ConfigHelper {

    type TransformFunction = DataFrameToHive.TransformFunction

    val SupportedInputFormats: Set[String] = Set("parquet", "json", "avro")

    /**
      * Config class for use config files and args.
      */
    case class Config(
        input_paths: Seq[String],
        input_format: String,
        input_schema_base_uri: String,
        input_schema_uri: String,
        output_table: String,
        partition_time_field: String,
        delete_after: DateTime,
        delete_before: DateTime,
        transform_functions: Seq[TransformFunction] = Seq.empty,
        dataframereader_options: Map[String, String] = Map("mode" -> FailFastMode.name)
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
            "input_schema_base_uri" ->
                """The base URI to retrieve the input schema.""",
            "input_schema_uri" ->
                """The base-relative URI to retrieve the input schema.""",
            "output_table" ->
                "Hive 'database.table' name in which to manage refined Hive tables.",
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
                   |a DataFrame.  These functions can be used to map from the input DataFrames
                   |to a new ones, applying various transformations along the way.
                   |NOTE: The functions will be executed in the given order (it matters:).
                   |Default: none""".stripMargin,
            "dataframereader_options" ->
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
              |  spark-submit --class org.wikimedia.analytics.refinery.job.refine.RefineSingleApp refinery-job.jar \
              |  # read configs out of this file
              |   --config_file                     /etc/refinery/refine_event.properties \
              |   # Override and/or set other configs on the CLI
              |   --input_paths                     '/wmf/data/raw/eventlogging_legacy/eventlogging_NavigationTiming/year=2024/month=02/day=15/hour=00' \
              |   --input_format                    'json' \
              |   --input_schema_base_uri           'https://schema.discovery.wmnet/repositories/secondary/jsonschema/'
              |   --input_schema_uri                'analytics/legacy/navigationtiming/1.6.0'
              |   --output_table                    'event.navigationtiming'
              |   --main_time_field                 'meta.dt'
              |   --delete_after                    '2024-02-15T00:00:00' \
              |   --delete_before                   '2024-02-16T00:00:00' \
              |   --tranform_functions              'org.wikimedia.analytics.refinery.job.refine.extractDatacenterFromFolderName,org.wikimedia.analytics.refinery.job.refine.filter_allowed_domains,org.wikimedia.analytics.refinery.job.refine.remove_canary_events,org.wikimedia.analytics.refinery.job.refine.deduplicate,org.wikimedia.analytics.refinery.job.refine.geocode_ip,org.wikimedia.analytics.refinery.job.refine.parse_user_agent,org.wikimedia.analytics.refinery.job.refine.add_is_wmf_domain,org.wikimedia.analytics.refinery.job.refine.add_normalized_host,org.wikimedia.analytics.refinery.job.refine.normalizeFieldNamesAndWidenTypes'
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

    /**
      * Convert from comma separated package.ObjectNames to Object callable apply() TransformFunctions.
      */
    implicit val decodeTransformFunctions: Decoder[Seq[TransformFunction]] = Decoder.decodeString.emap { s =>
        Either.catchNonFatal(
            s.split(",").map { objectName =>
                val transformMirror = ReflectUtils.getStaticMethodMirror(objectName)
                // Lookup the object's apply method as a reflect MethodMirror, and wrap
                // it in a anonymous function that has the signature expected by
                // DataFrameToHive's transformFunction parameter.
                val wrapperFn: TransformFunction = {
                    case (partDf) =>
                        log.debug(s"Applying ${transformMirror.receiver} to ${partDf.partition}")
                        transformMirror(partDf).asInstanceOf[PartitionedDataFrame]
                }
                wrapperFn
            }.toSeq
        ).leftMap(t =>
            throw new RuntimeException(s"Failed parsing '$s' into transform functions.", t)
        )
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
        apply(spark)(config) match {
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
        spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    )(
        config: Config
    ): Try[Unit] = {
        try {
            log.info(s"Refining ${config.input_paths} folder(s) into table ${config.output_table} " +
                s"between ${config.delete_after} and ${config.delete_before}.")

            val inputDf = readInputDataFrame(spark, config)

            val transformedDf = applyTransformFunctions(inputDf, config)

            val recordCount = writeToIcebergOutputTable(transformedDf, config)

            log.info(
                s"Successfully refined ${config.input_paths} into table ${config.output_table} " +
                    s"between ${config.delete_after} and ${config.delete_before} (total # refined records: $recordCount).")
            Success()
        } catch {
            case e: Exception => {
                log.error(s"Failed refinement of dataset ${config.input_paths} into ${config.output_table} " +
                    s"between ${config.delete_after} and ${config.delete_before}.", e)
                throw e
            }
        }
    }

    private def readInputDataFrame(spark: SparkSession, config: Config): DataFrame = {
        val schemaLoader = EventSparkSchemaLoader(
            Seq(config.input_schema_base_uri),
            loadLatest=false,
            timestampsAsStrings = false
        )
        val schemaForRead = schemaLoader.loadSchema(config.input_schema_uri)

        val reader = schemaForRead match {
            case None => spark.read
            case Some(s) => config.input_format match {
                case "json" | "text" => spark.read.schema(s.makeNullable())
                case _ => spark.read.schema(s)
            }
        }

        reader
            .options(config.dataframereader_options)
            .format(config.input_format)
            .load(config.input_paths: _*)
    }

    private def applyTransformFunctions(df: DataFrame, config: Config): DataFrame = {
        // Creating a partitioned-dataframe with no partition to abuse transform-functions
        val fakeHivePartition = HivePartition(config.output_table, ListMap.empty[String, String]).copy(location = Some(""))
        val inputPartDf = new PartitionedDataFrame(df, fakeHivePartition)

        // Add the Hive partition columns and apply any other configured transform functions,
        // and then normalize (lowercase top level fields names, widen certain types, etc.).
        // Note: The partition object is not used in transform-functions except for logging,
        // so we're safe using a fake partition as we do
        config.transform_functions
            .foldLeft(inputPartDf)((currPartDf, fn) => fn(currPartDf))

            // Keep number of partitions to reset it after DataFrame API changes it
            // Since the spark context is used across multiple jobs, we don't want
            // to use a global setting.
            .df.repartition(df.rdd.getNumPartitions)
    }

    private def writeToIcebergOutputTable(df: DataFrame, config: Config): Long = {
        //  Converting data to destination schema
        val dstSchema = df.sparkSession.table(config.output_table).schema
        val toWriteDf = df.convertToSchema(dstSchema).cache()

        // First delete data from the output table where meta.dt is within the processed hour
        log.info(s"Deleting existing data from ${config.output_table} between ${config.delete_after} and ${config.delete_before}.")
        val dtFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")
        df.sparkSession.sql(
            s"""
                 DELETE FROM ${config.output_table}
                 WHERE ${config.partition_time_field} BETWEEN '${dtFormatter.print(config.delete_after)}'
                                                     AND '${dtFormatter.print(config.delete_before)}'
                 """)

        // Then insert new data
        log.info(s"Writing new data to ${config.output_table} between ${config.delete_after} and ${config.delete_before}.")

        toWriteDf.writeTo(config.output_table).append()

        val recordCount = toWriteDf.count

        // Explicitly un-cache the cached DataFrame.
        toWriteDf.unpersist

        recordCount
    }
}
