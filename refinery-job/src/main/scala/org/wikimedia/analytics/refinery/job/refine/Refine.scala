package org.wikimedia.analytics.refinery.job.refine

import java.util

import com.github.nscala_time.time.Imports._
import io.circe.Decoder
import cats.syntax.either._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.joda.time.format.DateTimeFormatter
import org.wikimedia.analytics.refinery.core.{LogHelper, ReflectUtils, Utilities}
import org.wikimedia.analytics.refinery.core.config._
import org.wikimedia.analytics.refinery.core.jsonschema.{EventLoggingSchemaLoader, EventSchemaLoader}
import org.wikimedia.analytics.refinery.spark.connectors.DataFrameToHive
import org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.util.matching.Regex
import scala.util.{Success, Try}

// TODO: support append vs overwrite?
// TODO: Hive Table Locking?


/**
  * Looks for hourly input partition directories with data that need refinement,
  * and refines them into Hive Parquet tables using DataFrameToHive.
  */
object Refine extends LogHelper with ConfigHelper {
    type TransformFunction = DataFrameToHive.TransformFunction

    /**
      * Config class for use config files and args.
      */
    case class Config(
        input_path: String,
        input_path_regex: String,
        input_path_regex_capture_groups: Seq[String],
        output_path: String,
        database: String,
        since: DateTime                                 = DateTime.now - 24.hours, // 1 day ago
        until: DateTime                                 = DateTime.now,
        input_path_datetime_format: DateTimeFormatter   = DateTimeFormat.forPattern("'hourly'/yyyy/MM/dd/HH"),
        table_whitelist_regex: Option[Regex]            = None,
        table_blacklist_regex: Option[Regex]            = None,
        transform_functions: Seq[TransformFunction]     = Seq(),
        ignore_failure_flag: Boolean                    = false,
        parallelism: Option[Int]                        = None,
        compression_codec: String                       = "snappy",
        limit: Option[Int]                              = None,
        dry_run: Boolean                                = false,
        should_email_report: Boolean                    = false,
        smtp_uri: String                                = "mx1001.wikimedia.org:25",
        from_email: String                              = s"refine@${java.net.InetAddress.getLocalHost.getCanonicalHostName}",
        to_emails: Seq[String]                          = Seq(),
        ignore_done_flag: Boolean                       = false,
        schema_base_uris: Seq[String]                   = Seq.empty,
        schema_field: String                            = "/$schema",
        dataframereader_options: Map[String, String]    = Map()
    )

    object Config {
        // This is just used to ease generating help message with default values.
        // Required configs are set to dummy values.
        val default = Config("", "", Seq(), "", "")

        val propertiesDoc: ListMap[String, String] = ListMap(
            "config_file <file1.properties,files2.properties>" ->
                """Comma separated list of paths to properties files.  These files parsed for
                  |for matching config parameters as defined here.""",
            "input_path" ->
                """Path to input datasets.  This directory is expected to contain
                  |directories of individual (topic) table datasets.  E.g.
                  |/path/to/raw/data/{myprefix_dataSetOne,myprefix_dataSetTwo}, etc.
                  |Each of these subdirectories will be searched for refine target
                  |partitions.""",
            "output_path" ->
                """Base path of output data and of external Hive tables.  Completed refine targets
                  |are expected to be found here in subdirectories based on extracted table names.""",
            "database" ->
                s"Hive database name in which to manage refined Hive tables.  Default: ${default.database}",
            "since" ->
                s"""Look for refine targets since this date time.  This may either be given as an integer
                   |number of hours ago, or an ISO-8601 formatted date time.  Default: ${default.since}""",
            "until" ->
                s"""Look for refine targets until this date time.  This may either be given as an integer
                   |number of hours ago, or an ISO-8601 formatted date time.  Default: ${default.until}""",
            "input_path_regex" ->
                s"""This should match the input partition directory hierarchy starting from the
                   |dataset base path, and should capture the table name and the partition values.
                   |Along with input-capture, this allows arbitrary extraction of table names and and
                   |partitions from the input path.  You are required to capture at least "table"
                   |using this regex.  The default will match an hourly bucketed Camus import hierarchy,
                   |using the topic name as the table name.  Default: ${default.input_path_regex}""",
            "input_path_regex_capture_groups" ->
                s"""This should be a comma separated list of named capture groups
                   |corresponding to the groups captured byt input-regex.  These need to be
                   |provided in the order that the groups are captured.  This ordering will
                   |also be used for partitioning.  Default: ${default.input_path_regex_capture_groups.mkString(",")}""",
            "input_path_datetime_format" ->
                s"""This DateTimeFormat will be used to generate all possible partitions since
                   |the given lookback-hours in each dataset directory.  This format will be used
                   |to format a DateTime to input directory partition paths.  The finest granularity
                   |supported is hourly.  Every hour in the past lookback-hours will be generated,
                   |but if you specify a less granular format (e.g. daily, like "daily"/yyyy/MM/dd),
                   |the code will reduce the generated partition search for that day to 1, instead of 24.
                   |The default is suitable for generating partitions in an hourly bucketed Camus
                   |import hierarchy.  Default: ${default.input_path_datetime_format}""",
            "table_whitelist_regex" ->
                "Whitelist regex of table names to look for.",
            "table_blacklist_regex" ->
                "Blacklist regex of table names to skip.",
            "transform_functions" ->
                s"""Comma separated list of fully qualified module.ObjectNames.  The objects'
                   |apply methods should take a DataFrame and a HivePartition and return
                   |a DataFrame.  These functions can be used to map from the input DataFrames
                   |to a new ones, applying various transformations along the way.  Default: none""",
            "ignore_failure_flag" ->
                s"""Set this if you also want all discovered partitions with _REFINE_FAILED files to be
                   |(re)refined. Default: ${default.ignore_failure_flag}""",
            "ignore_done_flag" ->
                s"""Set this if you also want all discovered partitions with _REFINED files to be
                   |(re)refined. Default: ${default.ignore_done_flag}""",
            "parallelism" ->
                """Refine into up to this many tables in parallel.  Individual partitions
                  |destined for the same Hive table will be refined serially.
                  |Default: the number of local CPUs (i.e. what Scala parallel
                  |collections uses).""",
            "compression_codec" ->
                s"Value of spark.sql.parquet.compression.codec, default: ${default.compression_codec}",
            "limit" ->
                s"""Only refine this many partitions directories.  This is useful while
                   |testing to reduce the number of refinements to do at once.  Default:
                   |no limit.""",
            "dry_run" ->
                s"""Set to true if no action should actually be taken.  Instead, targets
                   |to refine will be printed, but they will not be refined.  NOTE: You
                   |actually set this to a value, e.g. --dry_run=true.  A valueless flag
                   |will not work.
                   |Default: ${default.dry_run}""",
            "should_email_report" ->
                s"""Set this flag if you want an email report of any missing refine targets.
                   |Default: ${default.should_email_report}""",
            "smtp_uri" ->
                s"SMTP server host:port. Default: mx1001.wikimedia.org.  Default: ${default.smtp_uri}",
            "from_email" ->
                s"Email report from sender email address.  Default: ${default.from_email}",
            "to_emails" ->
                s"Email report recipient email addresses (comma separated):  Default: ${default.to_emails.mkString(",")}.",
            "schema_base_uris" ->
                s"""If given, the input data is assumed to be JSONSchemaed event data.  An EventSparkSchemaLoader will
                  |be instantiated with an EventSchemaLoader that uses these URI prefixes to load schemas URIs found at
                  |schema_field.  If this is given as 'eventlogging', the special case EventLoggingSchemaLoader
                  |will be used, and the value of schema_field will be ignored.  Default: None""",
            "schema_field" ->
                s"""Will be used to extract the schema URI from event data.  This is a JsonPath pointer.
                   |Default: ${default.schema_field}""",
            "dataframereader_options" ->
                s"""Comma separated list of key:value pairs to use as options to DataFrameReader
                   |when reading the input DataFrame."""
        )

        val usage: String =
            """
            |Input Datasets -> Partitioned Hive Parquet tables.
            |
            |Given an input base path, this will search all subdirectories for input
            |partitions to convert to Parquet backed Hive tables.  This was originally
            |written to work with JSON data imported via Camus into hourly buckets, but
            |should be configurable to work with any regular directory hierarchy.
            |
            |Example:
            |  spark-submit --class org.wikimedia.analytics.refinery.job.refine.Refine refinery-job.jar \
            |  # read configs out of this file
            |   --config_file                 /etc/refinery/refine_eventlogging_eventbus.properties \
            |   # Override and/or set other configs on the CLI
            |   --input_path                  /wmf/data/raw/event \
            |   --output_path                 /user/otto/external/eventbus5' \
            |   --database                    event \
            |   --since                       24 \
            |   --input_regex                 '.*(eqiad|codfw)_(.+)/hourly/(\d+)/(\d+)/(\d+)/(\d+)' \
            |   --input_regex_capture_groups  'datacenter,table,year,month,day,hour' \
            |   --table_blacklist_regex       '.*page_properties_change.*'
            |"""
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
        if (args.contains("--help")) {
            println(help(Config.usage, Config.propertiesDoc))
            sys.exit(0)
        }

        val spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
        // if not running in yarn, make spark log level quieter.
        if (spark.conf.get("spark.master") != "yarn") {
            spark.sparkContext.setLogLevel("WARN")
        }

        val config = loadConfig(args)

        // Call apply with spark and Config properties as parameters
        val allSucceeded = apply(spark, config)

        // Exit with proper exit val if not running in YARN.
        if (spark.conf.get("spark.master") != "yarn") {
            if (allSucceeded) sys.exit(0) else sys.exit(1)
        }
    }

    def loadConfig(args: Array[String]): Config = {
        val config = try {
            configureArgs[Config](args)
        } catch {
            case e: ConfigHelperException =>
                log.fatal(e.getMessage + ". Aborting.")
                sys.exit(1)
        }
        log.info("Loaded configuration:\n" + prettyPrint(config))
        config
    }

    /**
      * Refine all discovered RefineTargets.
      *
      * @return true if all targets needing refinement succeeded, false otherwise.
      */
    def apply(spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate())(
        input_path: String,
        input_path_regex: String,
        input_path_regex_capture_groups: Seq[String],
        output_path: String,
        database: String,
        since: DateTime                                 = Config.default.since,
        until: DateTime                                 = Config.default.until,
        input_path_datetime_format: DateTimeFormatter   = Config.default.input_path_datetime_format,
        table_whitelist_regex: Option[Regex]            = Config.default.table_whitelist_regex,
        table_blacklist_regex: Option[Regex]            = Config.default.table_blacklist_regex,
        transform_functions: Seq[TransformFunction]     = Config.default.transform_functions,
        ignore_failure_flag: Boolean                    = Config.default.ignore_failure_flag,
        parallelism: Option[Int]                        = Config.default.parallelism,
        compression_codec: String                       = Config.default.compression_codec,
        limit: Option[Int]                              = Config.default.limit,
        dry_run: Boolean                                = Config.default.dry_run,
        should_email_report: Boolean                    = Config.default.should_email_report,
        smtp_uri: String                                = Config.default.smtp_uri,
        from_email: String                              = Config.default.from_email,
        to_emails: Seq[String]                          = Config.default.to_emails,
        ignore_done_flag: Boolean                       = Config.default.ignore_done_flag,
        schema_base_uris: Seq[String]                   = Config.default.schema_base_uris,
        schema_field: String                            = Config.default.schema_field,
        dataframereader_options: Map[String, String]    = Config.default.dataframereader_options
    ): Boolean = {
        // Initial setup - Spark Conf and Hadoop FileSystem
        spark.conf.set("spark.sql.parquet.compression.codec", compression_codec)

        // Ensure that inputPathPatternCaptureGroups contains "table", as this is needed
        // to determine the Hive table name we will refine into.
        if (!input_path_regex_capture_groups.contains("table")) {
            throw new RuntimeException(
                s"Invalid <input-capture> $input_path_regex_capture_groups. " +
                 "Must at least contain 'table' as a named capture group."
            )
        }

        // Combine the inputPathPattern with the capture groups to build a regex that
        // will use aliases for the named groups.  This will be used to extract
        // table and partitions out of the inputPath.
        val inputPathRegex = new Regex(
            input_path_regex,
            input_path_regex_capture_groups: _*
        )

        log.info(
            s"Looking for targets to refine in $input_path between " +
            s"$since and $until"
        )

        // If given a schema_base_uri, assume that this is a schema uri to use for
        // looking up schemas from events. Create an appropriate EventSparkSchemaLoader.
        val schemaLoader = schema_base_uris match {
            case Seq() => ExplicitSchemaLoader(None)
            // eventlogging is a special case.
            case Seq("eventlogging") => new EventSparkSchemaLoader(new EventLoggingSchemaLoader())
            case baseUris: Seq[String] => {
                new EventSparkSchemaLoader(new EventSchemaLoader(
                    baseUris.asJava,
                    schema_field
                ))
            }
        }

        // Need RefineTargets for every existent input partition since pastCutoffDateTime
        val targetsToRefine = RefineTarget.find(
            spark,
            new Path(input_path),
            new Path(output_path),
            database,
            input_path_datetime_format,
            inputPathRegex,
            since,
            until,
            schemaLoader
        )
        // Filter for tables in whitelist, filter out tables in blacklist,
        // and filter the remaining for targets that need refinement.
        .filter(_.shouldRefine(
            table_whitelist_regex, table_blacklist_regex, ignore_failure_flag, ignore_done_flag
        ))

        // At this point, targetsToRefine will be a Seq of RefineTargets in our targeted
        // time range that need refinement, either because they haven't yet been refined,
        // or the input data has changed since the previous refinement.

        // Return now if we didn't find any targets to refine.
        if (targetsToRefine.isEmpty) {
            log.debug(s"No targets needing refinement were found in $input_path")
            return true
        }

        // Locally parallelize the targets.
        // If limit, then take only the first limit input targets.
        // This is mainly only useful for testing.
        val targets = limit match {
            case Some(_) => targetsToRefine.take(limit.get).par
            case None    => targetsToRefine.par
        }

        // If custom parallelism was specified, create a new ForkJoinPool for this
        // parallel collection with the provided parallelism level.
        if (parallelism.isDefined) {
            targets.tasksupport = new ForkJoinTaskSupport(
                new ForkJoinPool(parallelism.get)
            )
        }

        val targetsByTable = targets.groupBy(_.tableName)

        log.info(
            s"Refining ${targets.length} dataset partitions in into tables " +
            s"${targetsByTable.keys.mkString(", ")} with local " +
            s"parallelism of ${targets.tasksupport.parallelismLevel}..."
        )

        if (dry_run)
            log.warn("NOTE: dry_run was set to true, nothing will actually be refined.")

        // Loop over the inputs in parallel and refine them to
        // their Hive table partitions.  jobStatuses should be a
        // iterable of Trys as Success/Failures.
        val jobStatusesByTable = targetsByTable.map { case (table, tableTargets) =>
            // We need tableTargets to run in serial instead of parallel.  When a table does
            // not yet exist, we want the first target here to issue a CREATE, while the
            // next one to use the created table, or ALTER it if necessary.  We don't
            // want multiple CREATEs for the same table to happen in parallel.
            if (!dry_run)
                table -> refineTargets(spark, tableTargets.seq, transform_functions, dataframereader_options)
            // If dry_run was given, don't refine, just map to Successes.
            else
                table -> tableTargets.seq.map(Success(_))
        }

        // Log successes and failures.
        val successesByTable = jobStatusesByTable.map(t => t._1 -> t._2.filter(_.isSuccess))
        val failuresByTable = jobStatusesByTable.map(t => t._1 -> t._2.filter(_.isFailure))

        var hasFailures = false
        if (successesByTable.nonEmpty) {
            for ((table, successes) <- successesByTable.filter(_._2.nonEmpty)) {
                val totalRefinedRecordCount = targetsByTable(table).map(_.recordCount).sum
                log.info(
                    s"Successfully refined ${successes.length} of ${targetsByTable(table).size} " +
                    s"dataset partitions into table $table (total # refined records: $totalRefinedRecordCount)"
                )
            }
        }

        // Collect a string of failures that we might email as a report later.
        var failureMessages = ""
        if (failuresByTable.nonEmpty) {

            for ((table, failures) <- failuresByTable.filter(_._2.nonEmpty)) {
                // Log each failed refinement.
                val message =
                    s"The following ${failures.length} of ${targetsByTable(table).size} " +
                    s"dataset partitions for table $table failed refinement:\n\t" +
                    failures.mkString("\n\t")

                log.error(message)
                failureMessages += "\n\n" + message

                hasFailures = true
            }

            failureMessages += s"\n\napplicationId: ${spark.sparkContext.applicationId}"
        }

        // If we should send this as a failure email report
        // (and this is not a dry run), do it!
        if (hasFailures && should_email_report && !dry_run) {
            val smtpHost = smtp_uri.split(":")(0)
            val smtpPort = smtp_uri.split(":")(1)

            log.info(s"Sending failure email report to ${to_emails.mkString(",")}")
            Utilities.sendEmail(
                smtpHost,
                smtpPort,
                from_email,
                to_emails.toArray,
                s"Refine failure report for $input_path -> $output_path",
                failureMessages
            )
        }

        // Return true if no failures, false otherwise.
        !hasFailures
    }


    /**
      * Overloaded apply that takes a SparkSession and a Refine.Config object
      *
      * @param spark
      * @param config
      * @return
      */
    def apply(spark: SparkSession, config: Config): Boolean = {
        // NOTE: tupled and unapply don't work with more than 22 parameters :(
        //(apply(spark) _).tupled(Config.unapply(config).get)

        apply(spark)(
            config.input_path,
            config.input_path_regex,
            config.input_path_regex_capture_groups,
            config.output_path,
            config.database,
            config.since,
            config.until,
            config.input_path_datetime_format,
            config.table_whitelist_regex,
            config.table_blacklist_regex,
            config.transform_functions,
            config.ignore_failure_flag,
            config.parallelism,
            config.compression_codec,
            config.limit,
            config.dry_run,
            config.should_email_report,
            config.smtp_uri,
            config.from_email,
            config.to_emails,
            config.ignore_done_flag,
            config.schema_base_uris,
            config.schema_field,
            config.dataframereader_options
        )
    }

    /**
      * Given a Seq of RefineTargets, this runs DataFrameToHive on each one.
      *
      * @param spark               SparkSession
      * @param targets             Seq of RefineTargets to refine
      * @param transformFunctions  The list of transform functions to apply
      * @param dataFrameReaderOptions Map of options to provide to DataFrameReader.
      * @return
      */
    def refineTargets(
        spark: SparkSession,
        targets: Seq[RefineTarget],
        transformFunctions: Seq[TransformFunction],
        dataFrameReaderOptions: Map[String, String] = Map()
    ): Seq[Try[RefineTarget]] = {
        targets.map(target => {
            log.info(s"Beginning refinement of $target...")

            try {
                val partDf = new PartitionedDataFrame(
                    target.inputDataFrame(dataFrameReaderOptions),
                    target.partition
                )
                val insertedDf = DataFrameToHive(
                    spark,
                    partDf,
                    () => target.writeDoneFlag(),
                    transformFunctions
                )
                val recordCount = insertedDf.df.count

                log.info(
                    s"Finished refinement of dataset $target. " +
                    s"(# refined records: $recordCount)"
                )

                target.success(recordCount)
            }
            catch {
                case e: Exception =>
                    log.error(s"Failed refinement of dataset $target.", e)
                    target.writeFailureFlag()
                    target.failure(e)
            }
        })
    }
}
