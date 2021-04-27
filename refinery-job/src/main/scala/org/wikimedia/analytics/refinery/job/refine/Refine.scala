package org.wikimedia.analytics.refinery.job.refine

import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex
import cats.syntax.either._
import com.github.nscala_time.time.Imports._
import io.circe.Decoder
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.PermissiveMode
import org.joda.time.format.DateTimeFormatter
import org.wikimedia.analytics.refinery.core.{LogHelper, ReflectUtils, Utilities}
import org.wikimedia.analytics.refinery.core.config._
import org.wikimedia.analytics.refinery.spark.connectors.DataFrameToHive
import org.wikimedia.analytics.refinery.spark.sql.HiveExtensions._
import org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame
import org.wikimedia.eventutilities.core.event.WikimediaDefaults

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
        input_path                          : Option[String] = None,
        input_path_regex                    : Option[String] = None,
        input_path_regex_capture_groups     : Option[Seq[String]] = None,
        input_path_datetime_format          : DateTimeFormatter = DateTimeFormat.forPattern("'hourly'/yyyy/MM/dd/HH"),
        input_database                      : Option[String] = None,
        output_path                         : Option[String] = None,
        output_database                     : Option[String] = None,
        since                               : DateTime = DateTime.now - 24.hours,
        until                               : DateTime = DateTime.now,
        table_include_regex                 : Option[Regex] = None,
        table_exclude_regex                 : Option[Regex] = None,
        transform_functions                 : Seq[TransformFunction] = Seq(),
        ignore_failure_flag                 : Boolean = false,
        ignore_done_flag                    : Boolean = false,
        parallelism                         : Option[Int] = None,
        compression_codec                   : String = "snappy",
        limit                               : Option[Int] = None,
        dry_run                             : Boolean = false,
        should_email_report                 : Boolean = false,
        smtp_uri                            : String = "mx1001.wikimedia.org:25",
        from_email                          : String = s"refine@${java.net.InetAddress.getLocalHost.getCanonicalHostName}",
        to_emails                           : Seq[String] = Seq(),
        schema_base_uris                    : Seq[String] = Seq.empty,
        schema_field                        : String = "/$schema",
        dataframereader_options             : Map[String, String] = Map(),
        merge_with_hive_schema_before_read  : Boolean = false,
        corrupt_record_failure_threshold    : Integer = 1
    ) {
        // Call validate now so we can throw at instantiation if this Config is not valid.
        validate()

        /**
          * Validates that configs as provided make sense.
          * Throws IllegalArgumentException if not.
          */
         private def validate(): Unit = {
            val illegalArgumentMessages: ArrayBuffer[String] = ArrayBuffer()

            if (input_path.isDefined && input_database.isDefined) {
                illegalArgumentMessages +=
                    "Only one of input_path or input_database is allowed."
            }
            if (input_path.isDefined) {
                if (input_path_regex.isEmpty) {
                    illegalArgumentMessages +=
                        "If using input_path, input_path_regex must be provided."
                }
                if (input_path_regex_capture_groups.isEmpty) {
                    illegalArgumentMessages +=
                        "If using input_path, input_path_regex_capture_groups must be provided."
                }
                // Ensure that input_path_regex_capture_groups contains "table", as this is needed
                // to determine the Hive table name we will refine into.
                else if (!input_path_regex_capture_groups.get.contains("table")) {
                    illegalArgumentMessages +=
                        s"Invalid <input-capture> $input_path_regex_capture_groups. " +
                        "Must at least contain 'table' as a named capture group."
                }
            } else if (input_database.isEmpty) {
                illegalArgumentMessages += "Must provide one of input_path or input_database."
            }

            if (output_path.isEmpty) {
                illegalArgumentMessages += "Must provide output_path."
            }

            if (output_database.isEmpty) {
                illegalArgumentMessages += "Must provide output_database."
            }

            if (illegalArgumentMessages.nonEmpty) {
                throw new IllegalArgumentException(
                    illegalArgumentMessages.mkString("\n")
                )
            }
        }

        /**
          * Compiled input_path_regex with capture groups.
          */
        def inputPathRegex: Regex = {
            // NOTE: this could be a val, but doing so messes with ConfigHelper.prettyPrint.
            // prettyPrint uses reflection to match case class constructor property names with
            // class vals, and if the number of constructor properties is different than the number
            // of vals, it falls back to toString.
            new Regex(
                // We know these are defined, as validate will have thrown if not.
                input_path_regex.get,
                input_path_regex_capture_groups.get: _*
            )
        }
    }

    object Config {
        // This is just used to ease generating help message with default values.
        // Required configs are set to dummy values so that validate() will succeed.
        val default: Config = new Config(
            input_database = Some("_INPUT_EXAMPLE_"),
            output_database = Some("_OUTPUT_EXAMPLE_"),
            output_path = Some("_OUTPUT_EXAMPLE_")
        )

        val propertiesDoc: ListMap[String, String] = ListMap(
            "config_file <file1.properties,files2.properties>" ->
                """Comma separated list of paths to properties files.  These files parsed for
                  |for matching config parameters as defined here.""",
            "input_path" ->
                """Base path to input datasets.  This directory is expected to contain
                  |directories of individual (topic) table datasets.  E.g.
                  |/path/to/raw/data/{myprefix_dataSetOne,myprefix_dataSetTwo}, etc.
                  |Each of these subdirectories will be searched for refine target
                  |partitions.  This parameter is incompatible with input_database.""",
            "input_database" ->
                """Hive database name from which to search for refine targets.
                  |This parameter is incompatible with input_path.""",
            "output_path" ->
                """Base path of output data and of external Hive tables.  Completed refine targets
                  |are expected to be found here in subdirectories based on extracted table names.""",
            "output_database" ->
                s"Hive database name in which to manage refined Hive tables.",
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
                   |using the topic name as the table name.  Ignored if using input_database.
                   |This is matched against directory paths, NOT Hive normalized table names, so
                   |even though extracted table name will eventually be normalized, this
                   |regex will match against the directory paths before normalization.
                   |Default: ${default.input_path_regex}""",
            "input_path_regex_capture_groups" ->
                s"""This should be a comma separated list of named capture groups
                   |corresponding to the groups captured byt input-regex.  These need to be
                   |provided in the order that the groups are captured.  This ordering will
                   |also be used for partitioning.  Ignored if using input_database.""",
            "input_path_datetime_format" ->
                s"""This DateTimeFormat will be used to generate all possible partitions since
                   |the given since property in each dataset directory.  This format will be used
                   |to format a DateTime to input directory partition paths.  The finest granularity
                   |supported is hourly.  Every hour in the past lookback-hours will be generated,
                   |but if you specify a less granular format (e.g. daily, like "daily"/yyyy/MM/dd),
                   |the code will reduce the generated partition search for that day to 1, instead of 24.
                   |The default is suitable for generating partitions in an hourly bucketed Camus
                   |import hierarchy.  Ignored if using input_database.
                   |Default: ${default.input_path_datetime_format}""",
            "table_include_regex" ->
                """Regex of table names to look for. Matched against Hive normalized table names,
                |so make sure your regex is normalized (lowercased, etc.) accordingly.""",
            "table_exclude_regex" ->
                """Regex of table names to skip. Matched against Hive normalized table names,
                |so make sure your regex is normalized (lowercased, etc.) accordingly.""",
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
                  |will be used, and the value of schema_field will be ignored.  This paramater
                  |is ignored if using input_database, as the schema used will be that of the input table.
                  |Default: None""",
            "schema_field" ->
                s"""Will be used to extract the schema URI from event data.  This is a JsonPath pointer.
                   |Default: ${default.schema_field}""",
            "dataframereader_options" ->
                s"""Comma separated list of key:value pairs to use as options to DataFrameReader
                   |when reading the input DataFrame.""",
            "merge_with_hive_schema_before_read" ->
                s"""If true, the loaded schema will be merged with the Hive schema and used when
                   |reading input data.  This might be useful if the Hive schema has
                   |more information (e.g. extra fields) than the schema loaded for the input
                   |data. (This is needed for legacy EventLogging metawiki schemas.)""".stripMargin,
            "corrupt_record_failure_threshold" ->
                s"""In DataFrameReader ${PermissiveMode.name} mode, if the number of corrupt records
                   |on the input DataFrame is greater than or equal to this, the Refine of a target
                   |will fail. If corrupt_record_failure_threshold is <= 0, any number of corrupt
                   |records will succeed. Default: ${default.corrupt_record_failure_threshold}"""
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
            |   --outout_database             event \
            |   --since                       24 \
            |   --input_regex                 '.*(eqiad|codfw)_(.+)/hourly/(\d+)/(\d+)/(\d+)/(\d+)' \
            |   --input_regex_capture_groups  'datacenter,table,year,month,day,hour' \
            |   --table_exclude_regex       '.*page_properties_change.*'
            |"""

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
        if (args.contains("--help")) {
            println(help(Config.usage, Config.propertiesDoc))
            sys.exit(0)
        }

        val spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
        // if not running in yarn, make spark log level quieter.
        if (spark.conf.get("spark.master") != "yarn") {
            spark.sparkContext.setLogLevel("WARN")
        }

        val config = Config(args)

        // Call apply with spark and Config properties as parameters
        val allSucceeded = apply(spark)(config)

        // Exit with proper exit val if not running in YARN.
        if (spark.conf.get("spark.master") != "yarn") {
            if (allSucceeded) sys.exit(0) else sys.exit(1)
        }
    }

    /**
      * Refine all discovered RefineTargets.
      *
      * @return true if all targets needing refinement succeeded, false otherwise.
      */
    def apply(
        spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    )(
        config: Config = Config.default
    ): Boolean = {
        // Initial setup - Spark Conf and Hadoop FileSystem
        spark.conf.set("spark.sql.parquet.compression.codec", config.compression_codec)

        // At this point, targetsToRefine will be a Seq of RefineTargets in our targeted
        // time range that need refinement, either because they haven't yet been refined,
        // or the input data has changed since the previous refinement.
        val targetsToRefine: Seq[RefineTarget] = getRefineTargets(
            spark,
            config
        )
        // Filter for targets that should be refined based on done and failure flags.
        .filter(_.shouldRefine(config.ignore_failure_flag, config.ignore_done_flag))

        // Return now if we didn't find any targets to refine.
        if (targetsToRefine.isEmpty) {
            log.info(
                s"No targets needing refinement were found in " +
                s"${config.input_path.getOrElse(config.input_database.get)}."
            )
            return true
        }

        // Locally parallelize the targets.
        // If limit, then take only the first limit input targets.
        // This is mainly only useful for testing.
        val targets = config.limit match {
            case Some(_) => targetsToRefine.take(config.limit.get).par
            case None    => targetsToRefine.par
        }

        // If custom parallelism was specified, create a new ForkJoinPool for this
        // parallel collection with the provided parallelism level.
        if (config.parallelism.isDefined) {
            targets.tasksupport = new ForkJoinTaskSupport(
                new ForkJoinPool(config.parallelism.get)
            )
        }

        val targetsByTable = targets.groupBy(_.tableName)

        log.info(
            s"Refining ${targets.length} dataset partitions in into tables " +
            s"${targetsByTable.keys.mkString(", ")} with local " +
            s"parallelism of ${targets.tasksupport.parallelismLevel}..."
        )

        if (config.dry_run)
            log.warn("NOTE: dry_run was set to true, nothing will actually be refined.")

        // Loop over the inputs in parallel and refine them to
        // their Hive table partitions.  jobStatuses should be a
        // iterable of Trys as Success/Failures.
        val jobStatusesByTable = targetsByTable.map { case (table, tableTargets) =>
            // We need tableTargets to run in serial instead of parallel.  When a table does
            // not yet exist, we want the first target here to issue a CREATE, while the
            // next one to use the created table, or ALTER it if necessary.  We don't
            // want multiple CREATEs for the same table to happen in parallel.
            if (!config.dry_run) {
                table -> refineTargets(
                    spark,
                    tableTargets.seq,
                    config.transform_functions,
                    config.corrupt_record_failure_threshold
                )
            } // If dry_run was given, don't refine, just map to Successes.
            else {
                table -> tableTargets.seq.map(Success(_))
            }
        }

        // Log successes:
        // Map of table name to Seq[Success[RefineTarget]]
        val successesByTable = jobStatusesByTable
            .mapValues(_.filter(_.isSuccess))
            .filter(_._2.nonEmpty)
        if (successesByTable.nonEmpty) {
            for ((table, successes) <- successesByTable) {
                val totalRefinedRecordCount = targetsByTable(table).map(_.recordCount).sum
                log.info(
                    s"Successfully refined ${successes.length} of ${targetsByTable(table).size} " +
                    s"dataset partitions into table $table (total # refined records: $totalRefinedRecordCount)"
                )
            }
        }

        // Log failures, and send an email report of failures if configured to do so:
        // Map of table name to Seq[RefineTargetException]
        val exceptionsByTable = jobStatusesByTable.mapValues({ tries: Seq[Try[RefineTarget]] =>
            tries.collect { case Failure(e) => e }
        }).filter(_._2.nonEmpty)

        // Build a report string about any encountered failures.
        var hasFailures = false
        var reportBody = ""
        var earliestFailureDt = config.since
        var latestFailureDt = config.until
        if (exceptionsByTable.nonEmpty) {
            val inputDescription = if (config.input_path.isDefined) {
                s"path ${config.input_path.get}"
            } else {
                s"database ${config.input_database}"
            }
            val outputDescription =
                s"database ${config.output_database.get} (${config.output_path.get})"

            reportBody += s"Refine failures for $inputDescription -> $outputDescription\n\n"

            for ((table, exceptions) <- exceptionsByTable) {
                // Get the RefineTargets out of all the RefineTargetExceptions
                // and sort by dt.  Then use the earliest and latest failed
                // RefineTarget to build helpful information about how to rerun.
                val sortedFailedTargets: Seq[RefineTarget] = exceptions
                    .collect { case e: RefineTargetException => e.target }
                    .sortBy  { t => t.partition.dt.getOrElse(new DateTime(0)) }

                earliestFailureDt = sortedFailedTargets.head match {
                    case t if t.partition.dt.isDefined && t.partition.dt.get < earliestFailureDt =>
                        t.partition.dt.get
                    case _ => earliestFailureDt
                }

                latestFailureDt = sortedFailedTargets.last match {
                    case t if t.partition.dt.isDefined && t.partition.dt.get > latestFailureDt =>
                        t.partition.dt.get
                    case _ => latestFailureDt
                }

                // Log each failed refinement.
                val message =
                    s"The following ${exceptions.length} of ${targetsByTable(table).size} " +
                    s"dataset partitions for output table $table failed refinement:\n\t" +
                    exceptions.mkString("\n\t")

                log.error(message)
                reportBody += "\n\n" + message
                hasFailures = true
            }

            reportBody += s"\n\napplicationId: ${spark.sparkContext.applicationId}"

            // Get a usable table_include_regex for rerunOptions.
            // We need to use the target.partition.table here, as the
            // tableName keys in exceptionsByTable are fully qualified like `db`.`table`,
            // and we need just table for the table_include_regex
            val tablesWithFailuresRegex = exceptionsByTable.collect {
                case (_, rtes: Seq[RefineTargetException]) => rtes.head.target.partition.table
            }.mkString("|")

            val rerunOptions = Seq(
                "--ignore_failure_flag=true",
                s"--table_include_regex='$tablesWithFailuresRegex'",
                s"--since='${earliestFailureDt.hourOfDay().roundFloorCopy()}'",
                s"--until='${latestFailureDt.hourOfDay().roundCeilingCopy()}'"
            )
            reportBody += s"\n\nTo rerun, use the following options:\n"
            reportBody += rerunOptions.mkString(" ")
        }

        // If we should send this as a failure email report
        // (and this is not a dry run), do it!
        if (hasFailures && config.should_email_report && !config.dry_run) {
            val smtpHost = config.smtp_uri.split(":")(0)
            val smtpPort = config.smtp_uri.split(":")(1)


            val jobName = spark.conf.get("spark.app.name")
            val subject = s"Refine failures for job $jobName"

            log.info(s"Sending failure email report to ${config.to_emails.mkString(",")}")
            Utilities.sendEmail(
                smtpHost,
                smtpPort,
                config.from_email,
                config.to_emails.toArray,
                subject,
                reportBody
            )
        }

        // Return true if no failures, false otherwise.
        !hasFailures
    }

    /**
      * Using Refine.Config, finds RefineTarget from either input_path or input_database.
      * @param spark
      * @param config
      * @return
      */
    def getRefineTargets(
        spark: SparkSession,
        config: Config
    ): Seq[RefineTarget] = {
        // if input_path, search for RefineTargets in the filesystem
        // by matching partitions out of file paths.
        if (config.input_path.isDefined) {
            getRefineTargetsFromFS(
                spark,
                config.input_path.get,
                config.inputPathRegex,
                config.input_path_datetime_format,
                config.output_path.get,
                config.output_database.get,
                config.since,
                config.until,
                config.schema_base_uris,
                config.schema_field,
                config.table_include_regex,
                config.table_exclude_regex,
                config.dataframereader_options,
                config.merge_with_hive_schema_before_read
            )
        } else if (config.input_database.isDefined) {
            // Else find RefineTargets using Hive.
            // This can be done if the inputs are already Hive tables.,
            // and Hive knows all about partitions.
            getRefineTargetsFromDB(
                spark,
                config.input_database.get,
                config.output_path.get,
                config.output_database.get,
                config.since,
                config.until,
                config.table_include_regex,
                config.table_exclude_regex
            )
        } else {
            throw new IllegalArgumentException(
                "Must provide one of input_path or input_database."
            )
        }

    }

    /**
      * Use inputPath and regexes to find RefineTargets from the filesystem.
      * See RefineTarget.find for more documentation.
      */
    def getRefineTargetsFromFS(
        spark                     : SparkSession,
        inputPath                 : String,
        inputPathRegex            : Regex,
        inputPathDateTimeFormatter: DateTimeFormatter,
        outputPath                : String,
        outputDatabase            : String,
        since                     : DateTime,
        until                     : DateTime,
        schemaBaseUris            : Seq[String] = Config.default.schema_base_uris,
        schemaField               : String = Config.default.schema_field,
        tableIncludeRegex         : Option[Regex] = None,
        tableExcludeRegex         : Option[Regex] = None,
        dfReaderOptions           : Map[String, String] = Config.default.dataframereader_options,
        useMergedSchemaForRead    : Boolean = Config.default.merge_with_hive_schema_before_read
    ): Seq[RefineTarget] = {
        log.info(
            s"Looking for targets to refine in ${inputPath} between $since and $until..."
        )

        // If given a schema_base_uri, assume that this is a schema uri to use for
        // looking up schemas from events. Create an appropriate EventSparkSchemaLoader.
        val schemaLoader =  schemaBaseUris match {
            case Seq() => ExplicitSchemaLoader(None)
            // eventlogging is a special case.
            case Seq("eventlogging") =>
                new EventSparkSchemaLoader(WikimediaDefaults.EVENTLOGGING_SCHEMA_LOADER)
            case baseUris: Seq[String] =>
                EventSparkSchemaLoader(baseUris, loadLatest=true, Some(schemaField))
        }

        RefineTarget.find(
            spark,
            new Path(inputPath),
            inputPathDateTimeFormatter,
            inputPathRegex,
            new Path(outputPath),
            outputDatabase,
            since,
            until,
            schemaLoader,
            dfReaderOptions,
            useMergedSchemaForRead,
            tableIncludeRegex,
            tableExcludeRegex
        )
    }

    /**
     * Use inputDatabase to find RefineTargets.  See RefineTarget.find for more docs.
     */
    def getRefineTargetsFromDB(
        spark: SparkSession,
        inputDatabase: String,
        outputPath: String,
        outputDatabase: String,
        since: DateTime,
        until: DateTime,
        tableIncludeRegex: Option[Regex] = None,
        tableExcludeRegex: Option[Regex] = None
    ): Seq[RefineTarget] = {
        log.info(
            s"Looking for targets to refine in the Hive `${inputDatabase}` " +
            s"database between $since and $until..."
        )

        RefineTarget.find(
            spark,
            inputDatabase,
            new Path(outputPath),
            outputDatabase,
            since,
            until,
            tableIncludeRegex,
            tableExcludeRegex
        )
    }


    /**
      * Given a Seq of RefineTargets, this runs DataFrameToHive on each one.
      *
      * @param spark
      *     SparkSession
      * @param targets
      *     Seq of RefineTargets to refine
      * @param transformFunctions
      *     The list of transform functions to apply
      * @param corruptRecordFailureThreshold
      *     If a target's DataFrameReader mode == PERMISSIVE and the
      *     target.addCorruptRecordColumnIfReaderModePermissive, corrupt records
      *     encountered during reading the input data will be stored
      *     in the resulting input DataFrame as columnNameOfCorruptRecord.
      *     If the number of these corrupt records is greater or equal to
      *     corruptRecordFailureThreshold, the Refine for the target will fail
      *     and an exception will be thrown.  Otherwise, an error will be logged.
      *     If this is negative, any number of corrupt records will pass.
      *     NOTE: Corrupt records will be filtered out of the final DataFrame to refine,
      *     and it will not have the columnNameOfCorruptRecord column.
      *     Default: 1
      * @return
      */
    def refineTargets(
        spark: SparkSession,
        targets: Seq[RefineTarget],
        transformFunctions: Seq[TransformFunction],
        corruptRecordFailureThreshold: Int = 1
    ): Seq[Try[RefineTarget]] = {
        targets.map(target => {
            log.info(s"Beginning refinement of $target...")

            try {
                val partDf = getInputPartitionedDataFrame(target, corruptRecordFailureThreshold)

                // as a side effect, spark writes a _SUCCESS flag here
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
            } catch {
                case e: Exception => {
                    log.error(s"Failed refinement of dataset $target.", e)
                    target.writeFailureFlag()
                    val f = target.failure(e)
                    f
                }
            }
        })
    }

    /**
      * Gets the RefineTarget's input PartitionedDataFrame.
      *
      * If the input DataFrameReader used PERMISSIVE mode
      * and stored corrupt records in columnNameOfCorruptRecord,
      * this function will deal with those corrupt records.
      * If the number of corrupt records is >= corruptRecordFailureThreshold,
      * (and corruptRecordFailureThreshold > 0) an exception will be thrown.
      * If corruptRecordFailureThreshold is <= 0 any number of corrupt records will pass.
      *
      * Once corrupt records pass the corruptRecordFailureThreshold check,
      * the records with a non-NULL columnNameOfCorruptRecord will be removed and
      * the columnNameOfCorruptRecord column will be dropped.  The
      * returned PartitionedDataFrame will not have columnNameOfCorruptRecord.
      * @param target
      * @param corruptRecordFailureThreshold
      * @return
      */
    def getInputPartitionedDataFrame(
        target: RefineTarget,
        corruptRecordFailureThreshold: Int = 1
    ): PartitionedDataFrame = {
        // Read the input data
        val workingPartDf = target.inputPartitionedDataFrame
        // Caching workingPartDf.df is a slight performance enhancement, but it is mostly
        // here as a workaround to a safeguard added in Spark 2.3 that won't allow
        // this type of query.  File based DataFrames with corruptRecordColumnName added
        // apparently don't work as expected, since the values of corruptRecordColumnName
        // are added dynamically by spark.  Caching the DataFrame causes it to no
        // longer be file based.  Without this, we get
        // org.apache.spark.sql.AnalysisException: Since Spark 2.3, the queries from
        // raw JSON/CSV files are disallowed when the
        // referenced columns only include the internal corrupt record column
        // See also:
        // https://issues.apache.org/jira/browse/SPARK-21610
        // https://github.com/apache/spark/pull/18865
        // https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/json/JsonFileFormat.scala#L110-L122
        workingPartDf.df.cache()

        val corruptRecordColumnName = target.dfReaderOptions("columnNameOfCorruptRecord")

        // If the input DataFrame has corruptRecordColumnName and
        // mode == PERMISSIVE, handle the corrupt records.
        // NOTE: we don't use the RefineTarget's addCorruptRecordColumnIfReaderModePermissive
        // boolean property for this check, as it is possible for an input DataFrame schema
        // to have the corrupt record column without it being added by RefineTarget
        // before reading.
        if (
            target.dfReaderOptions("mode") == PermissiveMode.name &&
            workingPartDf.df.hasColumn(corruptRecordColumnName)
        ) {
            val corruptRecordDf = workingPartDf.df
                .where(s"`$corruptRecordColumnName` IS NOT NULL")

            if (!corruptRecordDf.isEmpty) {
                // TODO:
                //  Do something useful with the corrupt record data,
                //  like save it to a side output, and/or write to a refine statistics
                //  table about Refine successes and failures.

                val inputRecordCount = workingPartDf.df.count()
                val corruptRecordCount = corruptRecordDf.count()
                // Get the value of the first corrupt record for logging purposes.
                val firstCorruptRecord: String = corruptRecordDf
                    .select(s"$corruptRecordColumnName").head().getString(0)

                val corruptRecordPercentage = (
                    corruptRecordCount.toFloat /
                    inputRecordCount.toFloat
                ) * 100.0f

                val corruptRecordMessage =
                    s"Encountered $corruptRecordCount corrupt records " +
                    s"out of $inputRecordCount total input records " +
                    s"($corruptRecordPercentage%) when reading $target input data"

                if (corruptRecordFailureThreshold > 0 &&
                    corruptRecordCount >= corruptRecordFailureThreshold
                ) {
                    throw new Exception(
                        s"$corruptRecordMessage, which is greater than " +
                        s"corruptRecordFailureThreshold $corruptRecordFailureThreshold. " +
                        s"First corrupt record:\n$firstCorruptRecord"
                    )
                }
                else {
                   log.warn(
                       s"$corruptRecordMessage. First corrupt record:\n" + firstCorruptRecord
                    )
                }
            }

            // If we get here, there were fewer than corruptRecordFailureThreshold
            // corrupt records. Drop the corrupt records from the DataFrame
            // and remove the corrupt record column so that the data we insert
            // is all non-corrupt data.
            workingPartDf.copy(df=workingPartDf.df
                .where(s"$corruptRecordColumnName IS NULL")
                .drop(corruptRecordColumnName)
            )
        } else {
            workingPartDf
        }
    }

}
