package org.wikimedia.analytics.refinery.job

import org.apache.log4j.LogManager
import scopt.OptionParser

import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

import org.joda.time.Hours
import org.joda.time.format.DateTimeFormatter
import com.github.nscala_time.time.Imports._

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import org.wikimedia.analytics.refinery.core.{HivePartition, SparkJsonToHive, Utilities}


// TODO: ERROR Hive: Table otto not found: default.otto table not found ???
// TODO: support append vs overwrite?
// TODO: Hive Table Locking?

/**
  * Looks for hourly input partition directories with JSON data that need refinement,
  * and refines them into Hive Parquet tables using SparkJsonToHive.
  */
object JsonRefine {

    private val log = LogManager.getLogger("JsonRefine")
    private val iso8601DateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss")
    /**
      * Config class for CLI argument parser using scopt
      */
    case class Params(
        inputBasePath: String                      = "",
        outputBasePath: String                     = "",
        databaseName: String                       = "default",
        sinceDateTime: DateTime                    = DateTime.now - 192.hours, // 8 days ago
        untilDateTime: DateTime                    = DateTime.now,
        inputPathPattern: String                   = ".*/(.+)/hourly/(\\d{4})/(\\d{2})/(\\d{2})/(\\d{2}).*",
        inputPathPatternCaptureGroups: Seq[String] = Seq("table", "year", "month", "day", "hour"),
        inputPathDateTimeFormat: DateTimeFormatter = DateTimeFormat.forPattern("'hourly'/yyyy/MM/dd/HH"),
        tableWhitelistRegex: Option[Regex]         = None,
        tableBlacklistRegex: Option[Regex]         = None,
        doneFlag: String                           = "_REFINED",
        parallelism: Option[Int]                   = None,
        compressionCodec: String                   = "snappy",
        isSequenceFile: Boolean                    = true,
        limit: Option[Int]                         = None,
        dryRun: Boolean                            = false,
        shouldEmailReport: Boolean                 = false,
        smtpURI: String                            = "mx1001.wikimedia.org:25",
        fromEmail: String                          = s"jsonrefine@${java.net.InetAddress.getLocalHost.getCanonicalHostName}",
        toEmails: Seq[String]                      = Seq("analytics-alerts@wikimedia.org")
    )


    // Support implicit Regex conversion from CLI opt.
    implicit val scoptRegexRead: scopt.Read[Regex] =
        scopt.Read.reads { _.r }

    // Support implicit Option[Regex] conversion from CLI opt.
    implicit val scoptOptionRegexRead: scopt.Read[Option[Regex]] =
        scopt.Read.reads {s => Some(s.r) }

    // Support implicit DateTimeFormatter conversion from CLI opt.
    implicit val scoptDateTimeFormatterRead: scopt.Read[DateTimeFormatter] =
        scopt.Read.reads { s => DateTimeFormat.forPattern(s) }

    // Support implicit DateTime conversion from CLI opt.
    // The opt can either be given in integer hours ago, or
    // as an ISO-8601 date time.
    implicit val scoptDateTimeRead: scopt.Read[DateTime] =
        scopt.Read.reads { s => {
            if (s.forall(Character.isDigit))
                DateTime.now - s.toInt.hours
            else
                DateTime.parse(s, iso8601DateFormatter)
        }
    }



    /**
      * Define the command line options parser.
      */
    val argsParser = new OptionParser[Params](
        "spark-submit --class org.wikimedia.analytics.refinery.job.JsonRefine refinery-job.jar"
    ) {
        head("""
              |JSON Datasets -> Partitioned Hive Parquet tables.
              |
              |Given an input base path, this will search all subdirectories for input
              |partitions to convert to Parquet backed Hive tables.  This was originally
              |written to work with JSON data imported via Camus into hourly buckets, but
              |should be configurable to work with any regular import directory hierarchy.
              |
              |Example:
              |  spark-submit --class org.wikimedia.analytics.refinery.job.JsonRefine refinery-job.jar \
              |   --input-base-path     /wmf/data/raw/event \
              |   --output-base-path    /user/otto/external/eventbus5' \
              |   --database            event \
              |   --since               24 \
              |   --input-regex         '.*(eqiad|codfw)_(.+)/hourly/(\d+)/(\d+)/(\d+)/(\d+)' \
              |   --input-capture       'datacenter,table,year,month,day,hour' \
              |   --table-blacklist     '.*page_properties_change.*'
              |
              |""".stripMargin, "")

        note("""NOTE: You may pass all of the described CLI options to this job in a single
               |string with --options '<options>' flag.\n""".stripMargin)

        help("help") text "Prints this usage text."

        opt[String]('i', "input-base-path").required().valueName("<path>").action { (x, p) =>
            p.copy(inputBasePath = if (x.endsWith("/")) x.dropRight(1) else x)
        } text
            """Path to input JSON datasets.  This directory is expected to contain
              |directories of individual (topic) table datasets.  E.g.
              |/path/to/raw/data/{myprefix_dataSetOne,myprefix_dataSetTwo}, etc.
              |Each of these subdirectories will be searched for partitions that
              |need to be refined.""".stripMargin.replace("\n", "\n\t") + "\n"

        opt[String]('o', "output-base-path") optional() valueName "<path>" action { (x, p) =>
            p.copy(outputBasePath = if (x.endsWith("/")) x.dropRight(1) else x)
        } text
            """Base path of output data and of external Hive tables.  Each table will be created
              |with a LOCATION in a subdirectory of this path."""
                .stripMargin.replace("\n", "\n\t") + "\n"

        opt[String]('d', "database") optional() valueName "<database>" action { (x, p) =>
            p.copy(databaseName = if (x.endsWith("/")) x.dropRight(1) else x)
        } text "Hive database name in which to manage refined Hive tables.\n"

        opt[DateTime]('s', "since") optional() valueName "<since-date-time>" action { (x, p) =>
            p.copy(sinceDateTime = x)
        } text
            """Refine all data found since this date time.  This may either be given as an integer
              |number of hours ago, or an ISO-8601 formatted date time.  Default: 192 hours ago."""
                .stripMargin.replace("\n", "\n\t") + "\n"

        opt[DateTime]('u', "until") optional() valueName "<until-date-time>" action { (x, p) =>
            p.copy(untilDateTime = x)
        } text
            """Refine all data found until this date time.  This may either be given as an integer
              |number of hours ago, or an ISO-8601 formatted date time.  Default: now."""
                .stripMargin.replace("\n", "\n\t") + "\n"

        opt[String]('R', "input-regex") optional() valueName "<regex>" action { (x, p) =>
            p.copy(inputPathPattern = x)
        } text
            """input-regex should match the input partition directory hierarchy starting from the
              |dataset base path, and should capture the table name and the partition values.
              |Along with input-capture, this allows arbitrary extraction of table names and and
              |partitions from the input path.  You are required to capture at least "table"
              |using this regex.  The default will match an hourly bucketed Camus import hierarchy,
              |using the topic name as the table name.""".stripMargin.replace("\n", "\n\t") + "\n"

        opt[String]('C', "input-capture") optional() valueName "<capture-list>" action { (x, p) =>
            p.copy(inputPathPatternCaptureGroups = x.split(","))
        } text
            """input-capture should be a comma separated list of named capture groups
              |corresponding to the groups captured byt input-regex.  These need to be
              |provided in the order that the groups are captured.  This ordering will
              |also be used for partitioning.""".stripMargin.replace("\n", "\n\t") + "\n"

        opt[DateTimeFormatter]('F', "input-datetime-format") optional() valueName "<format>" action { (x, p) =>
            p.copy(inputPathDateTimeFormat = x)
        } text
            """This DateTimeFormat will be used to generate all possible partitions since
              |the given lookback-hours in each dataset directory.  This format will be used
              |to format a DateTime to input directory partition paths.  The finest granularity
              |supported is hourly.  Every hour in the past lookback-hours will be generated,
              |but if you specify a less granular format (e.g. daily, like "daily"/yyyy/MM/dd),
              |the code will reduce the generated partition search for that day to 1, instead of 24.
              |The default is suitable for generating partitions in an hourly bucketed Camus
              |import hierarchy.
            """.stripMargin.replace("\n", "\n\t") + "\n"

        opt[(Option[Regex])]('w', "table-whitelist") optional() valueName "<regex>" action { (x, p) =>
            p.copy(tableWhitelistRegex = x)
        } text "Whitelist regex of table names to refine.\n".stripMargin

        opt[Option[Regex]]('b', "table-blacklist") optional() valueName "<regex>" action { (x, p) =>
            p.copy(tableBlacklistRegex = x)
        } text "Blacklist regex of table names to skip.\n".stripMargin

        opt[String]('D', "done-flag") optional() valueName "<filename>" action { (x, p) =>
            p.copy(doneFlag = x)
        } text
            """When a partition is successfully refined, this file will be created in the
              |output partition path with the binary timestamp of the input source partition's
              |modification timestamp.  This allows subsequent runs that to detect if the input
              |data has changed meaning the partition needs to be re-refined.
              |Default: _REFINED""".stripMargin.replace("\n", "\n\t") + "\n"

        opt[Int]('P', "parallelism") optional() valueName "<parallelism>" action { (x, p) =>
            p.copy(parallelism = Some(x))
        } text
            """Refine into up to this many tables in parallel.  Individual partitions
              |destined for the same Hive table will be refined serially.
              |Defaults to the number of local CPUs (i.e. what Scala parallel
              |collections uses).""".stripMargin.replace("\n", "\n\t") + "\n"


        opt[String]('c', "compression-codec") optional() valueName "<codec>" action { (x, p) =>
            p.copy(compressionCodec = x)
        } text "Value of spark.sql.parquet.compression.codec, default: snappy\n"

        opt[Boolean]('S', "sequence-file") optional() action { (x, p) =>
            p.copy(isSequenceFile = x)
        } text
            """Set to true if the input data is stored in Hadoop Sequence files.
              |Otherwise text is assumed.  Default: true"""
                .stripMargin.replace("\n", "\n\t") + "\n"

        opt[String]('L', "limit") optional() valueName "<limit>" action { (x, p) =>
            p.copy(limit = Some(x.toInt))
        } text
            """Only refine this many partitions directories.  This is useful while
              |testing to reduce the number of refinements to do at once.  Defaults
              |to no limit.""".stripMargin.replace("\n", "\n\t") + "\n"

        opt[Unit]('n', "dry-run") optional() action { (_, p) =>
            p.copy(dryRun = true)
        } text
            """Set to true if no action should actually be taken.  Instead, targets
              |to refine will be printed, but they will not be refined.
              |Default: false"""
                .stripMargin.replace("\n", "\n\t") + "\n"

        opt[Unit]('E', "send-email-report") optional() action { (_, p) =>
            p.copy(shouldEmailReport = true)
        } text
            "Set this flag if you want an email report of any failures during refinement."

        opt[String]('T', "smtp-uri") optional() valueName "<smtp-uri>" action { (x, p) =>
            p.copy(smtpURI = x)
        } text "SMTP server host:port. Default: mx1001.wikimedia.org"

        opt[String]('f', "from-email") optional() valueName "<from-email>" action { (x, p) =>
            p.copy(fromEmail = x)
        } text "Email report from sender email address."

        opt[String]('t', "to-emails") optional() valueName "<to-emails>" action { (x, p) =>
            p.copy(toEmails = x.split(","))
        } text
            "Email report recipient email addresses (comma separated). Default: analytics-alerts@wikimedia.org"

    }

    def main(args: Array[String]): Unit = {
        val params = args.headOption match {
            // Case when our job options are given as a single string.  Split them
            // and pass them to argsParser.
            case Some("--options") =>
                argsParser.parse(args(1).split("\\s+"), Params()).getOrElse(sys.exit(1))
            // Else the normal usage, each CLI opts can be parsed as a job option.
            case _ =>
                argsParser.parse(args, Params()).getOrElse(sys.exit(1))
        }

        // Exit non-zero if if any refinements failed.
        if (apply(params))
            sys.exit(0)
        else
            sys.exit(1)
    }


    /**
      * Given params, refine all discovered JsonTargets.
      *
      * @param params Params
      * @return true if all targets needing refinement succeeded, false otherwise.
      */
    def apply(params: Params): Boolean = {
        // Initial setup - Spark, HiveContext, Hadoop FileSystem
        val conf = new SparkConf()
        val sc = new SparkContext(conf)

        val fs = FileSystem.get(sc.hadoopConfiguration)
        val hiveContext = new HiveContext(sc)
        hiveContext.setConf("spark.sql.parquet.compression.codec", params.compressionCodec)

        // Ensure that inputPathPatternCaptureGroups contains "table", as this is needed
        // to determine the Hive table name we will refine into.
        if (!params.inputPathPatternCaptureGroups.contains("table")) {
            throw new RuntimeException(
                s"Invalid <input-capture> ${params.inputPathPatternCaptureGroups}. " +
                s"Must at least contain 'table' as a named capture group."
            )
        }

        // Combine the inputPathPattern with the capture groups to build a regex that
        // will use aliases for the named groups.  This will be used to extract
        // table and partitions out of the inputPath.
        val inputPathRegex = new Regex(
            params.inputPathPattern,
            params.inputPathPatternCaptureGroups: _*
        )


        log.info(
            s"Looking for JSON targets to refine in ${params.inputBasePath} between " +
            s"${params.sinceDateTime} and ${params.untilDateTime}"
        )

        // Need JsonTargets for every existent input partition since pastCutoffDateTime
        val targetsToRefine = jsonTargetsSince(
            fs,
            new Path(params.inputBasePath),
            params.isSequenceFile,
            new Path(params.outputBasePath),
            params.databaseName,
            params.doneFlag,
            params.inputPathDateTimeFormat,
            inputPathRegex,
            params.sinceDateTime
        )
        // Filter for tables in whitelist, filter out tables in blacklist,
        // and filter the remaining for targets that need refinement.
        .filter(target => shouldRefineJsonTarget(
            target, params.tableWhitelistRegex, params.tableBlacklistRegex
        ))

        // At this point, targetsToRefine will be a Seq of JsonTargets in our targeted
        // time range that need refinement, either because they haven't yet been refined,
        // or the input data has changed since the previous refinement.

        // Return now if we didn't find any targets to refine.
        if (targetsToRefine.isEmpty) {
            log.info(s"No targets needing refinement were found in ${params.inputBasePath}")
            return true
        }

        // Locally parallelize the targets.
        // If params.limit, then take only the first limit input targets.
        // This is mainly only useful for testing.
        val targets = params.limit match {
            case Some(_) => targetsToRefine.take(params.limit.get).par
            case None    => targetsToRefine.par
        }

        // If custom parallelism was specified, create a new ForkJoinPool for this
        // parallel collection with the provided parallelism level.
        if (params.parallelism.isDefined) {
            targets.tasksupport = new ForkJoinTaskSupport(
                new ForkJoinPool(params.parallelism.get)
            )
        }

        val targetsByTable = targets.groupBy(_.tableName)

        log.info(
            s"Refining ${targets.length} JSON dataset partitions in into tables " +
            s"${targetsByTable.keys.mkString(", ")} with local " +
            s"parallelism of ${targets.tasksupport.parallelismLevel}..."
        )

        if (params.dryRun)
            log.warn("NOTE: --dry-run was specified, nothing will actually be refined!")

        // Loop over the inputs in parallel and refine them to
        // their Hive table partitions.  jobStatuses should be a
        // iterable of Trys as Success/Failures.
        val jobStatusesByTable = targetsByTable.map { case (table, tableTargets) => {
            // We need tableTargets to run in serial instead of parallel.  When a table does
            // not yet exist, we want the first target here to issue a CREATE, while the
            // next one to use the created table, or ALTER it if necessary.  We don't
            // want multiple CREATEs for the same table to happen in parallel.
            if (!params.dryRun)
                table -> refineJsonTargets(hiveContext, tableTargets.seq)
            // If --dry-run was given, don't refine, just map to Successes.
            else
                table -> tableTargets.seq.map(Success(_))
        }}

        // Log successes and failures.
        val successesByTable = jobStatusesByTable.map(t => t._1 -> t._2.filter(_.isSuccess))
        val failuresByTable = jobStatusesByTable.map(t => t._1 -> t._2.filter(_.isFailure))

        var hasFailures = false
        if (successesByTable.nonEmpty) {
            for ((table, successes) <- successesByTable.filter(_._2.nonEmpty)) {
                val totalRefinedRecordCount = targetsByTable(table).map(_.recordCount).sum
                log.info(
                    s"Successfully refined ${successes.length} of ${targetsByTable(table).size} " +
                    s"raw JSON dataset partitions into table $table (total # refined records: $totalRefinedRecordCount)"
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
                    s"raw JSON dataset partitions for table $table failed refinement:\n\t" +
                    failures.mkString("\n\t")

                log.error(message)
                failureMessages += "\n\n" + message

                hasFailures = true
            }
        }

        // If we should send this as a failure email report
        // (and this is not a dry run), do it!
        if (hasFailures && params.shouldEmailReport && !params.dryRun) {
            val smtpHost = params.smtpURI.split(":")(0)
            val smtpPort = params.smtpURI.split(":")(1)

            log.info(s"Sending failure email report to ${params.toEmails.mkString(",")}")
            Utilities.sendEmail(
                smtpHost,
                smtpPort,
                params.fromEmail,
                params.toEmails.toArray,
                s"JsonRefine failure report for ${params.inputBasePath} -> ${params.outputBasePath}",
                failureMessages
            )
        }

        // Return true if no failures, false otherwise.
        !hasFailures
    }


    /**
      * Given a JsonTarget, and option whitelist regex and blacklist regex,
      * this returns true if the JsonTarget should be refined, based on regex matching and
      * on output existence and doneFlag content.
      *
      * @param target JsonTarget
      * @param tableWhitelistRegex Option[Regex]
      * @param tableBlacklistRegex Option[Regex]
      * @return
      */
    def shouldRefineJsonTarget(
        target: JsonTarget,
        tableWhitelistRegex: Option[Regex],
        tableBlacklistRegex: Option[Regex]
    ): Boolean = {

        // Filter for targets that will refine to tables that match the whitelist
        if (tableWhitelistRegex.isDefined &&
            !regexMatches(target.partition.table, tableWhitelistRegex.get)
        ) {
            log.info(
                s"$target table ${target.partition.table} does not match table whitelist regex " +
                s"${tableWhitelistRegex.get}', skipping."
            )
            false
        }
        // Filter out targets that will refine to tables that match the blacklist
        else if (tableBlacklistRegex.isDefined &&
            regexMatches(target.partition.table, tableBlacklistRegex.get)
        ) {
            log.info(
                s"$target table ${target.partition.table} matches table blacklist regex " +
                s"'${tableBlacklistRegex.get}', skipping."
            )
            false
        }
        // Finally filter for those that need to be refined (have new data).
        else if (!target.shouldRefine) {
            log.info(
                s"$target does not have new data since the last refine at " +
                s"${target.doneFlagMTime().getOrElse("_unknown_")}, skipping."
            )
            false
        }
        else {
            true
        }
    }


    /**
      * Given a Seq of JsonTargets, this runs SparkJsonToHive on each one.
      *
      * @param hiveContext HiveContext
      * @param targets     Seq of JsonTargets to refine
      * @return
      */
    def refineJsonTargets(
        hiveContext: HiveContext,
        targets: Seq[JsonTarget]
    ): Seq[Try[JsonTarget]] = {
        targets.map(target => {
            log.info(s"Beginning refinement of $target...")

            try {
                val recordCount = SparkJsonToHive(
                    hiveContext,
                    target.inputPath.toString,
                    target.partition,
                    target.inputIsSequenceFile,
                    () => target.writeDoneFlag()
                )

                log.info(
                    s"Finished refinement of JSON dataset $target. " +
                    s"(# refined records: $recordCount)"
                )

                target.success(recordCount)

            }
            catch {
                case e: Exception => {
                    log.error(s"Failed refinement of JSON dataset $target.", e)
                    target.failure(e)
                }
            }
        })
    }


    /**
      * Finds JsonTargets with existent input partition paths between sinceDateTime and untilDateTime.
      * The table and partitions are extracted from inputPath by combining inputPathDateTimeFormatter
      * and inputPathRegex.
      *
      * inputPathDateTimeFormatter will be used to construct the expected inputPath for each
      * input partition directory between sinceDateTime and untilDateTime.  E.g. a formatter
      * with a format of "'hourly'/yyyy/MM/dd/HH" will look for existent inputPaths
      * for every hour in the provided time period, like
      *     $baseInputPath/subdir1/hourly/2017/07/26/00,
      *     $baseInputPath/subdir1/hourly/2017/07/26/01,
      *     $baseInputPath/subdir2/hourly/2017/07/26/00,
      *     $baseInputPath/subdir2/hourly/2017/07/26/01,
      * etc.
      *
      * inputPathRegex is expected to capture named groups that include "table" and any other
      * partition keys.  inputPathRegex's capture groups must contain one named "table".
      * E.g. new Regex(
      *     "(eqiad|codfw)_(.+)/hourly/\\d{4}/\\d{2}/\\d{2}/\\d{2}",
      *     "datacenter", "table", "year", "month", "day", "hour"
      *
      * and an inputPath of
      *     $baseInputPath/eqiad_mediawiki_revision-create/2017/07/26/01
      *
      * Will construct a JsonTarget with table "mediawiki_revision_create" (hyphens are converted
      * to underscores) and partitions datacenter="eqiad",year=2017,month=07,day=26,hour=01
      *
      *
      * @param fs                           Hadoop FileSystem
      *
      * @param baseInputPath                Path to base input datasets.  Each subdirectory
      *                                     is assumed to be a unique dataset with individual
      *                                     partitions.  Every subdirectory's partition
      *                                     paths here must be compatible with the provided
      *                                     values of inputPathDateTimeFormatter and inputPathRegex.
      *
      * @param inputIsSequenceFile          Should be True if the input data files are Hadoop
      *                                     Sequence Files.
      *
      * @param baseTableLocationPath        Path to directory where Hive table data will be stored.
      *                                     $baseTableLocationPath/$table will be the value of the
      *                                     external Hive table's LOCATION.
      *
      * @param databaseName                 Hive database name
      *
      * @param doneFlag                     Done flag file.  A successful refinement will
      *                                     write this file to the output path with
      *                                     the Long timestamp of the inputPath's current mod time.
      *
      * @param inputPathDateTimeFormatter   Formatter used to construct input partition paths
      *                                     in the given time range.
      *
      * @param inputPathRegex               Regex used to extract table name and partition
      *                                     information.
      *
      * @param sinceDateTime                Start date time to look for input partitions.
      *
      * @param untilDateTime                End date time to look for input partitions.
      *                                     Defaults to DateTime.now
      * @return
      */
    def jsonTargetsSince(
        fs: FileSystem,
        baseInputPath: Path,
        inputIsSequenceFile: Boolean,
        baseTableLocationPath: Path,
        databaseName: String,
        doneFlag: String,
        inputPathDateTimeFormatter: DateTimeFormatter,
        inputPathRegex: Regex,
        sinceDateTime: DateTime,
        untilDateTime: DateTime = DateTime.now
    ): Seq[JsonTarget] = {
        val inputDatasetPaths = subdirectoryPaths(fs, baseInputPath)

        // Map all partitions in each inputPaths since pastCutoffDateTime to JsonTargets
        inputDatasetPaths.flatMap { inputDatasetPath =>
            // Get all possible input partition paths for all directories in inputDatasetPath
            // between sinceDateTime and untilDateTime.
            // This will include all possible partition paths in that time range, even if that path
            // does not actually exist.
            val pastPartitionPaths = partitionPathsSince(
                inputDatasetPath.toString,
                inputPathDateTimeFormatter,
                sinceDateTime,
                untilDateTime
            )

            // Convert each possible partition input path into a possible JsonTarget for refinement.
            pastPartitionPaths.map(partitionPath => {
                // Any capturedKeys other than table are expected to be partition key=values.
                val partition = HivePartition(
                    databaseName,
                    baseTableLocationPath.toString,
                    partitionPath.toString,
                    inputPathRegex
                )

                JsonTarget(
                    fs,
                    partitionPath,
                    inputIsSequenceFile,
                    partition,
                    doneFlag
                )
            })
            // We only care about input partition paths that actually exist,
            // so filter out those that don't.
            .filter(_.inputExists())
         }
    }


    /**
      * Returns true of s matches r, else false.
      * @param s    String to match
      * @param r    Regex
      * @return
      */
    def regexMatches(s: String, r: Regex): Boolean = {
        s match {
            case r(_*) => true
            case _     => false
        }
    }


    /**
      * Retruns a Seq of all directory Paths in a directory.
      * @param fs           Hadoop FileSystem
      * @param inDirectory  directory Path in which to look for subdirectories
      * @return
      */
    def subdirectoryPaths(fs: FileSystem, inDirectory: Path): Seq[Path] = {
        fs.listStatus(inDirectory).filter(_.isDirectory).map(_.getPath)
    }


    /**
      * Given 2 DateTimes, this generates a Seq of DateTimes representing all hours
      * between since d1 (inclusive) and d2 (exclusive).  E.g.
      *     DateTime.now -> 2017-08-10T21:42:32.820Z
      *
      *     hoursInBetween(DateTime.now - 2.hours, DateTime.now) ->
      *         Seq(2017-08-10T19:00:00.000Z, 2017-08-10T20:00:00.000Z)
      *
      * In the above example, the current hour is 21, and this function returns
      * the previous two hours.
      *
      * @param d1   sinceDateTime
      * @param d2   untilDateTime
      * @return
      */
    def hoursInBetween(d1: DateTime, d2: DateTime): Seq[DateTime] = {
        val oldestHour = new DateTime(d1, DateTimeZone.UTC).hourOfDay.roundCeilingCopy
        val youngestHour = new DateTime(d2, DateTimeZone.UTC).hourOfDay.roundFloorCopy

        for (h <- 0 to Hours.hoursBetween(oldestHour, youngestHour).getHours) yield {
            oldestHour + h.hours - 1.hours
        }
    }


    /**
      * Given a DateTimeFormatter and 2 DateTimes, this will generate
      * a Seq of Paths for every distinct result of fmt.print(hour) prefixed
      * witih pathPrefix.  If your date formatter generates the same
      * path for multiple hours, only one of those paths will be included
      * in the result.  This way, you can still generate a list more granular partitions, if
      * your data happens to be partitioned at a more granular time bucketing than hourly.
      *
      * @param pathPrefix   Prefix to prepend to every generated partition path
      * @param fmt          Date formatter to use to extract partition paths from hours
      *                     between d1 and d2
      * @param d1           sinceDateTime
      * @param d2           untilDateTime,  Defaults to DateTime.now
      * @return
      */
    def partitionPathsSince(
        pathPrefix: String,
        fmt: DateTimeFormatter,
        d1: DateTime,
        d2: DateTime = DateTime.now
    ): Seq[Path] = {
        hoursInBetween(d1, d2)
            .map(hour => new Path(pathPrefix + "/" + fmt.print(hour)))
            .distinct
    }


    /**
      * Represents a JSON dataset target for refinement.  This mainly exists to reduce the number
      * of parameters we have to pass around between functions here.  An instantiated JsonTarget
      * should contain enough information to use SparkJsonToHive to refine a single Json partition
      * into a Hive Parquet table.
      *
      * @param fs                   Hadoop FileSystem
      * @param inputPath            Full input partition path
      * @param inputIsSequenceFile  If the input is a Hadoop Sequence File
      * @param partition            HivePartition
      * @param doneFlag             Name of file that should be written upon success of
      *                             SparkJsonToHive run.  This can be created by calling
      *                             the writeDoneFlag method.
      */
    case class JsonTarget(
        fs: FileSystem,
        inputPath: Path,
        inputIsSequenceFile: Boolean,
        partition: HivePartition,
        doneFlag: String
    ) {
        /**
          * Easy access to the fully qualified Hive table name.
          */
        val tableName: String = partition.tableName

        /**
          * Easy access to the hive partition path, AKA the output destination path
          */
        val outputPath = new Path(partition.path)

        /**
          * Path to doneFlag in hive table partition output path
          */
        val doneFlagPath = new Path(s"$outputPath/$doneFlag")

        /**
          * Number of records successfully refined for this JsonTarget.
          */
        var recordCount: Long = -1

        /**
          * The mtime of the inputPath at the time this JsonTarget is instantiated.
          * caching this allows us to use the earliest mtime possible to store in doneFlag,
          * in case the inputPath changes while this target is being refined.
          */
        private val inputMTimeCached: Option[DateTime] = inputMTime()

        /**
          * True if the inputPath exists
          * @return
          */
        def inputExists(): Boolean = fs.exists(inputPath)

        /**
          * True if the outputPath exists
          * @return
          */
        def outputExists(): Boolean = fs.exists(outputPath)

        /**
          * True if the outputPath/doneFlag exists
          * @return
          */
        def doneFlagExists(): Boolean = fs.exists(doneFlagPath)

        /**
          * Returns the mtime Long timestamp of inputPath.  inputPath's
          * mtime will change if it or any of its direct files change.
          * It will not change if a content in a subdirectory changes.
          * @return
          */
        def inputMTime(): Option[DateTime] = {
            if (inputExists()) {
                Some(new DateTime(fs.getFileStatus(inputPath).getModificationTime))
            }
            else
                None
        }

        /**
          * Reads the Long timestamp out of the outputPath/doneFlag if it exists.
          * @return
          */
        def doneFlagMTime(): Option[DateTime] = {
            if (doneFlagExists()) {
                val inStream = fs.open(doneFlagPath)
                val mtime = new DateTime(inStream.readUTF())
                inStream.close()
                Some(mtime)
            }
            else
                None
        }

        /**
         * Write out doneFlag file for this output target partition
         *
         * This saves the modification timestamp of the inputPath as it when this target was
         * instantiated.  This will allow later comparison of the contents of doneFlag with the
         * inputPath modification time.  If they are different, the user might decide to rerun
         * SparkJsonToHive for this target, perhaps assuming that there is new
         * data in inputPath.  Note that inputPath directory mod time only changes if
         * its direct content changes, it will not change if something in a subdirectory
         * below it changes.
         */
        def writeDoneFlag(): Unit = {

            val mtime = inputMTimeCached.getOrElse(
                throw new RuntimeException(
                    s"Cannot write done flag, input mod time was not obtained when $this was " +
                        s"instantiated, probably because it did not exist. This should not happen"
                )
            )

            log.info(
                s"Writing done flag file at $doneFlagPath with $inputPath last modification " +
                s"timestamp of $mtime"
            )

            val outStream = fs.create(doneFlagPath)
            outStream.writeUTF(mtime.toString)
            outStream.close()
        }

        /**
          * This target needs refined if:
          * - The output doesn't exist OR
          * - The output doneFlag doesn't exist OR
          * - The input's mtime does not equal the timestamp in the output doneFlag file,
          *   meaning that something has changed in the inputPath since the last time doneFlag
          *   was written.
          *
          * @return
          */
        def shouldRefine(): Boolean = {
            !outputExists() || !doneFlagExists() || inputMTimeCached != doneFlagMTime()
        }

        /**
          * Returns a Failure with e wrapped in a new more descriptive Exception
          * @param e Original exception that caused this failure
          * @return
          */
        def failure(e: Exception): Try[JsonTarget] = {
            Failure(JsonTargetException(
                this, s"Failed refinement of JSON dataset $this. Original exception: $e", e
            ))
        }

        /**
          * Returns Success(this) of this JsonTarget
          * @return
          */
        def success(recordCount: Long): Try[JsonTarget] = {
            this.recordCount = recordCount
            Success(this)
        }

        override def toString: String = {
            s"$inputPath -> $partition"
        }
    }


    /**
      * Exception wrapper used to retrieve the JsonTarget instance from a Failure instance.
      * @param target   JsonTarget
      * @param message  exception message
      * @param cause    Original Exception
      */
    case class JsonTargetException(
        target: JsonTarget,
        message: String = "",
        cause: Throwable = None.orNull
    ) extends Exception(message, cause) { }

}
