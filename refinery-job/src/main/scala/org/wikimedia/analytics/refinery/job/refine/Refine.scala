package org.wikimedia.analytics.refinery.job.refine

import scopt.OptionParser

import scala.util.{Success, Try}
import scala.util.matching.Regex
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import org.joda.time.format.DateTimeFormatter
import com.github.nscala_time.time.Imports._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame
import org.wikimedia.analytics.refinery.core.{HivePartition, ReflectUtils, Utilities}


// TODO: support append vs overwrite?
// TODO: Hive Table Locking?


/**
  * Looks for hourly input partition directories with data that need refinement,
  * and refines them into Hive Parquet tables using DataFrameToHive.
  */
object Refine extends LogHelper {
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
        transformFunctions: Seq[String]            = Seq(),
        shouldIgnoreFailureFlag: Boolean           = false,
        parallelism: Option[Int]                   = None,
        compressionCodec: String                   = "snappy",
        limit: Option[Int]                         = None,
        dryRun: Boolean                            = false,
        shouldEmailReport: Boolean                 = false,
        smtpURI: String                            = "mx1001.wikimedia.org:25",
        fromEmail: String                          = s"refine@${java.net.InetAddress.getLocalHost.getCanonicalHostName}",
        toEmails: Seq[String]                      = Seq("analytics-alerts@wikimedia.org")
    )

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
        "spark-submit --class org.wikimedia.analytics.refinery.job.refine.Refine refinery-job.jar"
    ) {
        head("""
              |Input Datasets -> Partitioned Hive Parquet tables.
              |
              |Given an input base path, this will search all subdirectories for input
              |partitions to convert to Parquet backed Hive tables.  This was originally
              |written to work with JSON data imported via Camus into hourly buckets, but
              |should be configurable to work with any regular directory hierarchy.
              |
              |Example:
              |  spark-submit --class org.wikimedia.analytics.refinery.job.refine.Refine refinery-job.jar \
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
            """Path to input datasets.  This directory is expected to contain
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

        opt[String]('l', "transform-functions") optional() valueName "<fn>" action { (x, p) =>
            p.copy(transformFunctions = x.split(",").toSeq)
        } text
            """Comma separated list of fully qualified module.ObjectNames.  The objects'
               apply methods should take a DataFrame and a HivePartition and return
               a DataFrame.  These functions can be used to map from the input DataFrames
               to a new ones, applying various transformations along the way.
            """.stripMargin.replace("\n", "\n\t") + "\n"

        opt[Unit]('I', "ignore-failure-flag") optional() action { (_, p) =>
            p.copy(shouldIgnoreFailureFlag = true)
        } text
            """Set this if you want all discovered partitions with _REFINE_FAILED files to be
               |(re)refined. Default: false""".stripMargin.replace("\n", "\n\t") + "\n"

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
        if ((apply _).tupled(Params.unapply(params).get))
            sys.exit(0)
        else
            sys.exit(1)
    }



    /**
      * Refine all discovered RefineTargets.
      *
      * @return true if all targets needing refinement succeeded, false otherwise.
      */
    def apply(
        inputBasePath: String,
        outputBasePath: String,
        databaseName: String                       = "default",
        sinceDateTime: DateTime                    = DateTime.now - 192.hours, // 8 days ago
        untilDateTime: DateTime                    = DateTime.now,
        inputPathPattern: String                   = ".*/(.+)/hourly/(\\d{4})/(\\d{2})/(\\d{2})/(\\d{2}).*",
        inputPathPatternCaptureGroups: Seq[String] = Seq("table", "year", "month", "day", "hour"),
        inputPathDateTimeFormat: DateTimeFormatter = DateTimeFormat.forPattern("'hourly'/yyyy/MM/dd/HH"),
        tableWhitelistRegex: Option[Regex]         = None,
        tableBlacklistRegex: Option[Regex]         = None,
        transformFunctions: Seq[String]            = Seq(),
        shouldIgnoreFailureFlag: Boolean           = false,
        parallelism: Option[Int]                   = None,
        compressionCodec: String                   = "snappy",
        limit: Option[Int]                         = None,
        dryRun: Boolean                            = false,
        shouldEmailReport: Boolean                 = false,
        smtpURI: String                            = "mx1001.wikimedia.org:25",
        fromEmail: String                          = s"refine@${java.net.InetAddress.getLocalHost.getCanonicalHostName}",
        toEmails: Seq[String]                      = Seq("analytics-alerts@wikimedia.org")
    ): Boolean = {
        // Initial setup - Spark, HiveContext, Hadoop FileSystem
        val conf = new SparkConf()
        val sc = new SparkContext(conf)

        val fs = FileSystem.get(sc.hadoopConfiguration)
        val hiveContext = new HiveContext(sc)
        hiveContext.setConf("spark.sql.parquet.compression.codec", compressionCodec)

        // Ensure that inputPathPatternCaptureGroups contains "table", as this is needed
        // to determine the Hive table name we will refine into.
        if (!inputPathPatternCaptureGroups.contains("table")) {
            throw new RuntimeException(
                s"Invalid <input-capture> $inputPathPatternCaptureGroups. " +
                 "Must at least contain 'table' as a named capture group."
            )
        }

        // Combine the inputPathPattern with the capture groups to build a regex that
        // will use aliases for the named groups.  This will be used to extract
        // table and partitions out of the inputPath.
        val inputPathRegex = new Regex(
            inputPathPattern,
            inputPathPatternCaptureGroups: _*
        )

        // If we are given any transform function names, they should be fully
        // qualified package.objectName to an object with an apply method that
        // takes a DataFrame and HiveParititon, and returns a DataFrame.
        // Map these String names to callable functions.
        val transformers = transformFunctions.map { objectName =>
            val transformMirror = ReflectUtils.getStaticMethodMirror(objectName)
            // Lookup the object's apply method as a reflect MethodMirror, and wrap
            // it in a anonymous function that has the signature expected by
            // DataFrameToHive's transformFunction parameter.
            val wrapperFn: (DataFrame, HivePartition) => DataFrame = {
                case (df, hp) => {
                    log.debug(s"Applying ${transformMirror.receiver} to $hp")
                    transformMirror(df, hp).asInstanceOf[DataFrame]
                }
            }
            wrapperFn
        }

        log.info(
            s"Looking for targets to refine in $inputBasePath between " +
            s"$sinceDateTime and $untilDateTime"
        )

        // Need RefineTargets for every existent input partition since pastCutoffDateTime
        val targetsToRefine = RefineTarget.find(
            fs,
            new Path(inputBasePath),
            new Path(outputBasePath),
            databaseName,
            inputPathDateTimeFormat,
            inputPathRegex,
            sinceDateTime,
            untilDateTime
        )
        // Filter for tables in whitelist, filter out tables in blacklist,
        // and filter the remaining for targets that need refinement.
        .filter(_.shouldRefine(
            tableWhitelistRegex, tableBlacklistRegex, shouldIgnoreFailureFlag
        ))

        // At this point, targetsToRefine will be a Seq of RefineTargets in our targeted
        // time range that need refinement, either because they haven't yet been refined,
        // or the input data has changed since the previous refinement.

        // Return now if we didn't find any targets to refine.
        if (targetsToRefine.isEmpty) {
            log.debug(s"No targets needing refinement were found in $inputBasePath")
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

        if (dryRun)
            log.warn("NOTE: --dry-run was specified, nothing will actually be refined.")

        // Loop over the inputs in parallel and refine them to
        // their Hive table partitions.  jobStatuses should be a
        // iterable of Trys as Success/Failures.
        val jobStatusesByTable = targetsByTable.map { case (table, tableTargets) => {
            // We need tableTargets to run in serial instead of parallel.  When a table does
            // not yet exist, we want the first target here to issue a CREATE, while the
            // next one to use the created table, or ALTER it if necessary.  We don't
            // want multiple CREATEs for the same table to happen in parallel.
            if (!dryRun)
                table -> refineTargets(hiveContext, tableTargets.seq, transformers)
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
        }

        // If we should send this as a failure email report
        // (and this is not a dry run), do it!
        if (hasFailures && shouldEmailReport && !dryRun) {
            val smtpHost = smtpURI.split(":")(0)
            val smtpPort = smtpURI.split(":")(1)

            log.info(s"Sending failure email report to ${toEmails.mkString(",")}")
            Utilities.sendEmail(
                smtpHost,
                smtpPort,
                fromEmail,
                toEmails.toArray,
                s"Refine failure report for $inputBasePath -> $outputBasePath",
                failureMessages
            )
        }

        // Return true if no failures, false otherwise.
        !hasFailures
    }


    /**
      * Given a Seq of RefineTargets, this runs DataFrameToHive on each one.
      *
      * @param hiveContext HiveContext
      * @param targets     Seq of RefineTargets to refine
      * @return
      */
    def refineTargets(
        hiveContext: HiveContext,
        targets: Seq[RefineTarget],
        transformFunctions: Seq[(DataFrame, HivePartition) => DataFrame]
    ): Seq[Try[RefineTarget]] = {
        targets.map(target => {
            log.info(s"Beginning refinement of $target...")

            try {
                val insertedDf = DataFrameToHive(
                    hiveContext,
                    target.inputDataFrame(hiveContext),
                    target.partition,
                    () => target.writeDoneFlag(),
                    transformFunctions
                )
                val recordCount = insertedDf.count

                log.info(
                    s"Finished refinement of dataset $target. " +
                    s"(# refined records: $recordCount)"
                )

                target.success(recordCount)
            }
            catch {
                case e: Exception => {
                    log.error(s"Failed refinement of dataset $target.", e)
                    target.writeFailureFlag()
                    target.failure(e)
                }
            }
        })
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

}
