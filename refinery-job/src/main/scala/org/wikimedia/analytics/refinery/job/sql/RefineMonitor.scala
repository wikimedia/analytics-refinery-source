package org.wikimedia.analytics.refinery.job.sql

import com.github.nscala_time.time.Imports.{DateTime, DateTimeFormat, _}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.format.DateTimeFormatter
import org.wikimedia.analytics.refinery.core.{LogHelper, Utilities}
import scopt.OptionParser

import scala.util.matching.Regex

object RefineMonitor extends LogHelper {
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
               |Find missing refine targets and print or email a report.
               |
               |This job should be used with the same arguments as Refine in order to print out
               |a status report about incomplete Refine jobs.  Likely you'll want to provide
               |a delayed time range, to give any regularly scheduled Refine jobs time to
               |finish.
               |
               |Example:
               |  spark-submit --class org.wikimedia.analytics.refinery.job.refine.RefineMonitor refinery-job.jar \
               |   --input-base-path     /wmf/data/raw/event \
               |   --output-base-path    /user/otto/external/eventbus5' \
               |   --database            event \
               |   # Look for missing refine targets in the last 24 hours up until the last 4 hours.
               |   --since               24 \
               |   --until               4 \
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
              |Each of these subdirectories will be searched for refine target
              |partitions.""".stripMargin.replace("\n", "\n\t") + "\n"

        opt[String]('o', "output-base-path") optional() valueName "<path>" action { (x, p) =>
            p.copy(outputBasePath = if (x.endsWith("/")) x.dropRight(1) else x)
        } text
            """Base path of output data and of external Hive tables.  Completed refine targets
              |are expected to be found here in subdirectories based on extracted table names."""
                .stripMargin.replace("\n", "\n\t") + "\n"

        opt[String]('d', "database") optional() valueName "<database>" action { (x, p) =>
            p.copy(databaseName = if (x.endsWith("/")) x.dropRight(1) else x)
        } text "Hive database name in which to manage refined Hive tables.\n"

        opt[DateTime]('s', "since") optional() valueName "<since-date-time>" action { (x, p) =>
            p.copy(sinceDateTime = x)
        } text
            """Look for refine targets since this date time.  This may either be given as an integer
              |number of hours ago, or an ISO-8601 formatted date time.  Default: 192 hours ago."""
                .stripMargin.replace("\n", "\n\t") + "\n"

        opt[DateTime]('u', "until") optional() valueName "<until-date-time>" action { (x, p) =>
            p.copy(untilDateTime = x)
        } text
            """Look for refine targets until this date time.  This may either be given as an integer
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
        } text "Whitelist regex of table names to look for.\n".stripMargin

        opt[Option[Regex]]('b', "table-blacklist") optional() valueName "<regex>" action { (x, p) =>
            p.copy(tableBlacklistRegex = x)
        } text "Blacklist regex of table names to skip.\n".stripMargin

        opt[Unit]('E', "send-email-report") optional() action { (_, p) =>
            p.copy(shouldEmailReport = true)
        } text
            "Set this flag if you want an email report of any missing refine targets."

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
        shouldEmailReport: Boolean                 = false,
        smtpURI: String                            = "mx1001.wikimedia.org:25",
        fromEmail: String                          = s"refine@${java.net.InetAddress.getLocalHost.getCanonicalHostName}",
        toEmails: Seq[String]                      = Seq("analytics-alerts@wikimedia.org")
    ): Boolean = {
        val fs = FileSystem.get(new Configuration())

        // Ensure that inputPathPatternCaptureGroups contains "table", as this is needed
        // to determine the Hive table name we will refine into.
        if (!inputPathPatternCaptureGroups.contains("table")) {
            throw new RuntimeException(
                s"Invalid <input-capture> $inputPathPatternCaptureGroups. " +
                    s"Must at least contain 'table' as a named capture group."
            )
        }

        // Combine the inputPathPattern with the capture groups to build a regex that
        // will use aliases for the named groups.  This will be used to extract
        // table and partitions out of the inputPath.
        val inputPathRegex = new Regex(
            inputPathPattern,
            inputPathPatternCaptureGroups: _*
        )

        log.info(
            s"Looking for targets in $inputBasePath between " +
                s"$sinceDateTime and $untilDateTime"
        )

        // Need RefineTargets for every existent input partition since pastCutoffDateTime
        val targets = RefineTarget.find(
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
            .filter(_.shouldRefine(tableWhitelistRegex, tableBlacklistRegex))

        if (targets.isEmpty) {
            log.info(
                s"No dataset targets in $inputBasePath between " +
                    s"$sinceDateTime and $untilDateTime need " +
                    s"refinement to $outputBasePath"
            )
        }

        else {
            val report = s"The following dataset targets in $inputBasePath between " +
                s"$sinceDateTime and $untilDateTime have not yet " +
                s"been refined to $outputBasePath:\n\n\t" +
                targets.map(_.partition).mkString("\n\t")
            log.warn(report)

            if (shouldEmailReport) {
                val smtpHost = smtpURI.split(":")(0)
                val smtpPort = smtpURI.split(":")(1)

                log.info(s"Sending failure email report to ${toEmails.mkString(",")}")
                Utilities.sendEmail(
                    smtpHost,
                    smtpPort,
                    fromEmail,
                    toEmails.toArray,
                    s"Refined datasets missing for $inputBasePath to $outputBasePath",
                    report
                )
            }
        }

        targets.isEmpty
    }
}