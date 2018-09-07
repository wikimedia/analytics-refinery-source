package org.wikimedia.analytics.refinery.job.refine

import com.github.nscala_time.time.Imports.{DateTime, DateTimeFormat, _}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.format.DateTimeFormatter
import org.wikimedia.analytics.refinery.core.config._
import org.wikimedia.analytics.refinery.core.{LogHelper, Utilities}

import scala.util.matching.Regex

object RefineMonitor extends LogHelper with ConfigHelper {
    /**
      * Config class for use config files and args.
      */
    case class Config(
        input_path: String,
        output_path: String,
        database: String                                = "default",
        since: DateTime                                 = DateTime.now - 192.hours, // 8 days ago
        until: DateTime                                 = DateTime.now,
        input_path_regex: String                        = ".*/(.+)/hourly/(\\d{4})/(\\d{2})/(\\d{2})/(\\d{2}).*",
        input_path_regex_capture_groups: Seq[String]    = Seq("table", "year", "month", "day", "hour"),
        input_path_datetime_format: DateTimeFormatter   = DateTimeFormat.forPattern("'hourly'/yyyy/MM/dd/HH"),
        table_whitelist_regex: Option[Regex]            = None,
        table_blacklist_regex: Option[Regex]            = None,
        should_email_report: Boolean                    = false,
        smtp_uri: String                                = "mx1001.wikimedia.org:25",
        from_email: String                              = s"refine@${java.net.InetAddress.getLocalHost.getCanonicalHostName}",
        to_emails: Seq[String]                          = Seq("analytics-alerts@wikimedia.org")
    )

    object Config {
        // This is just used to ease generating help message with default values.
        // Required configs are set to dummy values.
        private final val default = Config("", "")

        final val paramsHelp: Map[String, String] = Map(
            "config_file <file1.properties,files2.properties>" ->
                """Comma separated list of paths to properties files.  These files parsed for
                  |for matching config parameters as defined here.""",
            "input_path <input-path>" ->
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
                s"""input-regex should match the input partition directory hierarchy starting from the
                  |dataset base path, and should capture the table name and the partition values.
                  |Along with input-capture, this allows arbitrary extraction of table names and and
                  |partitions from the input path.  You are required to capture at least "table"
                  |using this regex.  The default will match an hourly bucketed Camus import hierarchy,
                  |using the topic name as the table name.  Default: ${default.input_path_regex}""",
            "input_path_regex_capture_groups" ->
                s"""input-capture should be a comma separated list of named capture groups
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
            "should_email_report" ->
                s"Set this flag if you want an email report of any missing refine targets.  Default: ${default.should_email_report}",
            "smtp_uri" ->
                s"SMTP server host:port. Default: mx1001.wikimedia.org.  Default: ${default.smtp_uri}",
            "from_email" ->
                s"Email report from sender email address.  Default: ${default.from_email}",
            "to_emails" ->
                s"Email report recipient email addresses (comma separated):  Default: ${default.to_emails.mkString(",")}."
        ).mapValues(_.stripMargin.replace("\n", "\n    ") + "\n")

        final val help: String = {
            val usage = """
                |Find missing refine targets and print or email a report.
                |
                |This job should be used with the same arguments as Refine in order to print out
                |a status report about incomplete Refine jobs.  Likely you'll want to provide
                |a delayed time range, to give any regularly scheduled Refine jobs time to
                |finish.  The following example reads initial configs out of
                |refine_eventlogging_eventbus.properties and then overrides individual config settings
                |via CLI opts.
                |
                |Example:
                |  spark-submit --class org.wikimedia.analytics.refinery.job.refine.RefineMonitor refinery-job.jar \
                |  # read configs out of this file
                |   --config_file          /etc/refinery/refine_eventlogging_eventbus.properties \
                |   # Override and/or set other configs on the CLI
                |   --output_base_path     /user/otto/external/eventbus5' \
                |   --database            event \
                |   # Look for missing refine targets in the last 24 hours up until the last 4 hours.
                |   --since               24 \
                |   --until               4 \
                |   --table_blacklist     '.*page_properties_change.*'
                |
                |Note: the example only uses spark-submit to ease classpath discovery;
                |RefineMonitor does not use Spark itself.
                |
                |Properties:
                |
                |""".stripMargin

            usage + paramsHelp.map(t => s"  ${t._1}\n    ${t._2}").mkString("\n")
        }
    }


    def main(args: Array[String]): Unit = {
        if (args.contains("--help")) {
            println(Config.help)
            sys.exit(0)
        }

        val config = try {
            configureArgs[Config](args)
        } catch {
            case e: ConfigHelperException =>
                log.fatal(e.getMessage + ". Aborting.")
                sys.exit(1)
        }

        // Exit non-zero if if any refinements failed.
        if ((apply _).tupled(Config.unapply(config).get))
            sys.exit(0)
        else
            sys.exit(1)
    }

    def apply(
        input_path: String,
        output_path: String,
        database: String                                = "default",
        since: DateTime                                 = DateTime.now - 192.hours, // 8 days ago
        until: DateTime                                 = DateTime.now,
        input_path_regex: String                        = ".*/(.+)/hourly/(\\d{4})/(\\d{2})/(\\d{2})/(\\d{2}).*",
        input_path_regex_capture_groups: Seq[String]    = Seq("table", "year", "month", "day", "hour"),
        input_path_datetime_format: DateTimeFormatter   = DateTimeFormat.forPattern("'hourly'/yyyy/MM/dd/HH"),
        table_whitelist_regex: Option[Regex]            = None,
        table_blacklist_regex: Option[Regex]            = None,
        should_email_report: Boolean                    = false,
        smtp_uri: String                                = "mx1001.wikimedia.org:25",
        from_email: String                              = s"refine@${java.net.InetAddress.getLocalHost.getCanonicalHostName}",
        to_emails: Seq[String]                          = Seq("analytics-alerts@wikimedia.org")
    ): Boolean = {
        val fs = FileSystem.get(new Configuration())

        // Ensure that inputPathPatternCaptureGroups contains "table", as this is needed
        // to determine the Hive table name we will refine into.
        if (!input_path_regex_capture_groups.contains("table")) {
            throw new RuntimeException(
                s"Invalid <input-capture> $input_path_regex_capture_groups. " +
                    s"Must at least contain 'table' as a named capture group."
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
            s"Looking for targets in $input_path between " +
                s"$since and $until"
        )

        // Need RefineTargets for every existent input partition since pastCutoffDateTime
        val targets = RefineTarget.find(
            fs,
            new Path(input_path),
            new Path(output_path),
            database,
            input_path_datetime_format,
            inputPathRegex,
            since,
            until
        )
            // Filter for tables in whitelist, filter out tables in blacklist,
            // and filter the remaining for targets that need refinement.
            .filter(_.shouldRefine(table_whitelist_regex, table_blacklist_regex))

        if (targets.isEmpty) {
            log.info(
                s"No dataset targets in $input_path between " +
                    s"$since and $until need " +
                    s"refinement to $output_path"
            )
        }

        else {
            val report = s"The following dataset targets in $input_path between " +
                s"$since and $until have not yet " +
                s"been refined to $output_path:\n\n\t" +
                targets.map(_.partition).mkString("\n\t")
            log.warn(report)

            if (should_email_report) {
                val smtpHost = smtp_uri.split(":")(0)
                val smtpPort = smtp_uri.split(":")(1)

                log.info(s"Sending failure email report to ${to_emails.mkString(",")}")
                Utilities.sendEmail(
                    smtpHost,
                    smtpPort,
                    from_email,
                    to_emails.toArray,
                    s"Refined datasets missing for $input_path to $output_path",
                    report
                )
            }
        }

        targets.isEmpty
    }
}