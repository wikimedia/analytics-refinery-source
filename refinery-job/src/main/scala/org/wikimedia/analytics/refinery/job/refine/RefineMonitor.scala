package org.wikimedia.analytics.refinery.job.refine

import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.core.{LogHelper, Utilities}

object RefineMonitor extends LogHelper {
    val usage = """
        |Find missing refine targets and print or email a report.
        |
        |This job should be used with the same arguments as Refine in order to print out
        |a status report about incomplete Refine jobs.  Likely you'll want to provide
        |a delayed time range, to give any regularly scheduled Refine jobs time to
        |finish.  The following example reads initial configs out of
        |refine_eventlogging.properties and then overrides individual config settings
        |via CLI opts.
        |
        |Example:
        |  spark-submit --class org.wikimedia.analytics.refinery.job.refine.RefineMonitor refinery-job.jar \
        |  # read configs out of this file
        |   --config_file         /etc/refinery/refine_eventlogging.properties \
        |   # Override and/or set other configs on the CLI
        |   --output_path         /user/otto/external/events00' \
        |   --output_database            event \
        |   # Look for missing refine targets in the last 24 hours up until the last 4 hours.
        |   --since               24 \
        |   --until               4 \
        |   --table_exclude_regex     '.*page_properties_change.*'
        |"""

    def main(args: Array[String]): Unit = {
        if (args.contains("--help")) {
            println(Refine.help(usage, Refine.Config.propertiesDoc))
            sys.exit(0)
        }

        val spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
        // if not running in yarn, make spark log level quieter.
        if (spark.conf.get("spark.master") != "yarn") {
            spark.sparkContext.setLogLevel("WARN")
        }

        // RefineMonitor mostly uses the same parameters as Refine.
        val refineConfig = Refine.Config(args)

        // Exit non-zero if there were any targets in the time range that still need refined.
        if (apply(spark)(refineConfig)) {
            sys.exit(0)
        }
        else {
            sys.exit(1)
        }
    }

    def apply(
        spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    )(
        config: Refine.Config
    ): Boolean = {

        val targets = Refine.getRefineTargets(spark, config)
        // Filter for targets that still need, or have failed refinement.
        .filter(_.shouldRefine())

        val inputDescription = if (config.input_path.isDefined) {
            s"path ${config.input_path.get}"
        } else {
            s"database ${config.input_database}"
        }

        val outputDescription =
            s"database ${config.output_database.get} (${config.output_path.get})"

        if (targets.isEmpty) {
            log.info(
                s"No targets need refinement in $inputDescription -> $outputDescription"
            )
        }

        else {
            val doneFlag = targets.head.doneFlag
            val report = s" The following dataset targets in $inputDescription between " +
                s"${config.since} and ${config.until} need refinement into the output path ${config.output_path}\n\n" +
                s"If data is present in the output path, then either $doneFlag flag has been " +
                s"removed or it contains a timestamp older than the input data's modification time." +
                targets.map(_.partition).mkString("\n\t")
            log.warn(report)

            if (config.should_email_report) {
                val smtpHost = config.smtp_uri.split(":")(0)
                val smtpPort = config.smtp_uri.split(":")(1)

                log.info(s"Sending failure email report to ${config.to_emails.mkString(",")}")
                Utilities.sendEmail(
                    smtpHost,
                    smtpPort,
                    config.from_email,
                    config.to_emails.toArray,
                    s"Refined datasets missing for $inputDescription -> $outputDescription",
                    report
                )
            }
        }

        targets.isEmpty
    }
}
