package org.wikimedia.analytics.refinery.job.refine

import org.apache.hadoop.fs.Path
import org.joda.time.format.DateTimeFormatter
import com.github.nscala_time.time.Imports._
import scala.util.matching.Regex
import org.apache.spark.sql.SparkSession
import scala.collection.immutable.ListMap
import org.wikimedia.analytics.refinery.core.{LogHelper, ReflectUtils, Utilities}
import org.wikimedia.analytics.refinery.core.config.{ConfigHelper, ConfigHelperMacros, ConfigHelperException}

/**
  * Use RefineTarget.find to find all Refine targets for an input (camus job) in the last N hours.
  * Then filter for any for which the _REFINED_FAILED flag exists.
  */
object RefineFailuresChecker extends LogHelper with ConfigHelper {

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
        input_path_datetime_format: DateTimeFormatter   = DateTimeFormat.forPattern("'hourly'/yyyy/MM/dd/HH")
    )

    object Config {
        // This is just used to ease generating help message with default values.
        // Required configs are set to dummy values.
        val default = Config("", "", Seq(""), "", "")

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
            "input_path_regex" ->
                s"""This should match the input partition directory hierarchy starting from the
                   |dataset base path, and should capture the table name and the partition values.
                   |Along with input-capture, this allows arbitrary extraction of table names and and
                   |partitions from the input path.  You are required to capture at least "table"
                   |using this regex.  The default will match an hourly bucketed Camus import hierarchy,
                   |using the topic name as the table name.""",
            "input_path_regex_capture_groups" ->
                s"""This should be a comma separated list of named capture groups
                   |corresponding to the groups captured byt input-regex.  These need to be
                   |provided in the order that the groups are captured.  This ordering will
                   |also be used for partitioning.""",
            "output_path" ->
                """Base path of output data and of external Hive tables.  Completed refine targets
                  |are expected to be found here in subdirectories based on extracted table names.""",
            "database" ->
                s"Hive database name in which to manage refined Hive tables.",
            "since" ->
                s"""Look for refine targets since this date time.  This may either be given as an integer
                   |number of hours ago, or an ISO-8601 formatted date time.  Default: ${default.since}""",
            "until" ->
                s"""Look for refine targets until this date time.  This may either be given as an integer
                   |number of hours ago, or an ISO-8601 formatted date time.  Default: ${default.until}""",
            "input_path_datetime_format" ->
                s"""This DateTimeFormat will be used to generate all possible partitions since
                   |the given lookback-hours in each dataset directory.  This format will be used
                   |to format a DateTime to input directory partition paths.  The finest granularity
                   |supported is hourly.  Every hour in the past lookback-hours will be generated,
                   |but if you specify a less granular format (e.g. daily, like "daily"/yyyy/MM/dd),
                   |the code will reduce the generated partition search for that day to 1, instead of 24.
                   |The default is suitable for generating partitions in an hourly bucketed Camus
                   |import hierarchy.  Default: ${default.input_path_datetime_format}"""
        )

        val usage: String =
            """
            |Input Datasets -> List of targets with refine failed flag.
            |
            |Given an input base path, this will look for Refine targets that have
            |failed to refine during the time window requested.
            |
            |Example:
            |  spark-submit --class org.wikimedia.analytics.refinery.job.refine.RefineFailuresChecker refinery-job.jar \
            |  # read configs out of this file
            |   --config_file                 /etc/refinery/refine_eventlogging_eventbus.properties \
            |   # Override and/or set other configs on the CLI
            |   --input_path                  /wmf/data/raw/event \
            |   --output_path                 /user/otto/external/eventbus5' \
            |   --database                    event \
            |   --since                       24 \
            |   --input_regex                 '.*(eqiad|codfw)_(.+)/hourly/(\d+)/(\d+)/(\d+)/(\d+)'
            |"""
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

        val baseInputPath = new Path(config.input_path)
        val baseTableLocationPath = new Path(config.output_path)
        val inputPathRegex = new Regex(config.input_path_regex, config.input_path_regex_capture_groups: _*)
        val inputPathDateTimeFormatter =  config.input_path_datetime_format
        val sinceDateTime = config.since
        val untilDateTime = config.until
        val databaseName = config.database
        val targets = RefineTarget.find(
          spark,
          baseInputPath,
          baseTableLocationPath,
          databaseName,
          inputPathDateTimeFormatter,
          inputPathRegex,
          sinceDateTime,
          untilDateTime
        )
        val failedTargets = targets.filter(_.failureFlagExists)
        if (failedTargets.size > 0) {
            println("The following targets have the _REFINED_FAILED flag set:")
            failedTargets.foreach(println)
        } else {
            println("No target has the _REFINED_FAILED flag set for the selected time window.")
        }

        // Exit with proper exit val if not running in YARN.
        if (spark.conf.get("spark.master") != "yarn") {
            if (failedTargets.size == 0) sys.exit(0) else sys.exit(1)
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
}