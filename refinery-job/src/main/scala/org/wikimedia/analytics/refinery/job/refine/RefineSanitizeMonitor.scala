package org.wikimedia.analytics.refinery.job.refine

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.tools.config._
import org.wikimedia.analytics.refinery.tools.LogHelper

/**
  * A RefineMonitor job that uses the RefineSanitize allowlist_path property
  * to calculate the default table_whitelist_regex to use when looking for
  * RefineTargets that need monitored.  This is just a simple wrapper around
  * RefineMonitor.
  */
object RefineSanitizeMonitor extends LogHelper {
    val usage = """
        |Find missing RefineSanitize targets and print or email a report.
        |
        |This job should be used with the same arguments as RefineSanitize
        |in order to print out a status report about incomplete sanitization jobs.
        |Likely you'll want to provide a delayed time range, to give any regularly
        |scheduled sanitization jobs time to finish. The following example reads
        |initial configs out of a refine_sanitize.properties and then overrides
        |individual config settings via CLI opts.
        |
        |Example:
        |  spark-submit --class org.wikimedia.analytics.refinery.job.refine.RefineSanitizeMonitor refinery-job.jar \
        |   --config_file         /etc/refinery/refine_sanitize.properties \
        |   --output_path         /user/mforns/event_sanitized \
        |   --output_database     event \
        |   --since               28 \
        |   --until               4 \
        |   --allowlist_path      /wmf/datapath/to/allowlist
        |
        |Note: the example only uses spark-submit to ease classpath discovery;
        |RefineMonitor (RefineSanitizeMonitor) does not use Spark itself.
        |"""

    def main(args: Array[String]): Unit = {
        if (args.contains("--help")) {
            println(RefineSanitize.help(usage, RefineSanitize.Config.propertiesDoc))
            sys.exit(0)
        }

        val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
        // if not running in yarn, make spark log level quieter.
        if (spark.conf.get("spark.master") != "yarn") {
            spark.sparkContext.setLogLevel("WARN")
        }

        // Load RefineSanitize specific configs.
        val sanitizeConfig = RefineSanitize.Config(args)
        // Load Refine specific configs.
        val refineConfig = Refine.Config(args)

        val allSucceeded = apply(spark)(sanitizeConfig, refineConfig)

        // Exit with proper exit val if not running in YARN.
        if (spark.conf.get("spark.master") != "yarn") {
            sys.exit(if (allSucceeded) 0 else 1)
        }
    }

    def apply(spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate())(
        sanitizeConfig: RefineSanitize.Config,
        refineConfig  : Refine.Config
    ): Boolean = {
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

        val allowlist = SanitizeTransformation.loadAllowlist(fs)(
            sanitizeConfig.allowlist_path, sanitizeConfig.keep_all_enabled
        )

        // If --table_include_regex is not explicitly configured,
        // use the value of all table keys in the allowlist to build table_include_regex.
        // This prevents Refine to load RefineTargets for unwanted tables.
        val tableIncludeRegex = RefineSanitize.getTableIncludeList(
            refineConfig.table_include_regex,
            allowlist
        )

        // Call RefineMonitor with the default table_include_regex calculated from allowlist.
        RefineMonitor(spark)(
            refineConfig.copy(table_include_regex=tableIncludeRegex)
        )
    }
}
