package org.wikimedia.analytics.refinery.job.refine

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.core.config._
import org.wikimedia.analytics.refinery.core.LogHelper
import scala.collection.immutable.ListMap
import scala.util.matching.Regex

/**
  * A RefineMonitor job that uses the RefineSanitize allowlist_path property
  * to calculate the default table_whitelist_regex to use when looking for
  * RefineTargets that need monitored.  This is just a simple wrapper around
  * RefineMonitor.
  */
object RefineSanitizeMonitor extends LogHelper with ConfigHelper {

    // Specific parameters for RefineSanitizeMonitor.
    // Other parameters are reused from RefineMonitor.Config.
    case class Config(
        allowlist_path   : String
    )

    object Config {
        val propertiesDoc: ListMap[String, String] = RefineMonitor.Config.propertiesDoc ++ ListMap(
            "allowlist_path" ->  "Path to the sanitization allowlist file."
        )

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
            |   --output_base_path    /user/mforns/event_sanitized \
            |   --database            event \
            |   --since               28 \
            |   --until               4 \
            |
            |Note: the example only uses spark-submit to ease classpath discovery;
            |RefineMonitor (RefineSanitizeMonitor) does not use Spark itself.
            |"""
    }

    def main(args: Array[String]): Unit = {
        if (args.contains("--help")) {
            println(help(Config.usage, Config.propertiesDoc))
            sys.exit(0)
        }

        val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
        // if not running in yarn, make spark log level quieter.
        if (spark.conf.get("spark.master") != "yarn") {
            spark.sparkContext.setLogLevel("WARN")
        }

        // Load RefineSanitizeMonitor specific configs.
        val config = loadConfig(args)
        // Load RefineMonitor specific configs.
        val refineMonitorConfig = RefineMonitor.loadConfig(args)

        val allSucceeded = apply(spark)(
            config.allowlist_path,
            refineMonitorConfig
        )

        // Exit with proper exit val if not running in YARN.
        if (spark.conf.get("spark.master") != "yarn") {
            sys.exit(if (allSucceeded) 0 else 1)
        }
    }

    def loadConfig(args: Array[String]): Config = {
        val config = try {
            configureArgs[Config] (args)
        } catch {
            case e: ConfigHelperException =>
                log.fatal (e.getMessage + ". Aborting.")
                sys.exit(1)
        }
        log.info("Loaded configuration:\n" + prettyPrint(config))
        config
    }

    def apply(spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate())(
        allowlist_path: String,
        refineMonitorConfig  : RefineMonitor.Config
    ): Boolean = {
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

        val allowlist = SanitizeTransformation.loadAllowlist(fs)(allowlist_path)

        // If --table_whitelist_regex is not explicitly configured,
        // use the value of all table keys in the allowlist to build table_whitelist_regex.
        // This prevents Refine to load RefineTargets for unwanted tables.
        val tableWhitelistRegex = Some(refineMonitorConfig.table_whitelist_regex.getOrElse(
            new Regex("^(" + allowlist.keys.mkString("|") + ")$")
        ))

        // Call RefineMonitor with the default table_whitelist_regex calculated from allowlist.
        RefineMonitor.apply(
            spark,
            refineMonitorConfig.copy(table_whitelist_regex = tableWhitelistRegex)
        )
    }
}
