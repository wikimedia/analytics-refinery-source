package org.wikimedia.analytics.refinery.job.refine

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.core.config._
import org.wikimedia.analytics.refinery.core.LogHelper
import org.yaml.snakeyaml.Yaml
import scala.collection.immutable.ListMap
import scala.collection.JavaConverters._
import scala.util.matching.Regex

object EventLoggingSanitizationMonitor extends LogHelper with ConfigHelper {

    // Specific parameters for EventLoggingSanitizationMonitor.
    // Other parameters are reused from RefineMonitor.Config.
    case class Config(
        whitelist_path: String = "/wmf/refinery/current/static_data/eventlogging/whitelist.yaml"
    )

    object Config {
        val defaults = Config()

        val propertiesDoc: ListMap[String, String] = {
            val doc = ListMap(
                "whitelist_path" ->
                    s"Path to EventLogging's whitelist file. Default: ${defaults.whitelist_path}"
            )

            // We reuse Refine.Config and Refine.Config help documentation, but since
            // This job will always override the following properties, remove them
            // from this job's help message.
            val refineDoc = RefineMonitor.Config.propertiesDoc -- Set(
                "input_path_regex",
                "input_path_regex_capture_groups",
                "input_path_datetime_format",
                "table_whitelist_regex"
            )

            // Combine our property doc with Refine.Config's property doc
            doc ++ refineDoc
        }

        val usage = """
            |Find missing EventLoggingSanitization targets and print or email a report.
            |
            |This job should be used with the same arguments as EventLoggingSanitization
            |in order to print out a status report about incomplete sanitization jobs.
            |Likely you'll want to provide a delayed time range, to give any regularly
            |scheduled sanitization jobs time to finish. The following example reads
            |initial configs out of sanitize_eventlogging.properties and then overrides
            |individual config settings via CLI opts.
            |
            |Example:
            |  spark-submit --class org.wikimedia.analytics.refinery.job.refine.EventLoggingSanitizationMonitor refinery-job.jar \
            |   --config_file         /etc/refinery/sanitize_eventlogging.properties \
            |   --output_base_path    /user/mforns/event_sanitized \
            |   --database            event \
            |   --since               28 \
            |   --until               4 \
            |
            |Note: the example only uses spark-submit to ease classpath discovery;
            |RefineMonitor (EventLoggingSanitizationMonitor) does not use Spark itself.
            |"""
    }

    // These parameters will always be the same for EventLogging data sets.
    // The user does not need to specify them, but we have to pass them forward to RefineMonitor.
    val InputPathRegex = ".*/([^/]+)/year=(\\d{4})/month=(\\d{1,2})/day=(\\d{1,2})/hour=(\\d{1,2})"
    val InputPathRegexCaptureGroups = "table,year,month,day,hour"
    val InputPathDatetimeFormat = "'year='yyyy/'month='M/'day='d/'hour='H"

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

        // Load EventLoggingSanitizationMonitor specific configs.
        val config = loadConfig(args)
        // Load RefineMonitor specific configs.
        // The following extra args need to be passed at this point.
        val refineMonitorArgs = args ++ Array(
            "--input_path_regex", InputPathRegex,
            "--input_path_regex_capture_groups", InputPathRegexCaptureGroups,
            "--input_path_datetime_format", InputPathDatetimeFormat
        )
        val refineMonitorConfig = RefineMonitor.loadConfig(refineMonitorArgs)

        val allSucceeded = apply(spark)(
            config.whitelist_path,
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
        whitelist_path: String,
        refineMonitorConfig  : RefineMonitor.Config
    ): Boolean = {
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

        // Read whitelist from yaml file.
        val whitelistStream = fs.open(new Path(whitelist_path))
        val javaObject = new Yaml().load[Object](whitelistStream)
        val whitelist = EventLoggingSanitization.validateWhitelist(javaObject)

        // Get a Regex with all tables that are whitelisted.
        // This prevents RefineMonitor to collect RefineTargets
        // for tables that were not copied over the sanitized database.
        val tableWhitelistRegex = Some(refineMonitorConfig.table_whitelist_regex.getOrElse(
            new Regex("^(" + whitelist.keys.mkString("|") + ")$")
        ))

        // Call RefineMonitor with the updated config case class.
        val updatedConfig = refineMonitorConfig.copy(
            table_whitelist_regex = tableWhitelistRegex
        )
        (RefineMonitor.apply(spark) _).tupled(RefineMonitor.Config.unapply(updatedConfig).get)
    }
}
