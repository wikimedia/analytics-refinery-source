package org.wikimedia.analytics.refinery.job.refine

import com.github.nscala_time.time.Imports._
import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.core.LogHelper
import org.wikimedia.analytics.refinery.core.config._
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._
import scala.util.matching.Regex
import scala.collection.immutable.ListMap

object EventLoggingSanitization extends LogHelper with ConfigHelper
{
    // Config class for CLI argument parser using scopt.
    case class Config(
        whitelist_path : String = "/wmf/refinery/current/static_data/eventlogging/whitelist.yaml",
        salts_path     : Option[String] = None
    )

    object Config {
        // This is just used to ease generating help message with default values.
        // Required configs are set to dummy values.
        val default = Config()

        val propertiesDoc: ListMap[String, String] = {
            val doc = ListMap(
                "whitelist_path" ->
                    s"Path to EventLogging's whitelist file. Default: ${default.whitelist_path}",
                "salts_path" ->
                    s"""Read the cryptographic salts for hashing of fields from this path.
                       |Default: ${default.salts_path}"""
            )
            // We reuse Refine.Config and Refine.Config help documentation, but since
            // This job will always override the following properties, remove them
            // from this job's help message.
            val refineDoc = Refine.Config.propertiesDoc -- Set(
                "input_path_regex",
                "input_path_regex_capture_groups",
                "input_path_datetime_format"
            )

            // Combine our property doc with Refine.Config's property doc
            doc ++ refineDoc
        }

        val usage: String =
            """|Sanitize EventLogging Hive tables using a whitelist.
               |
               |Given an input base path for the data and one for the whitelist, this job
               |will search all subdirectories for input partitions to sanitize. It will
               |interpret the whitelist and apply it to keep only the tables and fields
               |mentioned in it.
               |
               |Example:
               |  spark-submit --class org.wikimedia.analytics.refinery.job.EventLoggingSanitization refinery-job.jar \
               |   # read configs out of this file
               |   --config_file                 /etc/refinery/refine/eventlogging_sanitization.properties \
               |   # Override and/or set other configs on the CLI
               |   --whitelist_path              /wmf/path/to/whitelist \
               |   --input_path                  /wmf/data/event \
               |   --output_path                 /user/mforns/sanitized' \
               |   --database                    mforns \
               |   --since                       24 \
               |"""
    }

    // These parameters will always be the same for EventLogging data sets.
    // The user does not need to specify them, but we have to pass them forward to Refine.
    val InputPathRegex = ".*/([^/]+)/year=(\\d{4})/month=(\\d{1,2})/day=(\\d{1,2})/hour=(\\d{1,2})"
    val InputPathRegexCaptureGroups = Seq("table", "year", "month", "day", "hour")


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

        // Load EventLoggingSanitization specific configs
        val config = loadConfig(args)
        // Load Refine job specific configs
        // The following 2 extra args need to be passed at this point
        val refineArgs = args ++ Array(
            "--input_path_regex", InputPathRegex,
            "--input_path_regex_capture_groups", InputPathRegexCaptureGroups.mkString(",")
        )
        val refineConfig = Refine.loadConfig(refineArgs)

        val allSucceeded = apply(spark)(
            config.whitelist_path,
            config.salts_path,
            refineConfig
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

    /**
      * Apply sanitization to EventLogging analytics tables in Hive with the specified params.
      *
      * @param refineConfig Refine.Config
      * @param spark Spark session
      * @return true if the sanitization succeeded, false otherwise.
      */
    def apply(spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate())(
        whitelist_path: String,
        salts_path    : Option[String],
        refineConfig  : Refine.Config
    ): Boolean = {
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

        // Read whitelist from yaml file.
        val whitelistStream = fs.open(new Path(whitelist_path))
        val javaObject = new Yaml().load(whitelistStream)
        val whitelist = validateWhitelist(javaObject)

        // Read hashing salts if provided.
        val hashingSalts = loadHashingSalts(fs, salts_path)

        // Get a Regex with all tables that are whitelisted.
        // This prevents Refine to collect RefineTargets for those tables
        // and to create a tree of directories just to store success files.
        val tableWhitelistRegex = Some(refineConfig.table_whitelist_regex.getOrElse(
            new Regex("^(" + whitelist.keys.mkString("|") + ")$")
        ))

        // Get WhitelistSanitization transform function.
        val sanitizationTransformFunction = WhitelistSanitization(
            whitelist,
            hashingSalts
        )

        // Use Refine with the sanitization transform function to sanitize EventLogging data.
        Refine(
            spark,
            // Force some Eventlogging analytics dataset configs, just in case someone tries to
            // set them to something that won't work for EventLogging Refine.
            refineConfig.copy(
                input_path_datetime_format = DateTimeFormat.forPattern("'year='yyyy/'month='M/'day='d/'hour='H"),
                transform_functions = refineConfig.transform_functions :+ sanitizationTransformFunction.asInstanceOf[Refine.TransformFunction],
                table_whitelist_regex = tableWhitelistRegex
            )
        )
    }


  /**
    * Tries to cast a java object into a [[WhitelistSanitization.Whitelist]],
    * and enforce no "keepall" tag is used.
    *
    * Throws a ClassCastException in case of casting failure,
    * or an IllegalArgumentException in case of keepall tag used
    *
    * @param javaObject  The unchecked whitelist structure
    * @return a Map[String, Any] having no keepall tag.
    */
  def validateWhitelist(javaObject: Object): WhitelistSanitization.Whitelist = {
        // Transform to java map.
        val javaMap = try {
            javaObject.asInstanceOf[java.util.Map[String, Any]]
        } catch {
            case e: ClassCastException => throw new ClassCastException(
                "Whitelist object can not be cast to Map[String, Any]."
            )
        }
        // Apply recursively.
        javaMap.asScala.toMap.map { case (key, value) =>
            value match {
                case "keepall" => throw new IllegalArgumentException(
                    "Keyword 'keepall' is not permitted in EventLogging whitelist."
                )
                case tag: String => key -> tag
                case nested: Object => key -> validateWhitelist(nested)
            }
        }
    }

    /**
      * Loads the salts stored in a salts directory in HDFS
      * and returns them in the format expected by WhiteListSanitization.
      * A Seq of tuples of the form: (<startDateTime>, <endDateTime>, <saltString>)
      */
    def loadHashingSalts(
        fs: FileSystem,
        saltsPath: Option[String]
    ): Seq[(DateTime, DateTime, String)] = {
        if (saltsPath.isDefined) {
            val dateTimeFormatter = DateTimeFormat.forPattern("yyyyMMddHH")
            val status = fs.listStatus(new Path(saltsPath.get))
            status.map((s) => {
                val fileNameParts = s.getPath.getName.split("_")
                val startDateTime = DateTime.parse(fileNameParts(0), dateTimeFormatter)
                // If file name does not have second component,
                // means the salt does not expire.
                val endDateTime = if (fileNameParts(1) != "") {
                    DateTime.parse(fileNameParts(1), dateTimeFormatter)
                } else new DateTime(3000, 1, 1, 0, 0) // Never expires.
                val saltStream = fs.open(s.getPath)
                val saltReader = new BufferedReader(new InputStreamReader(saltStream))
                val salt = saltReader.lines.toArray.mkString
                (startDateTime, endDateTime, salt)
            })
        } else Seq.empty
    }
}
