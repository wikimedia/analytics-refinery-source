package org.wikimedia.analytics.refinery.job.sql

import java.io.{File, FileInputStream}

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.SparkSession
import org.yaml.snakeyaml.Yaml
import scopt.OptionParser

import scala.collection.JavaConverters._
import scala.util.matching.Regex


object EventLoggingSanitization {
    private val iso8601DateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss")

    // Config class for CLI argument parser using scopt.
    case class Params(
        inputBasePath: String = "/wmf/data/event",
        whitelistPath: String = "/etc/eventlogging/whitelist.yaml",
        sinceDateTime: DateTime = DateTime.now - 192.hours, // 8 days ago
        untilDateTime: DateTime = DateTime.now,
        outputBasePath: String = "/wmf/data/event",
        outputDatabase: String = "event",
        ignoreFailureFlag: Boolean = false,
        parallelism: Option[Int] = None,
        limit: Option[Int] = None,
        dryRun: Boolean = false,
        sendEmailReport: Boolean = false,
        smtpURI: String = "mx1001.wikimedia.org:25",
        fromEmail: String = s"elsanitization@${java.net.InetAddress.getLocalHost.getCanonicalHostName}",
        toEmails: Seq[String] = Seq("analytics-alerts@wikimedia.org")
    )

    // Support implicit DateTime conversion from CLI opt.
    // The opt can either be given in integer hours ago,
    // or as an ISO-8601 date time.
    implicit val scoptDateTimeRead: scopt.Read[DateTime] =
        scopt.Read.reads { s => {
            if (s.forall(Character.isDigit))
                DateTime.now - s.toInt.hours
            else
                DateTime.parse(s, iso8601DateFormatter)
        }
    }

    // Define the command line options parser.
    val argsParser = new OptionParser[Params](
        "spark-submit --class org.wikimedia.analytics.refinery.job.EventLoggingSanitization refinery-job.jar"
    ) {
        head("""
            |Sanitize EventLogging Hive tables using a whitelist.
            |
            |Given an input base path for the data and one for the whitelist, this job
            |will search all subdirectories for input partitions to sanitize. It will
            |interpret the whitelist and apply it to keep only the tables and fields
            |mentioned in it.
            |
            |Example:
            |  spark-submit --class org.wikimedia.analytics.refinery.job.EventLoggingSanitization refinery-job.jar \
            |   --input-base-path     /wmf/data/raw/event \
            |   --input-database      event \
            |   --whitelist-path      /wmf/path/to/whitelist \
            |   --output-base-path    /user/mforns/sanitized' \
            |   --output-database     event \
            |   --since               24
            |
            |""".stripMargin, "")

        note("""NOTE: You may pass all of the described CLI options to this job in a single
            |string with --options '<options>' flag.\n""".stripMargin)

        help("help").text("Prints this usage text.")

        opt[String]('i', "input-base-path").optional().valueName("<path>").action { (x, p) =>
            p.copy(inputBasePath = if (x.endsWith("/")) x.dropRight(1) else x)
        }.text("Base path to EventLogging's data. Default: '/wmf/data/event'.")

        opt[String]('w', "whitelist-path").optional().valueName("<path>").action { (x, p) =>
            p.copy(whitelistPath = if (x.endsWith("/")) x.dropRight(1) else x)
        }.text("Path to EventLogging's whitelist file. Default: '/etc/eventlogging/whitelist.yaml'.")

        opt[DateTime]('s', "since").optional().valueName("<since-date-time>").action { (x, p) =>
            p.copy(sinceDateTime = x)
        }.text("Sanitize all data found since this date time. This may either be given as an integer " +
               "number of hours ago, or an ISO-8601 formatted date time. Default: 192 hours ago.")

        opt[DateTime]('u', "until").optional().valueName("<until-date-time>").action { (x, p) =>
            p.copy(untilDateTime = x)
        }.text("Sanitize all data found until this date time. This may either be given as an integer " +
               "number of hours ago, or an ISO-8601 formatted date time. Default: now.")

        opt[String]('o', "output-base-path").optional().valueName("<path>").action { (x, p) =>
            p.copy(outputBasePath = if (x.endsWith("/")) x.dropRight(1) else x)
        }.text("Base path for sanitized EventLogging data. Each table will be created " +
               "as a subdirectory of this path. Default: '/wmf/data/event'.")

        opt[String]('O', "output-database").optional().valueName("<database>").action { (x, p) =>
            p.copy(outputDatabase = x)
        }.text("Hive database name where to create or update sanitized tables. Default: 'event'.")

        opt[Unit]('f', "ignore-failure-flag").optional().action { (_, p) =>
            p.copy(ignoreFailureFlag = true)
        }.text("Set this if you want all partitions with failure files to be " +
               "re-sanitized. Default: false.")

        opt[Int]('P', "parallelism").optional().valueName("<parallelism>").action { (x, p) =>
            p.copy(parallelism = Some(x))
        }.text("Sanitize up to this many tables in parallel. Defaults to the number " +
               "of local CPUs (i.e. what Scala parallel collections uses).")

        opt[Int]('L', "limit").optional().valueName("<limit>").action { (x, p) =>
            p.copy(limit = Some(x))
        }.text("Only sanitize this many refine targets. This is useful while testing to reduce " +
               "the number of sanitizations to do at once. Defaults to no limit.")

        opt[Unit]('n', "dry-run").optional().action { (_, p) =>
            p.copy(dryRun = true)
        }.text("Set to true if no action should actually be taken. Default: false.")

        opt[Unit]('E', "send-email-report").optional().action { (_, p) =>
            p.copy(sendEmailReport = true)
        }.text("Set this flag if you want an email report of any failures during sanitization.")

        opt[String]('T', "smtp-uri").optional().valueName("<smtp-uri>").action { (x, p) =>
            p.copy(smtpURI = x)
        }.text("SMTP server host:port. Default: 'mx1001.wikimedia.org'.")

        opt[String]('a', "from-email").optional().valueName("<from-email>").action { (x, p) =>
            p.copy(fromEmail = x)
        }.text("Email report from sender email address.")

        opt[String]('t', "to-emails").optional().valueName("<to-emails>").action { (x, p) =>
            p.copy(toEmails = x.split(","))
        }.text("Email report recipient email addresses (comma separated). " +
               "Default: analytics-alerts@wikimedia.org")
    }

    def main(args: Array[String]): Unit = {
        val params = args.headOption match {
            case Some("--options") =>
                // Job options are given as a single string.
                argsParser.parse(args(1).split("\\s+"), Params()).getOrElse(sys.exit(1))
            case _ =>
                // Normal options usage.
                argsParser.parse(args, Params()).getOrElse(sys.exit(1))
        }

        // Exit non-zero if anything failed.
        if (apply(params)) sys.exit(0) else sys.exit(1)
    }


    /**
      * Apply sanitization to EventLogging database with the specified params.
      *
      * @param params Params
      * @return true if the sanitization succeeded, false otherwise.
      */
    def apply(params: Params): Boolean = {
        // Read whitelist from yaml file.
        val whitelistStream = new FileInputStream(new File(params.whitelistPath))
        val javaObject = new Yaml().load(whitelistStream)
        val whitelist = validateWhitelist(javaObject)

        // Get a Regex with all tables that are whitelisted.
        // This prevents Refine to collect RefineTargets for those tables
        // and to create a tree of directories just to store success files.
        val tableWhitelistRegex = new Regex("^(" + whitelist.keys.mkString("|") + ")$")

        // Initialize context.
        val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

        // Get WhitelistSanitization transform function.
        val sanitizationTransformFunction = WhitelistSanitization(whitelist)

        // Call Refine process with the sanitization transform function.
        Refine(
            inputBasePath = params.inputBasePath,
            outputBasePath = params.outputBasePath,
            spark = spark,
            databaseName = params.outputDatabase,
            sinceDateTime = params.sinceDateTime,
            untilDateTime = params.untilDateTime,
            inputPathPattern = "([^/]+)/year=(\\d{4})/month=(\\d{1,2})/day=(\\d{1,2})/hour=(\\d{1,2})",
            inputPathPatternCaptureGroups = Seq("table", "year", "month", "day", "hour"),
            inputPathDateTimeFormat = DateTimeFormat.forPattern("'year='yyyy/'month='M/'day='d/'hour='H"),
            tableWhitelistRegex = Some(tableWhitelistRegex),
            transformFunctions = Seq(sanitizationTransformFunction.asInstanceOf[Refine.TransformFunction]),
            shouldIgnoreFailureFlag = params.ignoreFailureFlag,
            parallelism = params.parallelism,
            compressionCodec = "snappy",
            limit = params.limit,
            dryRun = params.dryRun,
            shouldEmailReport = params.sendEmailReport,
            smtpURI = params.smtpURI,
            fromEmail = params.fromEmail,
            toEmails = params.toEmails
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
}
