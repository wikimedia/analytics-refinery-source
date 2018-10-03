package org.wikimedia.analytics.refinery.job

import org.apache.log4j.LogManager
import org.apache.spark.sql.functions.{expr, col}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, SparkSession}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.wikimedia.analytics.refinery.spark.connectors.{IngestionStatus, DataFrameToDruid}
import scopt.OptionParser


object EventLoggingToDruid {

    val log = LogManager.getLogger("EventLoggingToDruid")
    val DateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH")
    val TimeMeasureBucketsSuffix = "_buckets"

    case class Params(
        table: String = "",
        startDate: DateTime = new DateTime(0),
        endDate: DateTime = new DateTime(0),
        database: String = "event",
        metrics: Seq[String] = Seq.empty,
        timeMeasures: Seq[String] = Seq.empty,
        blacklist: Seq[String] = Seq.empty,
        segmentGranularity: String = "hour",
        queryGranularity: String = "minute",
        numShards: Int = 2,
        reduceMemory: String = "8192",
        hadoopQueue: String = "default",
        druidHost: String = "druid1001.eqiad.wmnet",
        druidPort: String = "8090",
        dryRun: Boolean = false
    )

    // Support implicit DateTime conversion from CLI opt.
    // The opt can either be given in integer hours ago, or
    // as an ISO-8601 date time.
    implicit val scoptDateTimeRead: scopt.Read[DateTime] =
        scopt.Read.reads { s => {
            if (s.forall(Character.isDigit))
                DateTime.now.minusHours(s.toInt).withMinuteOfHour(0).withSecondOfMinute(0)
            else
                DateTime.parse(s, DateFormatter)
        }
    }

    val argsParser = new OptionParser[Params](
        "spark-submit --class org.wikimedia.analytics.refinery.job.EventLoggingToDruid refinery-job.jar"
    ) {
        head("""
            |EventLogging Hive tables -> Druid data sets
            |
            |Example:
            |  spark-submit --class org.wikimedia.analytics.refinery.job.EventLoggingToDruid refinery-job.jar \
            |   --database         event \
            |   --table            NavigationTiming \
            |   --start-date       2017-09-29T03 \
            |   --end-date         2017-12-15T21 \
            |   --metrics          numClicks \
            |   --timeMeasures     timeToLoad,timeToFirstClick \
            |   --blacklist        pageId,namespaceId,revId \
            |   --dry-run
            |
            |""".stripMargin, "")

        note("""NOTE: You may pass all of the described CLI options to this job in a single
               |      string with --options '<options>' flag.\n""".stripMargin)

        help("help") text "Prints this usage text and exit."

        opt[String]('T', "table").required().valueName("<table>").action { (x, p) =>
            p.copy(table = x)
        }.text("Hive input table.")

        opt[DateTime]('S', "start-date").required().valueName("<YYYY-MM-DDTHH>").action { (x, p) =>
            p.copy(startDate = new DateTime(x))
        }.text("Start date of the interval to load or number of hours ago from now (inclusive).")

        opt[DateTime]('E', "end-date").required().valueName("<YYYY-MM-DDTHH>").action { (x, p) =>
            p.copy(endDate = new DateTime(x))
        }.text("End date of the interval to load or number of hours ago from now (exclusive).")

        opt[String]('D', "database").optional().valueName("<database>").action { (x, p) =>
            p.copy(database = x)
        }.text("Hive input database. Default: event.")

        opt[Seq[String]]('m', "metrics").optional().valueName("<column1>,<column2>...").action { (x, p) =>
            p.copy(metrics = x)
        }.text("List of numeric columns that are to be loaded as metrics to Druid." +
               "You can specify a struct subfield as a metric by using column_subField." +
               "eventCount will always be a metric in the loaded data set.")

        opt[Seq[String]]('t', "timeMeasures").optional().valueName("<column1>,<column2>...").action { (x, p) =>
            p.copy(timeMeasures = x)
        }.text("List of numeric columns that are to be loaded as time measures to Druid." +
               "You can specify a struct subfield as a time measure by using column_subField." +
               "Time measure columns will be bucketized assuming they are millisecond values.")

        opt[Seq[String]]('b', "blacklist").optional().valueName("<column1>,<column2>...").action { (x, p) =>
            p.copy(blacklist = x)
        }.text("List of columns that are not to be loaded. For struct columns, " +
               "passing the column name will blacklist all data, whereas " +
               "column_subField will only blacklist this sub-field.")

        opt[String]('g', "segment-granularity").optional().valueName("<granularity>").action { (x, p) =>
            p.copy(segmentGranularity = x)
        }.text("Granularity for Druid segments (quarter|month|week|day|hour). Default: hour.")

        opt[String]('q', "query-granularity").optional().valueName("<granularity>").action { (x, p) =>
            p.copy(queryGranularity = x)
        }.text("Granularity for Druid queries (week|day|hour|minute|second). Default: minute.")

        opt[Int]('x', "num-shards").optional().valueName("<N>").action { (x, p) =>
            p.copy(numShards = x)
        }.text("Number of shards for Druid ingestion. Default: 2.")

        opt[Int]('x', "reduce-memory").optional().valueName("<N>").action { (x, p) =>
            p.copy(reduceMemory = x.toString)
        }.text("Memory to be used by Hadoop for reduce operations. Default: 8192.")

        opt[String]('h', "hadoop-queue").optional().valueName("<N>").action { (x, p) =>
            p.copy(hadoopQueue = x)
        }.text("Hadoop queue where to execute the loading. Default: default.")

        opt[String]('d', "druid-host").optional().valueName("<host>").action { (x, p) =>
            p.copy(druidHost = x)
        }.text("Druid host to load the data to. Default: druid1001.eqiad.wmnet.")

        opt[Int]('p', "druid-port").optional().valueName("<port>").action { (x, p) =>
            p.copy(druidPort = x.toString)
        }.text("Druid port to load the data to. Default: 8090.")

        opt[Unit]('n', "dry-run").optional().action { (x, p) =>
            p.copy(dryRun = true)
        }.text("Do not execute any loading, only check and print parameters.")
    }

    val blacklistedHiveFields = Set("year", "month", "day", "hour")
    val blacklistedCapsuleFields = Set("schema", "seqId", "uuid", "clientValidated", "isTruncated", "clientIp")
    val legitCapsuleFields = Set("wiki", "webHost", "revision", "topic", "recvFrom", "userAgent")

    // Entry point
    def main(args: Array[String]): Unit = {
        // Parse arguments.
        val params = args.headOption match {
            case Some("--options") =>
                // If job options are given as a single string.
                // Split them before passing them to argsParser.
                argsParser.parse(args(1).split("\\s+"), Params()).getOrElse(sys.exit(1))
            case _ =>
                argsParser.parse(args, Params()).getOrElse(sys.exit(1))
        }

        // Initialize SparkSession.
        val spark = SparkSession.builder().enableHiveSupport().appName("EventLoggingToDruid").getOrCreate()

        // Execute loading.
        val success = apply(params, spark)

        // Exit with proper exit val if not running in YARN.
        if (spark.conf.get("spark.master") != "yarn") {
            sys.exit(if (success) 0 else 1)
        }
    }

    // This will be called after command line parameters have been parsed and checked.
    def apply(
        params: Params,
        spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    ): Boolean = {

        log.info(s"Starting process for ${params.database}_${params.table}.")
        log.info(s"Querying Hive for intervals: " + Seq((params.startDate, params.endDate)).toString())

        // Get data already filtered by time range.
        val comparisonFormat = "yyyyMMddHH"
        val comparisonStartDate = params.startDate.toString(comparisonFormat)
        val comparisonEndDate = params.endDate.toString(comparisonFormat)
        val concatTimestamp = "CONCAT(year, LPAD(month, 2, '0'), LPAD(day, 2, '0'), LPAD(hour, 2, '0'))"
        val df = spark.sql(s"""
            SELECT *
            FROM ${params.database}.${params.table}
            WHERE $concatTimestamp >= $comparisonStartDate
            AND $concatTimestamp < $comparisonEndDate
        """)

        log.info("Preparing dimensions and metrics.")

        // Flatten nested fields.
        val flatColumns = getFlatColumns(df.schema)
        val flatDf = df.select(flatColumns:_*)

        // Remove blacklisted fields.
        val cleanColumns = getCleanColumns(flatDf.schema, params.blacklist)
        val cleanDf = flatDf.select(cleanColumns:_*)

        // Bucketize time measures.
        val bucketizedColumns = getBucketizedColumns(cleanDf.schema, params.timeMeasures)
        val finalDf = cleanDf.select(bucketizedColumns:_*)

        // Get dimensions.
        val dimensions = getDimensions(finalDf.schema, params.metrics)

        log.info("Dimensions: " + dimensions.mkString(", "))
        log.info("Metrics: " + params.metrics.mkString(", "))

        if (params.dryRun) {
            log.info("Dry run finished: no data was loaded.")
            true
        } else {
            // Execute loading process.
            log.info("Launching DataFrameToDruid process.")
            val dftd = new DataFrameToDruid(
                spark = spark,
                dataSource = s"${params.database}_${params.table}",
                inputDf = finalDf,
                dimensions = dimensions,
                metrics = params.metrics,
                intervals = Seq((params.startDate, params.endDate)),
                timestampColumn = "dt",
                timestampFormat = "auto",
                segmentGranularity = params.segmentGranularity,
                queryGranularity = params.queryGranularity,
                numShards = params.numShards,
                reduceMemory = params.reduceMemory,
                hadoopQueue = params.hadoopQueue,
                druidHost = params.druidHost,
                druidPort = params.druidPort
            ).start().await()
            log.info("Done.")

            // Return whether the process was successful.
            dftd.status() == IngestionStatus.Done
        }
    }

    def getFlatColumns(schema: StructType, prefix: String = null): Seq[Column] = {
        // HACK: This map corrects casing for capsule fields, given that Hive kills camelCase.
        val capsuleFields = legitCapsuleFields.union(blacklistedCapsuleFields)
        val capsuleCaseMap = capsuleFields.map(f => (f.toLowerCase(), f)).toMap

        schema.fields.flatMap(field => {
            val columnName = if (prefix == null) field.name else prefix + "." + field.name
            val columnAlias = columnName.split("\\.").map(n => capsuleCaseMap.getOrElse(n, n)).mkString("_")

            field.dataType match {
                case struct: StructType => getFlatColumns(struct, columnName)
                case _ => Seq(col(columnName).as(columnAlias))
            }
        })
    }

    def getCleanColumns(schema: StructType, blacklist: Seq[String]): Seq[Column] = {
        val blacklistNames = blacklist.toSet
            .union(blacklistedCapsuleFields)
            .union(blacklistedHiveFields)
        val fieldNames = schema.fields.map(f => f.name)
        val withPrefix = "([^_]*)_.*".r
        fieldNames.filter(f =>
            !blacklistNames.contains(f) &&
            (f match {
                case withPrefix(prefix) => !blacklistNames.contains(prefix)
                case _ => true
            })
        ).map(col)
    }

    def getBucketizedColumns(schema: StructType, timeMeasures: Seq[String]): Seq[Column] = {
        val fieldNames = schema.fields.map(f => f.name)
        fieldNames.map(f => f match {
            case _ if timeMeasures.contains(f) =>
                expr(s"""CASE
                    WHEN ($f >= 0 AND $f < 50) THEN '0ms-50ms'
                    WHEN ($f >= 50 AND $f < 250) THEN '50ms-250ms'
                    WHEN ($f >= 250 AND $f < 1000) THEN '250ms-1sec'
                    WHEN ($f >= 1000 AND $f < 4000) THEN '1sec-4sec'
                    WHEN ($f >= 4000 AND $f < 15000) THEN '4sec-15sec'
                    WHEN ($f >= 15000 AND $f < 60000) THEN '15sec-1min'
                    WHEN ($f >= 60000 AND $f < 240000) THEN '1min-4min'
                    WHEN ($f >= 240000 AND $f < 900000) THEN '4min-15min'
                    WHEN ($f >= 900000 AND $f < 3600000) THEN '15min-1hr'
                    WHEN ($f >= 3600000 AND $f < 18000000) THEN '1hr-5hr'
                    WHEN ($f >= 18000000 AND $f < 86400000) THEN '5hr-24hr'
                    WHEN ($f >= 86400000) THEN '24hr+'
                    ELSE NULL
                END""").as(f + TimeMeasureBucketsSuffix)
            case _ => col(f)
        })
    }

    def getDimensions(
        schema: StructType,
        metrics: Seq[String]
    ): Seq[String] = {
        val allFieldNames = schema.fields.filter(f => f.name != "dt").map(_.name)
        allFieldNames.filter(!metrics.contains(_))
    }
}
