package org.wikimedia.analytics.refinery.job

import org.apache.spark.sql.functions.{expr, col}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, SparkSession}
import org.joda.time.DateTime
import org.wikimedia.analytics.refinery.core.config._
import org.wikimedia.analytics.refinery.core.LogHelper
import org.wikimedia.analytics.refinery.spark.connectors.{IngestionStatus, DataFrameToDruid}
import scala.collection.immutable.ListMap


object EventLoggingToDruid extends LogHelper with ConfigHelper {

    case class Config(
        config_file         : String      = "",
        start_date          : DateTime    = new DateTime(0),
        end_date            : DateTime    = new DateTime(0),
        database            : String      = "event",
        table               : String      = "",
        dimensions          : Seq[String] = Seq.empty,
        time_measures       : Seq[String] = Seq.empty,
        metrics             : Seq[String] = Seq.empty,
        segment_granularity : String      = "hour",
        query_granularity   : String      = "minute",
        num_shards          : Int         = 2,
        reduce_memory       : String      = "8192",
        hadoop_queue        : String      = "default",
        druid_host          : String      = "druid1001.eqiad.wmnet",
        druid_port          : String      = "8090",
        dry_run             : Boolean     = false
    )

    object Config {
        val defaults = Config()

        val usage =
            """|Load EventLogging Hive tables to Druid datasources.
               |
               |Examples:
               |
               |  spark-submit --class org.wikimedia.analytics.refinery.job.EventLoggingToDruid refinery-job.jar \
               |   --config_file      ./navigation_timing.properties
               |
               |  spark-submit --class org.wikimedia.analytics.refinery.job.EventLoggingToDruid refinery-job.jar \
               |   --start_date       2017-09-29T03 \
               |   --end_date         2017-12-15T21 \
               |   --database         event \
               |   --table            NavigationTiming \
               |   --dimensions       namespaceId,editCountBucket \
               |   --time_measures    timeToLoad,timeToClose \
               |   --metrics          numClicks \
               |   --dry_run
               |"""

        val propertiesDoc = ListMap(
            "config_file" ->
                """Config properties file. Properties specified in the command line
                  |override those specified in the config file.""",
            "start_date" ->
                """Start date of the interval to load (YYYY-MM-DDTHH),
                  |or number of hours ago from now (both inclusive).""",
            "end_date" ->
                """End date of the interval to load (YYYY-MM-DDTHH),
                  |or number of hours ago from now (both exclusive).""",
            "database" ->
                s"Input Hive database name. Default: ${defaults.database}.",
            "table" ->
                "Input Hive table name.",
            "dimensions" ->
                """List of column names (<column1>,<column2>,...,<columnN>)
                  |that are to be loaded as Druid dimensions. To ingest a struct
                  |subfield as a Druid dimension, use <columnName_subfieldName>.""",
            "time_measures" ->
                """List of numeric column names (<column1>,<column2>,...,<columnN>)
                  |that are to be loaded as time measure dimensions to Druid. To ingest
                  |a struct subfield as a time measure, use <columnName_subfieldName>.
                  |Only millisecond values are supported. Time measure fields will be
                  |bucketized into string dimensions, like: 50ms-250ms, 1sec-4sec, etc.""",
            "metrics" ->
                """List of column names (<column1>,<column2>,...,<columnN>)
                  |that are to be loaded as Druid metrics. To ingest a struct
                  |subfield as a Druid dimension, use <columnName_subfieldName>.
                  |An additional metric eventCount will always be loaded.""",
            "segment_granularity" ->
                s"""Granularity for Druid segments (quarter|month|week|day|hour).
                   |Default: ${defaults.segment_granularity}.""",
            "query_granularity" ->
                s"""Granularity for Druid queries (week|day|hour|minute|second).
                   |Default: ${defaults.query_granularity}.""",
            "num_shards" ->
                s"Number of shards for Druid ingestion. Default: ${defaults.num_shards}.",
            "reduce_memory" ->
                s"""Memory to be used by Hadoop for reduce operations.
                   |Default: ${defaults.reduce_memory}.""",
            "hadoop_queue" ->
                s"Hadoop queue where to execute the loading. Default: ${defaults.hadoop_queue}.",
            "druid_host" ->
                s"Druid host to load the data to. Default: ${defaults.druid_host}.",
            "druid_port" ->
                s"Druid port to load the data to. Default: ${defaults.druid_port}.",
            "dry_run" ->
                s"""Set to 'true' to not execute any loading, only check and print parameters.
                   |Set to 'false' to execute the job normaly. Default: ${defaults.dry_run}."""
        )
    }


    val BlacklistedHiveFields = Set(
        "year",
        "month",
        "day",
        "hour"
    )
    val BlacklistedCapsuleFields = Set(
        "clientIp",
        "clientValidated",
        "dt",
        "isTruncated",
        "schema",
        "seqId",
        "uuid"
    )
    val LegitCapsuleFields = Set(
        "geocoded_data",
        "recvFrom",
        "revision",
        "topic",
        "userAgent",
        "webHost",
        "wiki"
    )
    val TimeMeasureBucketsSuffix = "_buckets"


    def main(args: Array[String]): Unit = {
        if (args.contains("--help")) {
            println(help(Config.usage, Config.propertiesDoc))
            sys.exit(0)
        }

        val config = loadConfig(args)
        if (!validFieldLists(config)) {
            println("Some fields specified as dimensions, time measures or metrics are invalid.")
            sys.exit(1)
        }

        val spark = SparkSession.builder().enableHiveSupport().appName("EventLoggingToDruid").getOrCreate()
        val success = apply(spark)(config)

        // Exit with proper code only if not running in YARN.
        if (spark.conf.get("spark.master") != "yarn") {
            sys.exit(if (success) 0 else 1)
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

    def apply(
        spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    )(config: Config): Boolean = {
        log.info(s"Starting process for ${config.database}_${config.table}.")

        log.debug(s"Querying Hive for intervals: " + Seq((config.start_date, config.end_date)).toString())
        val comparisonFormat = "yyyyMMddHH"
        val comparisonStartDate = config.start_date.toString(comparisonFormat)
        val comparisonEndDate = config.end_date.toString(comparisonFormat)
        val concatTimestamp = "CONCAT(year, LPAD(month, 2, '0'), LPAD(day, 2, '0'), LPAD(hour, 2, '0'))"
        val df = spark.sql(s"""
            SELECT *
            FROM ${config.database}.${config.table}
            WHERE $concatTimestamp >= $comparisonStartDate
            AND $concatTimestamp < $comparisonEndDate
        """)

        log.debug("Flattening nested fields.")
        val flatColumns = getFlatColumns(df.schema)
        val flatDf = df.select(flatColumns:_*)

        log.debug("Removing unused fields.")
        val cleanColumns = getCleanColumns(flatDf.schema, config)
        val cleanDf = flatDf.select(cleanColumns:_*)

        log.debug("Bucketizing time measures.")
        val bucketizedColumns = getBucketizedColumns(cleanDf.schema, config)
        val finalDf = cleanDf.select(bucketizedColumns:_*)

        if (config.dry_run) {
            log.info("Dry run finished: no data was loaded.")
            true
        } else {
            log.info("Launching DataFrameToDruid process.")
            val dftd = new DataFrameToDruid(
                spark = spark,
                dataSource = s"${config.database}_${config.table}",
                inputDf = finalDf,
                dimensions = config.dimensions,
                metrics = config.metrics,
                intervals = Seq((config.start_date, config.end_date)),
                timestampColumn = "dt",
                timestampFormat = "auto",
                segmentGranularity = config.segment_granularity,
                queryGranularity = config.query_granularity,
                numShards = config.num_shards,
                reduceMemory = config.reduce_memory,
                hadoopQueue = config.hadoop_queue,
                druidHost = config.druid_host,
                druidPort = config.druid_port
            ).start().await()

            log.info("Done.")
            dftd.status() == IngestionStatus.Done
        }
    }

    def validFieldLists(config: Config): Boolean = {
        val withPrefix = "([^_]*)_.*".r
        val notBlacklisted = (f: String) => (
            !BlacklistedHiveFields.contains(f) &&
            !BlacklistedCapsuleFields.contains(f)
        )
        Seq(config.dimensions, config.time_measures, config.metrics).forall { s =>
            s.forall(f => {
                f match {
                    // Checks whether the prefix is a blacklisted field.
                    case withPrefix(prefix) => notBlacklisted(prefix) && notBlacklisted(f)
                    case _ => notBlacklisted(f)
                }
            })
        }
    }

    def getFlatColumns(schema: StructType, prefix: String = null): Seq[Column] = {
        // HACK: This map corrects casing for capsule fields, given that Hive kills camelCase.
        val capsuleFields = LegitCapsuleFields.union(BlacklistedCapsuleFields)
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

    def getCleanColumns(schema: StructType, config: Config): Seq[Column] = {
        val specifiedFieldNames = config.dimensions ++ config.time_measures ++ config.metrics ++ Seq("dt")
        val currentFieldNames = schema.fields.map(f => f.name)
        currentFieldNames.filter(f => specifiedFieldNames.contains(f)).map(col)
    }

    def getBucketizedColumns(schema: StructType, config: Config): Seq[Column] = {
        val fieldNames = schema.fields.map(f => f.name)
        fieldNames.map(f => f match {
            case _ if config.time_measures.contains(f) =>
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

}
