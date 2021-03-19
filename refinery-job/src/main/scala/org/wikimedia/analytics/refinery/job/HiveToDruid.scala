package org.wikimedia.analytics.refinery.job

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime
import org.wikimedia.analytics.refinery.core.config._
import org.wikimedia.analytics.refinery.core.LogHelper
import org.wikimedia.analytics.refinery.spark.connectors.{IngestionStatus, DataFrameToDruid}
import scala.collection.immutable.ListMap
import scala.collection.JavaConversions._

// TODO: Use HivePartition.getBetweenCondition instead of the versions here.

/**
 * Loads Hive tables to Druid.
 *
 * This job creates a DataFrame on top of a given Hive table,
 * and passes it to DataFrameToDruid along with other parsed params.
 * This will load a timely slice of the Hive table to Druid.
 */
object HiveToDruid extends LogHelper with ConfigHelper {

    case class Config(
        config_file         : String         = "",
        since               : DateTime       = new DateTime(0),
        until               : DateTime       = new DateTime(0),
        database            : String         = "",
        table               : String         = "",
        druid_datasource    : Option[String] = None,
        timestamp_column    : String         = "dt",
        timestamp_format    : String         = "auto",
        dimensions          : Seq[String]    = Seq.empty,
        metrics             : Seq[String]    = Seq.empty,
        time_measures       : Seq[String]    = Seq.empty,
        transforms          : String         = "",
        segment_granularity : String         = "day",
        query_granularity   : String         = "hour",
        num_shards          : Int            = 2,
        reduce_memory       : String         = "8192",
        hadoop_queue        : String         = "default",
        druid_host          : String         = "druid1001.eqiad.wmnet",
        druid_port          : String         = "8090",
        dry_run             : Boolean        = false
    )

    object Config {
        val defaults = Config()

        val usage =
            """|Load Hive tables to Druid datasources.
               |
               |Examples:
               |
               |  spark-submit --class org.wikimedia.analytics.refinery.job.HiveToDruid refinery-job.jar \
               |   --config_file      navigationtiming.properties
               |
               |  spark-submit --class org.wikimedia.analytics.refinery.job.HiveToDruid refinery-job.jar \
               |   --since            2017-09-29T03 \
               |   --until            2017-12-15T21 \
               |   --database         event \
               |   --table            navigationtiming \
               |   --dimensions       namespaceId,wiki \
               |   --metrics          redirects \
               |   --dry_run
               |"""

        val propertiesDoc = ListMap(
            "config_file" ->
                """Config properties file. Properties specified in the command line
                  |override those specified in the config file.""",
            "since" ->
                """Start date of the interval to load (YYYY-MM-DDTHH),
                  |or number of hours ago from now (both inclusive). Mandatory.""",
            "until" ->
                """End date of the interval to load (YYYY-MM-DDTHH),
                  |or number of hours ago from now (both exclusive). Mandatory.""",
            "database" ->
                "Input Hive database name. Mandatory.",
            "table" ->
                "Input Hive table name. Mandatory.",
            "druid_datasource" ->
                "Output Druid datasource name. Default: <database>_<table>.",
            "timestamp_column" ->
                s"""Name of the column that indicates timestamp.
                   |Default: ${defaults.timestamp_column}.""",
            "timestamp_format" ->
                s"""Format of timestamp column (iso|millis|posix|auto|or any Joda time format).
                   |Default: ${defaults.timestamp_format}.""",
            "dimensions" ->
                """List of column names (<column1>,<column2>,...,<columnN>)
                  |that are to be loaded as Druid dimensions. To ingest a struct
                  |subfield as a Druid dimension, use <columnName.subfieldName>.""",
            "metrics" ->
                """List of column names (<column1>,<column2>,...,<columnN>)
                  |that are to be loaded as Druid metrics. To ingest a struct
                  |subfield as a Druid dimension, use <columnName.subfieldName>.
                  |An additional metric 'count' will always be loaded.""",
            "time_measures" ->
                """List of numeric column names (<column1>,<column2>,...,<columnN>)
                  |that are to be loaded as time measure dimensions to Druid.
                  |Only millisecond values are supported. Time measure fields will be
                  |bucketized into string dimensions, like: 50ms-250ms, 1sec-4sec, etc.""",
            "transforms" ->
                """List of transforms (<transform1>;<transform2>;...;<transformN>)
                  |that are to be applied to the schema before ingestion.
                  |Note the list is semi-colon separated (transforms can contain commas).
                  |A transform must have the form: <expression> as <name>.
                  |Where <expression> follows the spec in:
                  |http://druid.io/docs/latest/misc/math-expr.html
                  |and <name> will be the resulting field name.""",
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

    def main(args: Array[String]): Unit = {
        if (args.contains("--help")) {
            println(help(Config.usage, Config.propertiesDoc))
            sys.exit(0)
        }

        val config = loadConfig(args)
        val spark = SparkSession.builder().enableHiveSupport().appName("HiveToDruid").getOrCreate()
        val success = apply(spark)(config)

        // Exit with proper code only if not running in YARN or deploy mode is client.
        if (spark.conf.get("spark.master") != "yarn" ||
            spark.conf.get("spark.submit.deployMode") == "client") {
            sys.exit(if (success) 0 else 1)
        }
    }

    def loadConfig(args: Array[String]): Config = {
        val config = try {
            configureArgs[Config](args)
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
        val df = getDataFrameByTimeInterval(
            spark,
            config.database,
            config.table,
            config.since,
            config.until
        )

        // Parse the transforms string into a sequence of Transforms.
        val transformSeq = if (config.transforms != "") {
            config.transforms.split(";").map((t) => {
                val transformParts = t.split("\\s+[Aa][Ss]\\s+")
                DataFrameToDruid.Transform(
                    expression = transformParts(0).trim(),
                    name = transformParts(1).trim()
                )
            }).toSeq
        } else Seq.empty

        if (config.dry_run) {
            log.info("Dry run finished: no data was loaded.")
            true
        } else {
            log.info("Launching DataFrameToDruid process.")
            val dftd = new DataFrameToDruid(
                spark = spark,
                dataSource = config.druid_datasource.getOrElse(s"${config.database}_${config.table}"),
                inputDf = df,
                dimensions = config.dimensions,
                timeMeasures = config.time_measures,
                metrics = config.metrics,
                timestampColumn = config.timestamp_column,
                timestampFormat = config.timestamp_format,
                intervals = Seq((config.since, config.until)),
                transforms = transformSeq,
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

    /**
     * Returns a DataFrame for the given database and table
     * sliced by the given since and until DateTimes.
     * Uses the metaStore to obtain the partition keys.
     */
    def getDataFrameByTimeInterval(
        spark: SparkSession,
        database: String,
        table: String,
        since: DateTime,
        until: DateTime
    ): DataFrame = {
        val hiveConf = new HiveConf(spark.sparkContext.hadoopConfiguration, classOf[HiveConf])
        val metaStore = new HiveMetaStoreClient(hiveConf)
        val partitionKeys = metaStore.getTable(database, table).getPartitionKeys.map(_.getName).toSeq
        val intervalCondition = getBetweenCondition(
            getDateTimeMap(since, partitionKeys),
            getDateTimeMap(until, partitionKeys)
        )
        log.debug(s"Querying Hive for intervals: " + Seq((since, until)).toString())
        spark.sql(s"""
            SELECT *
            FROM ${database}.${table}
            WHERE $intervalCondition
        """)
    }

    /**
     * Transforms a given DateTime (2019-01-01T00) into a ListMap like:
     * ListMap("year" -> 2019, "month" -> 1, "day" -> 1, "hour" -> 0)
     * These are used for getBetweenCondition and getThresholdCondition.
     * The partitionKeys parameter should contain the names of all time
     * partitions of the data set (not necessarily in order), like:
     * Seq("year", "month", "day", "hour")
     */
    def getDateTimeMap(
        dateTime: DateTime,
        partitionKeys: Seq[String]
    ): ListMap[String, Int] = {
        // Assumes year will always be present.
        ListMap("year" -> dateTime.year.get) ++
        (if (partitionKeys.contains("month")) {
            ListMap("month" -> dateTime.monthOfYear.get) ++
            // Checks for presence of day only if month is present.
            (if (partitionKeys.contains("day")) {
                ListMap("day" -> dateTime.dayOfMonth.get) ++
                // Checks for presence of hour only if month and day are present.
                (if (partitionKeys.contains("hour")) {
                    ListMap("hour" -> dateTime.hourOfDay.get)
                } else ListMap.empty)
            } else ListMap.empty)
        } else ListMap.empty)
    }

    /**
     * Returns a string with a SparkSQL condition that can be inserted into
     * a WHERE clause to timely slice a table between two given datetimes,
     * and cause partition pruning (condition can not use CONCAT to compare).
     * The since and until datetimes are passed in the form of ListMaps:
     * ListMap("year" -> 2019, "month" -> 1, "day" -> 1, "hour" -> 0)
     */
    def getBetweenCondition(
        sinceMap: ListMap[String, Int],
        untilMap: ListMap[String, Int]
    ): String = {
        // Check that partition keys of sinceMap and untilMap match.
        val sincePartitionKeys = sinceMap.keysIterator.toList
        val untilPartitionKeys = untilMap.keysIterator.toList
        if (sincePartitionKeys != untilPartitionKeys) throw new IllegalArgumentException(
            s"Since partition keys ($sincePartitionKeys) do not " +
            s"match until partition keys ($untilPartitionKeys)."
        )

        // Get values for current partition.
        val key = sinceMap.head._1
        val since = sinceMap.head._2
        val until = untilMap.head._2

        // Check that since is smaller than until.
        if (since > until) throw new IllegalArgumentException(
            s"Since ($since) greater than until ($until) for partition key '$key'."
        )

        if (since == until) {
            // Check that since does not equal until for last partition.
            // Nothing would fulfill the condition, because until is exclusive.
            if (sinceMap.size == 1) throw new IllegalArgumentException(
                s"Since equal to until ($since) for last partition key '$key'."
            )

            // If since equals until for a given partition key, specify that
            // in the condition and AND it with the recursive call to generate
            // the between condition for further partitions.
            s"$key = $since AND " + getBetweenCondition(sinceMap.tail, untilMap.tail)
        } else {
            // If since is smaller than until, AND two threshold conditions,
            // one to specify greater than since, and another to specify
            // smaller than until.
            getThresholdCondition(sinceMap, ">") +
            " AND " +
            getThresholdCondition(untilMap, "<")
        }
    }

    /**
     * Returns a string with a SparkSQL condition that can be inserted into
     * a WHERE clause to timely slice a table below or above a given datetime,
     * and cause partition pruning (condition can not use CONCAT to compare).
     * The threshold datetime is passed in the form of a ListMap:
     * ListMap("year" -> 2019, "month" -> 1, "day" -> 1, "hour" -> 0)
     * The order parameter determines whether the condition should accept
     * values above (>) or below (<) the threshold.
     */
    def getThresholdCondition(
        thresholdMap: ListMap[String, Int],
        order: String
    ): String = {
        val key = thresholdMap.head._1
        val threshold = thresholdMap.head._2

        if (thresholdMap.size == 1) {
            // If there's only one partition key to compare,
            // output a simple comparison expression.
            // Note: > becomes inclusive, while < remains exclusive.
            if (order == ">") s"$key >= $threshold" else s"$key < $threshold"
        } else {
            // If there's more than one partition key to compare,
            // output a condition that covers the following 2 cases:
            // 1) The case where the value for the current partition is
            //    exclusively smaller (or greater) than the threshold
            //    (further partitions are irrelevant).
            // 2) The case where the value for the current partition equals
            //    the threshold, provided that further partitions fulfill the
            //    condition created by the corresponding recursive call.
            s"($key $order $threshold OR $key = $threshold AND " +
            getThresholdCondition(thresholdMap.tail, order) +
            ")"
        }
    }
}
