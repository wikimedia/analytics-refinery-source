package org.wikimedia.analytics.refinery.job

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime
import org.wikimedia.analytics.refinery.core.HivePartition
import org.wikimedia.analytics.refinery.spark.connectors.{DataFrameToDruid, IngestionStatus}
import org.wikimedia.analytics.refinery.tools.LogHelper
import org.wikimedia.analytics.refinery.tools.config._

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
        snapshot            : Option[String] = None,
        druid_datasource    : Option[String] = None,
        timestamp_column    : String         = "dt",
        timestamp_format    : String         = "auto",
        dimensions          : Seq[String]    = Seq.empty,
        metrics             : Seq[String]    = Seq.empty,
        time_measures       : Seq[String]    = Seq.empty,
        transforms          : String         = "",
        count_metric_name   : String         = "count",
        segment_granularity : String         = "day",
        query_granularity   : String         = "hour",
        num_shards          : Int            = 2,
        map_memory          : String         = "2048",
        reduce_memory       : String         = "8192",
        hadoop_queue        : String         = "default",
        temp_directory      : Option[String] = None,
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
            "snapshot" ->
                """Value of the Hive snapshot to load in druid. If set, `since` and
                  |`until` are used only to define druid interval, not hive snapshot.""",
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
            "count_metric_name" ->
                s"""Name of the metric of type count that is added by default.
                   |Default: ${defaults.count_metric_name}.""",
            "segment_granularity" ->
                s"""Granularity for Druid segments (quarter|month|week|day|hour).
                   |Default: ${defaults.segment_granularity}.""",
            "query_granularity" ->
                s"""Granularity for Druid queries (week|day|hour|minute|second).
                   |Default: ${defaults.query_granularity}.""",
            "num_shards" ->
                s"Number of shards for Druid ingestion. Default: ${defaults.num_shards}.",
            "map_memory" ->
                s"""Memory to be used by Hadoop for map operations.
                   |Default: ${defaults.map_memory}.""",
            "reduce_memory" ->
                s"""Memory to be used by Hadoop for reduce operations.
                   |Default: ${defaults.reduce_memory}.""",
            "hadoop_queue" ->
                s"Hadoop queue where to execute the loading. Default: ${defaults.hadoop_queue}.",
            "temp_directory" ->
                """HDFS path where to remporarily store files to load.
                   |Default set in refinery-spark DataFrameToDruid.scala.""",
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
            config.until,
            config.snapshot
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
                countMetricName = config.count_metric_name,
                segmentGranularity = config.segment_granularity,
                queryGranularity = config.query_granularity,
                numShards = config.num_shards,
                mapMemory = config.map_memory,
                reduceMemory = config.reduce_memory,
                hadoopQueue = config.hadoop_queue,
                druidHost = config.druid_host,
                druidPort = config.druid_port,
                tempFilePathOver = config.temp_directory.getOrElse(null.asInstanceOf[String])
            ).start().await()

            log.info("Done.")
            dftd.status() == IngestionStatus.Done
        }
    }

    /**
     * Returns a DataFrame for the given database and table
     * sliced by the given since and until DateTimes.
     * Uses the metaStore to obtain the partition keys.
     * If the table is snapshot-based, selects the given snapshot.
     * If the table has no partitions, selects everything in it.
     */
    def getDataFrameByTimeInterval(
        spark: SparkSession,
        database: String,
        table: String,
        since: DateTime,
        until: DateTime,
        snapshot: Option[String]
    ): DataFrame = {
        val hiveConf = new HiveConf(spark.sparkContext.hadoopConfiguration, classOf[HiveConf])
        val metaStore = new HiveMetaStoreClient(hiveConf)
        val partitionKeys = metaStore.getTable(database, table).getPartitionKeys.map(_.getName).toSeq
        val intervalCondition = if (partitionKeys.isEmpty) {
            "true"
        } else if (partitionKeys.contains("snapshot")) {
            if (snapshot.isDefined) {
                HivePartition.getSnapshotCondition(snapshot.get)
            } else {
                HivePartition.getSnapshotCondition(since, until)
            }
        } else {
            HivePartition.getBetweenCondition(since, until, partitionKeys)
        }
        log.debug(s"Querying Hive for intervals: " + Seq((since, until)).toString())
        spark.sql(s"""
            SELECT *
            FROM ${database}.${table}
            WHERE $intervalCondition
        """)
    }
}
