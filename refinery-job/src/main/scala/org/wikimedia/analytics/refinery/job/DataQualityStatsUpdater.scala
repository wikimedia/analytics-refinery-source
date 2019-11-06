
package org.wikimedia.analytics.refinery.job

import java.util.UUID.randomUUID
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

/**
 * Data Quality Stats Updater
 *
 * Updates the given data_quality_stats table with the data present in the
 * given data_quality_stats_incoming table. The update is just for the given
 * partition, meaning: source_table, query_name and granularity.
 *
 * This is needed because the data_quality_stats table is not partitioned by
 * time fields. We decided to not do so, to avoid having a big number of Hive
 * partitions with very small files.
 *
 * This job reads data from both the data_quality_stats table and the
 * data_quality_stats_incoming table, combines their data, and writes the
 * results to a temporary directory. Then moves the updated data to overwrite
 * the original data_quality_stats data.
 *
 * Parameters:
 *   qualityTable     Fully qualified name of the data_quality_stats table.
 *   incomingTable    Fully qualified name of the data_quality_stats_incoming
 *                    table.
 *   sourceTable      Fully qualified name of the table used to calculate the
 *                    metrics to be updated.
 *   queryName        Name of the query file (without .hql) used to calculate
 *                    the metrics to be updated.
 *   granularity      Granularity of the data to update (hourly|daily|monthly).
 *   tempDirectory    HDFS path where to write temporary files.
 *   outputBasePath   HDFS base path of the data_quality_stats data set to
 *                    update.
 *
 * Example of usage:
 *
 * sudo -u analytics spark2-submit \
 *   --master yarn \
 *   --deploy-mode cluster \
 *   --class org.wikimedia.analytics.refinery.job.DataQualityStatsUpdater \
 *   /srv/deployment/analytics/refinery/artifacts/refinery-job.jar \
 *   --quality-table wmf.data_quality_stats \
 *   --incoming-table analytics.data_quality_stats_incoming \
 *   --source-table event.navigationtiming \
 *   --query-name useragent_entropy \
 *   --granularity hourly \
 *   --temp-directory /tmp/analytics \
 *   --output-base-path /wmf/data/wmf/data_quality_stats
 */

object DataQualityStatsUpdater {

    case class Params(
        qualityTable: String = "",
        incomingTable: String = "",
        sourceTable: String = "",
        queryName: String = "",
        granularity: String = "",
        tempDirectory: String = "",
        outputBasePath: String = ""
    )

    val argsParser = new OptionParser[Params]("Data quality stats updater") {
        help("help") text ("Print this usage text and exit.")

        opt[String]("quality-table") required() valueName ("<table_name>") action { (x, p) =>
            p.copy(qualityTable = x)
        } text ("Fully qualified name of the data_quality_stats table.")

        opt[String]("incoming-table") required() valueName ("<table_name>") action { (x, p) =>
            p.copy(incomingTable = x)
        } text ("Fully qualified name of the data_quality_stats_incoming table.")

        opt[String]("source-table") required() valueName ("<table_name>") action { (x, p) =>
            p.copy(sourceTable = x)
        } text ("Fully qualified name of the table used to calculate the metrics to be updated.")

        opt[String]("query-name") required() valueName ("<query_name>") action { (x, p) =>
            p.copy(queryName = x)
        } text ("Name of the query file (without .hql) used to calculate the metrics to be updated.")

        opt[String]("granularity") required() valueName ("<granularity>") action { (x, p) =>
            p.copy(granularity = x)
        } text ("Granularity of the data to update (hourly|daily|monthly).")

        opt[String]("temp-directory") required() valueName ("<path>") action { (x, p) =>
            p.copy(tempDirectory = if (x.endsWith("/")) x.dropRight(1) else x)
        } text ("HDFS path where to write temporary files.")

        opt[String]("output-base-path") required() valueName ("<path>") action { (x, p) =>
            p.copy(outputBasePath = if (x.endsWith("/")) x.dropRight(1) else x)
        } text ("HDFS base path of the data_quality_stats data set to update.")
    }

    /**
     * Parse command line arguments and call apply.
     */
    def main(args: Array[String]): Unit = {
        val params = argsParser.parse(args, Params()).getOrElse(sys.exit(1))
        apply(params)
    }

    /**
     * Main method. Configure Spark and process data.
     */
    def apply(params: Params): Unit = {
        // Set up Spark.
        val conf = new SparkConf().setAppName(s"DataQualityStatsUpdater")
        val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

        // Get data from both data quality stats table and incoming table.
        // The ROW_NUMBER trick is there to remove possible duplicates, in case
        // this job is doing reruns and updating a time slot that does already
        // have data in the data_quality_stats table.
        val df = spark.sql(s"""
            WITH unioned AS (
                SELECT
                    'current' AS data_type,
                    dt, metric, value
                FROM ${params.qualityTable}
                WHERE
                    source_table = '${params.sourceTable}' AND
                    query_name = '${params.queryName}' AND
                    granularity = '${params.granularity}'
                UNION ALL
                SELECT
                    'incoming' AS data_type,
                    dt, metric, value
                FROM ${params.incomingTable}
                WHERE
                    source_table = '${params.sourceTable}' AND
                    query_name = '${params.queryName}' AND
                    granularity = '${params.granularity}'
            ), numbered AS (
                SELECT dt, metric, value,
                    ROW_NUMBER() OVER (
                        PARTITION BY dt, metric
                        ORDER BY IF(data_type = 'incoming', 0, 1)
                    ) AS row_num
                FROM unioned
            )
            SELECT dt, metric, value
            FROM numbered
            WHERE row_num = 1
        """)

        // Write data to temp directory.
        val tempSubdirectory = Seq(
            params.tempDirectory,
            "data_quality_stats_updater",
            randomUUID.toString
        ).mkString("/")
        df.repartition(1).write.
            format("csv").
            option("sep", "\t").
            option("compression", "none").
            save(tempSubdirectory)

        // Prepare final location.
        val outputSubdirectory = Seq(
            params.outputBasePath,
            s"source_table=${params.sourceTable}",
            s"query_name=${params.queryName}",
            s"granularity=${params.granularity}"
        ).mkString("/")
        val outputPath = new Path(outputSubdirectory)
        fs.delete(outputPath, true)
        fs.mkdirs(outputPath)

        // Transfer updated files.
        val tempPath = new Path(tempSubdirectory)
        fs.listStatus(tempPath).foreach { f =>
            val fileName = f.getPath.getName
            fs.rename(
                new Path(tempSubdirectory + "/" + fileName),
                new Path(outputSubdirectory + "/" + fileName)
            )
        }

        // If partition did not exist, needs to be created.
        spark.sql(s"MSCK REPAIR TABLE ${params.qualityTable}")

        // Clean up.
        fs.delete(tempPath, true)
    }

}
