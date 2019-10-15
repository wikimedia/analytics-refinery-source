
package org.wikimedia.analytics.refinery.job.mediawikihistory

import java.util.{TimeZone, Calendar}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.SparkConf
import org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized.{
    MediawikiEvent,
    MediawikiEventUserDetails,
    MediawikiEventPageDetails
}
import scopt.OptionParser

/**
 * MediaWiki History Dumper
 *
 * Dumps the given MediaWiki History data set into a given base path,
 * in a Bzip2 format, and partitioned by wiki and time_bucket.
 * Size of the time bucket varies depending on the size of the wiki.
 * For example, big wikis might be split in 1-month buckets, while
 * medium wikis might be split in 1-year buckets, and small wikis
 * might be outputed as a single file.
 *
 * Parameters:
 *   snapshot         Mediawiki snapshot to dump (usually YYYY-MM).
 *   inputBasePath    HDFS base path where to read data from.
 *   tempDirectory    HDFS temporary directory for intermediate files.
 *   tempPartitions   Number of partitions to rehash data with (internal).
 *   outputBasePath   HDFS base path where to write the dump.
 *
 * Example of usage:
 *
 * sudo -u analytics spark2-submit \
 *     --master yarn \
 *     --deploy-mode cluster \
 *     --executor-memory 32G \
 *     --driver-memory 16G \
 *     --executor-cores 4 \
 *     --conf spark.dynamicAllocation.maxExecutors=32 \
 *     --class org.wikimedia.analytics.refinery.job.mediawikihistory.MediawikiHistoryDumper \
 *     /path/to/refinery/jar/refinery-job-0.0.100.jar \
 *     --snapshot 2019-06 \
 *     --input-base-path /wmf/data/wmf/mediawiki/history \
 *     --temp-directory /tmp/mforns/mediawiki_history_dumps_12345 \
 *     --temp-partitions 256 \
 *     --output-base-path /wmf/data/archive/mediawiki/history
 *
 */

object MediawikiHistoryDumper {

    case class Params(
        snapshot: String = "",
        inputBasePath: String = "",
        tempDirectory: String = "",
        tempPartitions: Int = 256,
        outputBasePath: String = ""
    )

    val argsParser = new OptionParser[Params]("Mediawiki history dumper") {
        help("help") text ("Print this usage text and exit.")

        opt[String]('s', "snapshot") required() valueName ("<snapshot>") action { (x, p) =>
            p.copy(snapshot = x)
        } text ("Mediawiki snapshot to dump (usually YYYY-MM).")

        opt[String]('i', "input-base-path") required() valueName ("<path>") action { (x, p) =>
            p.copy(inputBasePath = if (x.endsWith("/")) x.dropRight(1) else x)
        } text ("HDFS base path where to read data from.")

        opt[String]('i', "temp-directory") required() valueName ("<path>") action { (x, p) =>
            p.copy(tempDirectory = if (x.endsWith("/")) x.dropRight(1) else x)
        } text ("HDFS temporary directory for intermediate files.")

        opt[Int]('p', "temp-partitions") optional() valueName ("<number>") action { (x, p) =>
            p.copy(tempPartitions = x)
        } text ("Number of partitions to rehash data with (internal).")

        opt[String]('o', "output-base-path") required() valueName ("<path>") action { (x, p) =>
            p.copy(outputBasePath = if (x.endsWith("/")) x.dropRight(1) else x)
        } text ("HDFS base path where to write the dump.")
    }

    val WikisInMonthlyBuckets = Seq(
        "wikidatawiki",
        "commonswiki",
        "enwiki"
    )
    val WikisInYearlyBuckets = Seq(
        "dewiki", "frwiki", "eswiki", "itwiki", "ruwiki", "jawiki", "viwiki",
        "zhwiki", "ptwiki", "enwiktionary", "plwiki", "nlwiki", "svwiki",
        "metawiki", "arwiki", "shwiki", "cebwiki", "mgwiktionary", "fawiki",
        "frwiktionary", "ukwiki", "hewiki", "kowiki", "srwiki", "trwiki",
        "loginwiki", "huwiki", "cawiki", "nowiki", "mediawikiwiki", "fiwiki",
        "cswiki", "idwiki", "rowiki", "enwikisource", "frwikisource",
        "ruwiktionary", "dawiki", "bgwiki", "incubatorwiki", "enwikinews",
        "specieswiki", "thwiki"
    )

    /**
     * Parse command line arguments and call apply.
     */
    def main(args: Array[String]): Unit = {
        val params = argsParser.parse(args, Params()).getOrElse(sys.exit(1))
        apply(params)
    }

    /**
     * Main method. Configure Spark, and process data.
     */
    def apply(params: Params): Unit = {
        val conf = new SparkConf().
            setAppName(s"MediawikiHistoryDumper").
            set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
            registerKryoClasses(Array(
                classOf[MediawikiEvent],
                classOf[MediawikiEventUserDetails],
                classOf[MediawikiEventPageDetails]
            ))
        val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

        repartitionData(
            spark,
            params.inputBasePath,
            params.snapshot,
            params.tempPartitions,
            params.tempDirectory
        )

        archiveData(
            spark,
            params.tempDirectory,
            params.outputBasePath,
            params.snapshot
        )
    }

    /**
     * Rehash data into proper partitioning.
     *
     * A mediawiki_history snapshot has no further partitions. But a dump
     * snapshot needs to be partitioned by wiki and time bucket as well.
     * This method rehashes the data so that all records for each pair
     * (wiki, time_bucket) are stored in a separate and single file.
     */
    def repartitionData(
        spark: SparkSession,
        inputBasePath: String,
        snapshot: String,
        tempPartitions: Int,
        tempDirectory: String
    ): Unit = {
        import spark.implicits._
        val snapshotPath = s"${inputBasePath}/snapshot=${snapshot}"
        spark.read.parquet(snapshotPath).
            rdd.
            map(r => MediawikiEvent.fromRow(r)).
            // Add desired partition fields: wiki and time bucket.
            map(e => (e.wikiDb, eventTimeBucket(e), e.toTSVLine)).
            // Filter out records with unknown time_bucket. See eventTimeBucket().
            filter(e => e._2 != "unknown").
            toDF.
            withColumnRenamed("_1", "wiki").
            withColumnRenamed("_2", "time_bucket").
            // The following line applies the repartitioning. It redistributes
            // the data among tempPartitions partitions. And makes sure that
            // all records for a given pair (wiki, time_bucket) go to the same
            // partition. TempPartitions must be big enough so that 1 partition
            // can be handled by 1 Spark task. Note that one node will process
            // up to 4 such tasks. This rehashing allows later to write all data
            // belonging to the same pair (wiki, time_bucket) to be written to
            // the same file.
            repartition(tempPartitions, col("wiki"), col("time_bucket")).
            write.
            mode(SaveMode.Overwrite).
            // Then each task will write each pair (wiki, time_bucket) into
            // their own file. Given the previous repartitioning, this ensures
            // one single file per (wiki, time_bucket) pair.
            partitionBy("wiki", "time_bucket").
            option("compression", "bzip2").
            text(tempDirectory)
    }

    /**
     * Extract the time_bucket from an event depending on the wiki category.
     */
    def eventTimeBucket(event: MediawikiEvent): String = {
        event.eventTimestamp match {
            case Some(timestamp) =>
                val nowCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
                val eventCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
                eventCalendar.setTime(timestamp)
                val year = eventCalendar.get(Calendar.YEAR)
                if (year < 2001 || eventCalendar.after(nowCalendar)) {
                    // Mark events with timestamps older than 2001 as unknown.
                    // Those are rare mediawiki history reconstruction errors.
                    // See: https://phabricator.wikimedia.org/T218824
                    // Also mark events with timestamp after current date as unknown.
                    // Those are rare archived-revisions with incorrect dates and null page.
                    // See: https://phabricator.wikimedia.org/T235269
                    "unknown"
                } else if (MediawikiHistoryDumper.WikisInMonthlyBuckets.contains(event.wikiDb)) {
                    year.toString + "-%02d".format(eventCalendar.get(Calendar.MONTH) + 1)
                } else if (MediawikiHistoryDumper.WikisInYearlyBuckets.contains(event.wikiDb)) {
                    year.toString
                } else {
                    "all-time"
                }
            // Mark events with null timestamp as unknown. Those are page create
            // events that were inferred by the mediawiki history reconstruction
            // and don't give any valuable information to the user.
            case None => "unknown"
        }
    }

    /**
     * Moves the repartitioned data to its final location and naming.
     *
     * The repartitioning process outputs files in an "ugly" directory tree.
     * Folders use Hive syntax (/wiki=enwiki/time_bucket=2019-07/0_000000.txt).
     * This method moves files to their final location and renames them
     * for pretty naming (/enwiki/2019-07.tsv.bz2).
     */
    def archiveData(
        spark: SparkSession,
        tempDirectory: String,
        outputBasePath: String,
        snapshot: String
    ): Unit = {
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val wikiDirectories = fs.listStatus(new Path(tempDirectory))
        wikiDirectories.foreach { wikiDirectory =>
            if (wikiDirectory.getPath.getName != "_SUCCESS") {
                val timeDirectories = fs.listStatus(wikiDirectory.getPath)
                timeDirectories.foreach { timeDirectory =>
                    // The substring removes Hive partition prefix (wiki=).
                    val wiki = wikiDirectory.getPath.getName.substring(5)
                    // The substring removes Hive partition prefix (time_bucket=).
                    val timeBucket = timeDirectory.getPath.getName.substring(12)

                    val dataFiles = fs.listStatus(timeDirectory.getPath)
                    if (dataFiles.length > 1) {
                        // This should not happen.
                        // Just making sure that we do not leave out any file.
                        throw new RuntimeException("More than one file per folder generated.")
                    }
                    val sourcePath = dataFiles(0).getPath

                    val destinationDirectory = Seq(
                        outputBasePath,
                        snapshot,
                        wiki
                    ).mkString("/")
                    fs.mkdirs(new Path(destinationDirectory))
                    val destinationPath = new Path(Seq(
                        destinationDirectory,
                        wiki + "." +timeBucket + ".tsv.bz2"
                    ).mkString("/"))
                    fs.rename(sourcePath, destinationPath)
                }
            }
        }
    }
}
