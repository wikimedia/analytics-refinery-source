package org.wikimedia.analytics.refinery.job

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.log
import org.apache.spark.sql.{Column, SparkSession}
import org.wikimedia.analytics.refinery.core.{LogHelper, HivePartition}
import org.wikimedia.analytics.refinery.core.config._
import org.wikimedia.analytics.refinery.spark.connectors.DataFrameToHive
import org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame
import org.apache.spark.sql.functions.{udf, coalesce, lit}

import scala.collection.immutable.ListMap

/**
  * Split a webrequest-hour into subsets based on webrequest-tags through dynamic-partitioning
  *
  * Example launch command (using joal settings):
  *
  * spark2-submit \
  *      --master yarn \
  *      --executor-memory 8G \
  *      --driver-memory 4G \
  *      --executor-cores 2 \
  *      --conf spark.dynamicAllocation.maxExecutors=128 \
  *      --files /etc/hive/conf/hive-site.xml \
  *      --conf spark.driver.extraClassPath=/usr/lib/hive/lib/hive-jdbc.jar:/usr/lib/hadoop-mapreduce/hadoop-mapreduce-client-common.jar:/usr/lib/hive/lib/hive-service.jar \
  *      --class org.wikimedia.analytics.refinery.job.WebrequestSubsetPartitioner \
  *      /home/joal/code/refinery-source/refinery-job/target/refinery-job-0.0.79-SNAPSHOT.jar \
  *      --year  2018 \
  *      --month 10 \
  *      --day 19 \
  *      --hour 0 \
  *      --webrequest_subset_table joal.webrequest_subset \
  *      --webrequest_subset_tags_table joal.webrequest_subset_tags \
  *      --webrequest_subset_base_path hdfs:///user/joal/wmf/data/wmf/webrequest_subset
  *
  * The joinWebrequestSubsetTags transform-function is used through
  * [[org.wikimedia.analytics.refinery.spark.connectors.DataFrameToHive]]
  * over the wmf.webrequest data. Its purpose is to split the big webrequest
  * dataset into smaller portions of it, called webrequest_subset. Those smaller
  * subsets should then be used instead of requesting the full dataset, when possible.
  *
  * For instance, we want to be able to query the pageviews subset of webrequest,
  * as well as the wikidata and wdqs subsets. The pageview rows are identified by having
  * a "pageview" tag in their tag-list. Similarly "wikidata" and "wikidata-query" tags in
  * webrequest rows define "wikidata" and "wdqs" subsets. The link from tags to subsets
  * is made through a webrequest_subset_tags, which defines arrays of tags to be present
  * in webrequest and their associated subsets.
  *
  * Also in order to split the webrequest data, dynamic-partitioning is used. In addition
  * to the already existing and defined date partitions, subset values are used splits the
  * dataset as it is writen, in hive-partition format, for instance
  * `base_path/year=2018/month=10/day=18/hour=17/subset=pageview`,
  * `base_path/year=2018/month=10/day=18/hour=17/subset=wikidata`,
  * etc...
  *
  * WARNING: Each subset can contain rows also present in other subsets - You should therefore
  *          NOT query webrequest_subset for more than a single subset, as this could lead to
  *          overcounting some rows.
  *
  * Bonus: Since the webrequest_subset_tags can contain multiple rows for the same subset,
  *        we are given for free the ability to have multiple tags-arrays grouped in the
  *        same subset -- We do not yet have a real use-case for that.
  *        This is equivalent to a OR in logical statement, while tags being present in
  *        the same array is equivalent to a AND: to be in the subset for tags
  *        "pageviews OR wikidata-query" can be expressed in two rows in the
  *        webrequest_subset_tags table, having both the same same subset, and one
  *        referencing the "pageview tag, the other the "wikidata-query" one.
  */

object WebrequestSubsetPartitioner extends ConfigHelper {

    type TransformFunction = DataFrameToHive.TransformFunction

    @transient
    lazy val log: Logger = Logger.getLogger(this.getClass)

    val dbTablePattern = "(.+)\\.(.+)".r

    /**
     * Generic method returning true if the first given sequence s1 is a subset of
     * the second sequence s2.
     * This allows to check if a webrequest row contains AT LEAST a subset-tags list:
     * isSubset(subsetTags, webrequestTags) == true
     *
     * Note: Sequences are treated as sets - Duplicate elements are considered single
     */
    def isSubset[T](s1: Seq[T], s2: Seq[T]): Boolean = {
        s1.toSet.subsetOf(s2.toSet)
    }

    /**
      * Case class storing arguments parameters, used with [[ConfigHelper]].
      */
    case class Params(
        year: Int,
        month: Int,
        day: Int,
        hour: Int,
        webrequest_table: String = "wmf.webrequest",
        webrequest_subset_table: String = "wmf.webrequest_subset",
        webrequest_subset_tags_table: String = "wmf.webrequest_subset_tags",
        webrequest_subset_base_path: String = "/wmf/data/wmf/webrequest_subset",
        hive_server_url: String = "an-coord1001.eqiad.wmnet:10000"
    )

    object Params {
        // This is just used to ease generating help message with default values.
        // Required parameters are set to dummy values.
        val default = Params(0, 0, 0, 0)

        val propertiesDoc: ListMap[String, String] = ListMap(
            "config_file <file1.properties,files2.properties>" ->
                """Comma separated list of paths to properties files.  These files parsed for
                  |for matching config parameters as defined here.""",
            "year" ->
                """The year of the webrequest partition to process""",
            "month" ->
                """The month of the webrequest partition to process""",
            "day" ->
                s"The day of the webrequest partition to process",
            "hour" ->
                s"""The hour of the webrequest partition to process""",
            "webrequest_table" ->
                s"""The fully qualified webrequest table to get data from.
                   |Default: ${default.webrequest_table}""",
            "webrequest_subset_table" ->
                s"""The fully qualified webrequest_subset table where to store partitioned data.
                    |Default: ${default.webrequest_subset_table}""",
            "webrequest_subset_tags_table" ->
                s"""The fully qualified webrequest_subset_tags table to get subset-to-tags relations.
                    |Default: ${default.webrequest_subset_tags_table}""",
            "webrequest_subset_base_path" ->
                s"""The base path of the webrequest_subset table, to write partitioned data.
                    |Default: ${default.webrequest_subset_base_path}"""
        )

        val usage: String =
            """
              |webrequest -> webrequest_subset
              |
              |Partitions a webrequest hour into smaller subsets based on tags
              |
              |Example:
              |  spark2-submit --class org.wikimedia.analytics.refinery.job.WebrequestSubsetPartitioner refinery-job.jar \
              |  # read configs out of this file
              |   --config_file                 /etc/refinery/partition_webrequest_subsets.properties \
              |   # Override and/or set other configs on the CLI
              |   --year  2018 \
              |   --month 10 \
              |   --day   18 \
              |   --hour  17
              |"""
    }


    def main(args: Array[String]) {
        // Argument parsing
        if (args.contains("--help")) {
            println(help(Params.usage, Params.propertiesDoc))
            sys.exit(0)
        }
        val params = try {
            configureArgs[Params](args)
        } catch {
            case e: ConfigHelperException =>
                log.fatal(e.getMessage + ". Aborting.")
                sys.exit(1)
        }
        log.info("Loaded configuration:\n" + prettyPrint(params))

        // Initial setup - Spark, SQLContext
        val conf = new SparkConf().setAppName("WebrequestSubsetPartition")
        val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

        // Get the webreques data to split
        val webrequestDataframe = spark.sql(
            s"""
               |SELECT *
               |FROM ${params.webrequest_table}
               |-- Get any webrequest_source in order to build complete subsets
               |WHERE year = ${params.year}
               |  AND month = ${params.month}
               |  AND day = ${params.day}
               |  AND hour = ${params.hour}
             """.stripMargin)

        // Prepare the partition (database, table, base-path, time-partition-values)
        val (subset_db, subset_table) = params.webrequest_subset_table match {
            case dbTablePattern(db, table) => (db, table)
            case _ =>
                log.error("webrequest_subset table name should be in DB.TABLE format")
                throw new RuntimeException()
        }
        val timePartition = {
            new ListMap[String, Option[String]]() +
                ("year" -> Some(f"${params.year}%04d")) +
                ("month" -> Some(f"${params.month}%02d")) +
                ("day" -> Some(f"${params.day}%02d")) +
                ("hour" -> Some(f"${params.hour}%02d"))
        }
        val webrequestSubsetPartition = new HivePartition(
            subset_db,
            subset_table,
            params.webrequest_subset_base_path,
            timePartition
        )

        // Build the PartitionedDataframe to process
        // It is a bit counter-intuitive, but this object needs to contain the
        // not-yet-processed data in dataframe, and the to-be-written partition information
        val webrequestToSplit = new PartitionedDataFrame(df = webrequestDataframe, partition =  webrequestSubsetPartition)

        // Preparing the transform function sequence with the joinWebrequestSubsetTags function
        // initialized to to used params.webrequest_subset_tags_table
        val transformFunctions = Seq[TransformFunction](joinWebrequestSubsetTags(params.webrequest_subset_tags_table))

        // Actually split the data and update the hive table
        DataFrameToHive(
            spark,
            params.hive_server_url,
            webrequestToSplit,
            () => log.info(s"Done splitting webrequest into subsets for $webrequestSubsetPartition"),
            transformFunctions
        )
    }

    /**
     * This function get the subset-tags from webrequestSubsetTagsTable parameter, and joins
     * them with the dataframe parameter (supposedly webrequest) using the [[isSubset]]
     * function defined above as a UDF. It cleans the dataframe from columns that were only
     * for the join, and updates the partitions for the dataframe, adding "subset" as a dynamic
     * partition (with no defined value). It finally returns a new PartitionDataframe, built from
     * the joint-dataframe and having the newly defined partitions.
     */
    def joinWebrequestSubsetTags(webrequestSubsetTagsTable: String)(partDf: PartitionedDataFrame): PartitionedDataFrame = {
        log.debug(s"Joining webrequest with subset_tags  and adding subset as partition in ${partDf.partition}")

        val spark = partDf.df.sparkSession

        // Instantiate isSubset for String type, and make it a val to facilitate UDF creation
        val isSubsetString = (a1: Seq[String], a2: Seq[String]) => isSubset[String](a1, a2)
        val isSubsetUdf = udf(isSubsetString)

        val webrequestSubsetTagsSql = s"""
            SELECT
              webrequest_tags,
              -- Alias to get correct name for webrequest directly
              webrequest_subset AS subset
            FROM $webrequestSubsetTagsTable
            """

        // Get subsetTags using SQL query. Dataframe contains (webrequest_tags, subset)
        val subsetTagsDf = spark.sql(webrequestSubsetTagsSql)

        // Join if webrequest_subset_tags.webrequest_tags is a subset of webrequest.tags,
        // coalescing webrequest_tags to empty array to prevent null errors.
        val joinExpr: Column = isSubsetUdf(
            col("webrequest_tags"),
            coalesce(col("tags"), lit(Array.empty[String]))
        )

        // Join webrequest with webrequest_subset_tags then Drop not needed field
        val jointDf = partDf.df.join(subsetTagsDf, joinExpr).drop("webrequest_tags")

        // Update partitions adding "subset" dynamic level
        val newPartitions = partDf.partition.partitions + ("subset" -> None)

        // Return updated PartitionedDataframe
        partDf.copy(df = jointDf, partition = partDf.partition.copy(partitions = newPartitions))
    }

}
