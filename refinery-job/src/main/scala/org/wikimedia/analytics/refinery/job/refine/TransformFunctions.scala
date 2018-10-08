package org.wikimedia.analytics.refinery.job.refine

/**
  * This file contains objects with apply methods suitable for passing
  * to JSONRefine to do transformations on a DataFrame before
  * inserting into a Hive table.
  *
  * See the JSONRefine --transform-function CLI option documentation.
  */
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{MapType, StringType}
import org.apache.spark.sql.{Column, AnalysisException, Row}
import org.wikimedia.analytics.refinery.core.LogHelper
import org.wikimedia.analytics.refinery.core.maxmind.MaxmindDatabaseReaderFactory
import org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame

import scala.collection.JavaConverters._


/**
  * dropDuplicates from df based on a top level `uuid` field.
  */
object deduplicate_eventlogging extends LogHelper {
    def apply(partDf: PartitionedDataFrame): PartitionedDataFrame = {
        log.debug(s"Dropping duplicates based on `uuid` field in ${partDf.partition}")
        partDf.copy(df = partDf.df.dropDuplicates(Seq("uuid")))
    }
}


/**
  * dropDupcliates from df based on the `meta.id` field.
  */
object deduplicate_eventbus extends LogHelper {

    val metaColumnName = "meta.id"
    val tempColumnName = "__meta_id"

    def apply(partDf: PartitionedDataFrame): PartitionedDataFrame = {
        log.debug(s"Dropping duplicates based on `$metaColumnName` field in ${partDf.partition}")
        partDf.copy(df = partDf.df.withColumn(tempColumnName, col(metaColumnName))
            .dropDuplicates(Seq(tempColumnName))
            .drop(tempColumnName))
    }
}


/**
  * Geocodes a top level field named `ip` to a `geocoded_data` Map[String, String]
  * using refinery Maxmind geocode classes.
  */
object geocode_ip extends LogHelper {

    val ipColumnName = "ip"
    val geocodedDataColumnName = "geocoded_data"

    def apply(partDf: PartitionedDataFrame): PartitionedDataFrame = {
        // Make sure this df has an ip column
        try {
            partDf.df(ipColumnName)
        } catch {
            case e: AnalysisException =>
                log.warn(s"${partDf.partition} does not contain an `$ipColumnName` field, cannot geocode. Skipping.")
                return partDf
        }

        log.debug(s"Geocoding `$ipColumnName` into `$geocodedDataColumnName` in ${partDf.partition}")
        // create a new DataFrame
        partDf.copy(df = partDf.df.sparkSession.createDataFrame(
            // Map each of our input df to its Spark partitions
            partDf.df.rdd.mapPartitions { iter =>
                // Instantiate a Maxmind database reader for this Spark partition
                val geocoder = MaxmindDatabaseReaderFactory.getInstance().getGeocodeDatabaseReader()
                // Map each Row in this partition to a new row that includes the geocoded IP data Map
                iter.map { row: Row =>
                    Row.fromSeq(row.toSeq :+ geocoder.getResponse(row.getAs[String](ipColumnName)).getMap.asScala.toMap)
                }
            },
            // The new DataFrame will be created with the df schema + appeneded geocoded_data Map column.
            partDf.df.schema.add(geocodedDataColumnName, MapType(StringType, StringType), nullable=true)
        ))
    }
}

/**
  * This transform-function is to be used through
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
object partition_webrequest_subset extends LogHelper {

    // Making this a var to overcome global name issue
    var webrequestSubsetTagsTable: String = "wmf.webrequest_subset_tags"

    import org.apache.spark.sql.functions.{udf, coalesce, lit}

    /**
     * SQL query getting the tags-to-subset values
     */
    lazy val webrequestSubsetTagsSql = s"""
                                  SELECT
                                      webrequest_tags,
                                      -- Alias to get correct name for webrequest directly
                                      webrequest_subset AS subset
                                  FROM $webrequestSubsetTagsTable
                                  """

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
     * This function get the subset-tags from the [[webrequestSubsetTagsSql]], and joins
     * them with the given parameter dataframe (supposedly webrequest) using the [[isSubset]]
     * function defined above as a UDF. It cleans the dataframe from columns that were only
     * for the join, and updates the partitions for the dataframe, adding "subset" as a dynamic
     * partition (with no defined value). It finally returns a new PartitionDataframe, built from
     * the joint-dataframe and having the newly defined partitions.
     */
    def apply(partDf: PartitionedDataFrame): PartitionedDataFrame = {
        log.debug(s"Joining webrequest with subset_tags  and adding subset as partition in ${partDf.partition}")

        val spark = partDf.df.sparkSession

        // Instantiate isSubset for String type, and make it a val to facilitate UDF creation
        val isSubsetString = (a1: Seq[String], a2: Seq[String]) => isSubset[String](a1, a2)
        val isSubsetUdf = udf(isSubsetString)

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


