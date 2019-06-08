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
import org.wikimedia.analytics.refinery.core.PageviewDefinition
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
  * Filters out records that have a hostname that is not a wiki.
  * Accepted values include:
  *   wikipedia.org, en.wiktionary.org, ro.m.wikibooks,
  *   zh-an.wikivoyage.org, mediawiki.org, www.wikidata.org, etc.
  * Filtered out values include:
  *   en-wiki.org, en.wikipedia.nom.it, en.wikipedi0.org,
  *   translate.googleusercontent.com, www.translatoruser-int.com, etc.
 *
  * Given that webhost is an optional field on the capsule we need to accept
  * as valid records for which webhost is null.
  */
object filter_out_non_wiki_hostname extends LogHelper {

    // The hostname should be in a field named 'webHost'.
    val hostnameColumnName = "webHost"

    def apply(partDf: PartitionedDataFrame): PartitionedDataFrame = {
        if (partDf.df.columns.contains(hostnameColumnName)) {
            log.debug(s"Filtering out non-wiki hostnames in ${partDf.partition}")
            val schema = partDf.df.schema
            partDf.copy(df = partDf.df.sparkSession.createDataFrame(
                partDf.df.rdd.filter { row =>
                    val hostname = row.getAs[String](hostnameColumnName)
                    hostname == null || PageviewDefinition.getInstance.isWikimediaHost(hostname)
                },
                schema
            ))
        } else {
            log.info(s"${partDf.partition} does not have a `$hostnameColumnName` field, skipping non-wiki filtering.")
            partDf
        }
    }
}
