package org.wikimedia.analytics.refinery.job.refine

/**
  * This file contains objects with apply methods suitable for passing
  * to JSONRefine to do transformations on a DataFrame before
  * inserting into a Hive table.
  *
  * After https://gerrit.wikimedia.org/r/#/c/analytics/refinery/source/+/521563/
  * we are merging JSONSchema with Hive schema before we get to these transforms
  * this means that if there are additional columns on Hive that are not on
  * the JSON Schema they will already be part of the dataframe (with null values)
  * when we get to these transform functions.
  *
  * Then, if a transform method is the one that determines the value
  * of this Hive-only column, the transform code needs to drop the column
  * (it holds a null value as it has not been populated with schema values)
  * and re-insert it with the calculated value. See geocode column
  * for an example.
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
import org.wikimedia.analytics.refinery.spark.sql.HiveExtensions._

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

        // If the input DataFrame already has a geocodedDataColumnName column, drop it now.
        // We'll re-add it with newly geocoded data as the same name.
        val workingDf = if (partDf.df.hasColumn(geocodedDataColumnName)) {
            log.debug(s"Input DataFrame already has ${geocodedDataColumnName} column; dropping it before geocoding")
            partDf.df.drop(geocodedDataColumnName)
        } else {
            partDf.df
        }

        log.debug(s"Geocoding `$ipColumnName` into `$geocodedDataColumnName` in ${partDf.partition}")
        // create a new DataFrame
        partDf.copy(df = workingDf.sparkSession.createDataFrame(
            // Map each of our input df to its Spark partitions
            workingDf.rdd.mapPartitions { iter =>
                // Instantiate a Maxmind database reader for this Spark partition
                val geocoder = MaxmindDatabaseReaderFactory.getInstance().getGeocodeDatabaseReader()
                // Map each Row in this partition to a new row that includes the geocoded IP data Map
                iter.map { row: Row =>
                    Row.fromSeq(row.toSeq :+ geocoder.getResponse(row.getAs[String](ipColumnName)).getMap.asScala.toMap)
                }
            },
            // The new DataFrame will be created with the df schema + appeneded geocoded_data Map column.
            workingDf.schema.add(geocodedDataColumnName, MapType(StringType, StringType), nullable=true)
        ))
    }
}

/**
  * Filters out records that have a hostname that is not a wiki
  * except if those records match domains on a whitelist
  *
  * Accepted values include:
  *   wikipedia.org, en.wiktionary.org, ro.m.wikibooks,
  *   zh-an.wikivoyage.org, mediawiki.org, www.wikidata.org, etc.
  *
  * Filtered out values include:
  *   en-wiki.org, en.wikipedia.nom.it, en.wikipedi0.org,
  *   www.translatoruser-int.com, etc.
  *
  * Given that webhost is an optional field on the capsule we need to accept
  * as valid records for which webhost is null.
  *
  */
object eventlogging_filter_is_allowed_hostname extends LogHelper {

    import org.apache.spark.sql.functions.udf;
    val hostnameColumnName = "webhost"

    // TODO
    // if this changes frequently data for whitelist should
    // probably come from hive
    //using scala 'r' regex builder
    var whitelist  = List("translate.google").mkString("|").r;


    val isAllowedHostname = udf((hostname:String) => {
        if (hostname == null || hostname.isEmpty) true
        else if (whitelist.findFirstMatchIn(hostname.toLowerCase()) != None) true
        else if (PageviewDefinition.getInstance.isWikimediaHost(hostname)) true
        else false
    })


    def apply(partDf: PartitionedDataFrame): PartitionedDataFrame = {
        if (partDf.df.columns.contains(hostnameColumnName)) {
            log.debug(s"Filtering for allowed EventLogging hostnames in ${partDf.partition}")
            val schema = partDf.df.schema
            partDf.copy( df=partDf.df.filter(isAllowedHostname(col(hostnameColumnName))))

        } else {
            log.info(s"${partDf.partition} does not have a `$hostnameColumnName` field, skipping allowed EventLogging hostname filtering.")
            partDf
        }
    }
}
