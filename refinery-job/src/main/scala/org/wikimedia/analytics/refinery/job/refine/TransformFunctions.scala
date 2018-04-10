package org.wikimedia.analytics.refinery.job.refine

/**
  * This file contains objects with apply methods suitable for passing
  * to JSONRefine to do transformations on a DataFrame before
  * inserting into a Hive table.
  *
  * See the JSONRefine --transform-function CLI option documentation.
  */
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{MapType, StringType}
import org.wikimedia.analytics.refinery.core.maxmind.MaxmindDatabaseReaderFactory
import collection.JavaConverters._


/**
  * dropDuplicates from df based on a top level `uuid` field.
  */
object deduplicate_eventlogging extends LogHelper {
    def apply(df: DataFrame, partition: HivePartition): DataFrame = {
        log.debug(s"Dropping duplicates based on `uuid` field in $partition")
        df.dropDuplicates(Seq("uuid"))
    }
}


/**
  * dropDupcliates from df based on the `meta.id` field.
  */
object deduplicate_eventbus extends LogHelper {

    val metaColumnName = "meta.id"
    val tempColumnName = "__meta_id"

    def apply(df: DataFrame, partition: HivePartition): DataFrame = {
        log.debug(s"Dropping duplicates based on `$metaColumnName` field in $partition")
        df.withColumn(tempColumnName, col(metaColumnName))
            .dropDuplicates(Seq(tempColumnName))
            .drop(tempColumnName)
    }
}


/**
  * Geocodes a top level field named `ip` to a `geocoded_data` Map[String, String]
  * using refinery Maxmind geocode classes.
  */
object geocode_ip extends LogHelper {

    val ipColumnName = "ip"
    val geocodedDataColumnName = "geocoded_data"

    def apply(df: DataFrame, partition: HivePartition): DataFrame = {
        // Make sure this df has an ip column
        try {
            df(ipColumnName)
        } catch {
            case e: AnalysisException =>
                log.warn(s"$partition does not contain an `$ipColumnName` field, cannot geocode. Skipping.")
                return df
        }

        log.debug(s"Geocoding `$ipColumnName` into `$geocodedDataColumnName` in $partition")
        // create a new DataFrame
        df.sparkSession.createDataFrame(
            // Map each of our input df to its Spark partitions
            df.rdd.mapPartitions { iter =>
                // Instantiate a Maxmind database reader for this Spark partition
                val geocoder = MaxmindDatabaseReaderFactory.getInstance().getGeocodeDatabaseReader()
                // Map each Row in this partition to a new row that includes the geocoded IP data Map
                iter.map { row: Row =>
                    Row.fromSeq(row.toSeq :+ geocoder.getResponse(row.getAs[String](ipColumnName)).getMap.asScala.toMap)
                }
            },
            // The new DataFrame will be created with the df schema + appeneded geocoded_data Map column.
            df.schema.add(geocodedDataColumnName, MapType(StringType, StringType), nullable=true)
        )
    }
}
