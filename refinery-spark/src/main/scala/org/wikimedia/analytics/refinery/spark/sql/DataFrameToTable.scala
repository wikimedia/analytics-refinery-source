package org.wikimedia.analytics.refinery.spark.sql

import com.github.nscala_time.time.Imports.{DateTime, DateTimeFormat}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.wikimedia.analytics.refinery.core.HivePartition
import org.wikimedia.analytics.refinery.tools.LogHelper

// Import implicit StructType and StructField conversions.
// This allows us to use these types with an extended API
// that includes schema merging and Hive DDL statement generation.
import org.wikimedia.analytics.refinery.spark.sql.HiveExtensions._

object DataFrameToTable extends LogHelper {

    /**
     * Insert data into an Iceberg table, deleting first data that falls within a given time range.
     *
     * @param df                 DataFrame The data to insert into the table.
     * @param tableName          String The fully qualified name of the Iceberg table.
     * @param partitionTimeField String The name of the partition time column in the table.
     * @param deleteBefore       DateTime The time before which data should be deleted.
     * @param deleteAfter        DateTime The time after which data should be deleted.
     * @return Long The number of records written to the table.
     */
    def icebergDeleteInsert(
        df: DataFrame,
        tableName: String,
        partitionTimeField: String,
        deleteBefore: DateTime,
        deleteAfter: DateTime,
    ): Long = {
        //  Converting data to destination schema
        val dstSchema = df.sparkSession.table(tableName).schema
        val toWriteDf = df.convertToSchema(dstSchema).cache()

        // First delete data from the output table where meta.dt is within the processed hour
        log.info(s"Deleting existing data from ${tableName} between ${deleteAfter} and ${deleteBefore}.")
        val dtFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")
        df.sparkSession.sql(
            s"""DELETE FROM ${tableName}
                 |WHERE ${partitionTimeField}
                 |  BETWEEN '${dtFormatter.print(deleteAfter)}'
                 |    AND '${dtFormatter.print(deleteBefore)}'""".stripMargin
        )

        // Then insert new data
        log.info(s"Writing new data to ${tableName} between ${deleteAfter} and ${deleteBefore}.")

        toWriteDf.writeTo(tableName).append()

        val recordCount = toWriteDf.count

        // Explicitly un-cache the cached DataFrame.
        toWriteDf.unpersist

        recordCount
    }

    /**
     * Insert and overwrite the DataFrame into a Hive table.
     *
     * Insertion into the correct Hive partitions are handled by the
     * schema definition and the DataFrame writer.
     * Make sure the DataFrame has the correct partition columns and values.
     *
     * @param df DataFrame
     * @param tableName Fully qualified table name, e.g. "db.table"
     * @param outputFilesNumber Number of output files to write. Default: 1
     * @return Number of records written to the Hive partition
     */
    def hiveInsertOverwrite(
        df: DataFrame,
        hivePartition: HivePartition,
        outputFilesNumber: Int
    ): Long = {
        val spark: SparkSession = df.sparkSession

        // Now convert the df so that it matches the schema that the
        // Hive table has. This ensures that columns that are not in df
        // will be set to null, and that columns that might have been placed
        // in the wrong order get re-ordered to match the Hive table schema.

        // Merge the table schema with the df schema.
        // This is the schema that we need df to look like for successful insertion into Hive.
        val compatibleSchema = spark.table(hivePartition.tableName)
            .schema
            .merge(df.schema)

        val outputDf = df
            .convertToSchema(compatibleSchema) // Recursively convert the df to match the Hive compatible schema.
            .repartition(outputFilesNumber) // Fix number of output files
        outputDf.cache() // Cache the DataFrame. We are performing 2 actions on it.

        // We have tried to use
        // spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        // but it loads all partitions into the catalog putting high pressure on the Hive Metastore.
        // We fall back on implementing the delete+insert strategy instead
        // Note: If partition is dynamic, write using partitionBy, if not, write directly to
        // partition path (prevents to group-by partition columns when values are set).
        // Finally add the partition to the Hive table (either directly or through repair table).

        spark.sql(hivePartition.dropPartitionQL)

        if (hivePartition.isDynamic) {
            log.info(
                s"Writing dynamically-partitioned DataFrame to ${hivePartition.location} with schema:\n${outputDf.schema.treeString}"
            )
            outputDf.write.partitionBy(hivePartition.keys:_*).mode(SaveMode.Overwrite).parquet(hivePartition.location.get)
        } else {
            log.info(
                s"Writing DataFrame to ${hivePartition.path} with schema:\n${outputDf.schema.treeString}"
            )
            outputDf.write.mode(SaveMode.Overwrite).parquet(hivePartition.path)
        }

        spark.sql(hivePartition.addPartitionQL)

        val recordCount = outputDf.count()
        log.info(s"Wrote $recordCount rows to ${hivePartition.tableName}")

        outputDf.unpersist // Explicitly un-cache the cached DataFrame.
        recordCount
    }

}