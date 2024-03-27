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
    * @param df DataFrame The data to insert into the table.
    * @param tableName String The fully qualified name of the Iceberg table.
    * @param partitionTimeField String The name of the partition time column in the table.
    * @param deleteBefore DateTime The time before which data should be deleted.
    * @param deleteAfter DateTime The time after which data should be deleted.
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
               |    AND '${dtFormatter.print(deleteBefore)}'""".stripMargin)

        // Then insert new data
        log.info(s"Writing new data to ${tableName} between ${deleteAfter} and ${deleteBefore}.")

        toWriteDf.writeTo(tableName).append()

        val recordCount = toWriteDf.count

        // Explicitly un-cache the cached DataFrame.
        toWriteDf.unpersist

        recordCount
    }

    /**
     * Create or Overwrite a partition in a Hive table with the data in a DataFrame.
     * @param df DataFrame
     * @param fullyQualifiedTableName e.g. "db.table"
     * @param hivePartitionPath e.g. "datacenter=eqiad/year=2024/month=2/day=15/hour=0"
     * @return Number of records written to the Hive partition
     */
    def hiveInsertOverwritePartition(
        df: DataFrame,
        fullyQualifiedTableName: String,
        hivePartitionPath: String
    ): Long = {
        val spark: SparkSession = df.sparkSession
        val fullyQualifiedTableNameParts: Array[String] = fullyQualifiedTableName.split("\\.")
        val partitionedDataFrame = PartitionedDataFrame(
            df,
            HivePartition(
                fullyQualifiedTableNameParts(0),  // database
                fullyQualifiedTableNameParts(1),  // table
                "",  // baseLocation (not used here)
                hivePartitionPath  // partitionPath
            )
        ).applyPartitions  // Add partitions columns to the DataFrame with applyPartitions

        // Now convert the df so that it matches the schema that the
        // Hive table has. This ensures that columns that are not in df
        // will be set to null, and that columns that might have been placed
        // in the wrong order get re-ordered to match the Hive table schema.

        // Merge the table schema with the df schema.
        // This is the schema that we need df to look like for successful insertion into Hive.
        val compatibleSchema = spark.table(partitionedDataFrame.partition.tableName)
            .schema
            .merge(partitionedDataFrame.df.schema)

        val outputDf = partitionedDataFrame.df
            .convertToSchema(compatibleSchema)  // Recursively convert the df to match the Hive compatible schema.
            .repartition(partitionNumber(spark))  // Fix number of output files
        outputDf.cache()  // Cache the DataFrame. We are performing 2 actions on it.

        // The following configuration is necessary to allow dynamic partition overwrite.
        // In other word, overwriting the partition defined in the DataFrame.
        spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

        log.info(s"Writing DataFrame to $fullyQualifiedTableName with schema:\n${outputDf.schema.treeString}")

        outputDf.write
            .mode(SaveMode.Overwrite)
            .insertInto(fullyQualifiedTableName)

        log.info(s"Wrote DataFrame to $fullyQualifiedTableName with partition $hivePartitionPath")

        val recordCount = outputDf.count()
        outputDf.unpersist // Explicitly un-cache the cached DataFrame.
        recordCount
    }

    /**
     * Returns the number of partitions to use when writing a DataFrame to disk.
     * If running in yarn mode, this will return the number of executors cross the number of cores per executor.
     * If running in standalone mode, this will return 1.
     * Those parameters match the statistically determined size of the dataset.
     * @param spark SparkSession
     * @return Number of partitions to use when writing a DataFrame to disk.
     */
    def partitionNumber(spark: SparkSession): Int = {
        if (spark.conf.get("spark.master", "local") == "yarn") {
            val executorCores: Int = spark.conf.get("spark.executor.cores", "1").toInt
            val maxNumberOfExecutors: Int = spark.conf.get("spark.dynamicAllocation.maxExecutors", "1").toInt
            executorCores * maxNumberOfExecutors
        } else 1
    }
}
