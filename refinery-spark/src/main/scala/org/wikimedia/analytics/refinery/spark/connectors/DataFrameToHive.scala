package org.wikimedia.analytics.refinery.spark.connectors

import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.spark.sql.TableSchemaManager.HiveTableSchemaManager
import org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame
import org.wikimedia.analytics.refinery.tools.LogHelper

// Import implicit StructType and StructField conversions.
// This allows us use these types with an extendend API
// that includes schema merging and Hive DDL statement generation.
import org.wikimedia.analytics.refinery.spark.sql.HiveExtensions._


/**
  * Converts A DataFrame to a partition in a Hive table by 'evolving' a Hive table to
  * add new fields as they are encountered in the DataFrame.
  *
  * Usage:
  *
  *     DataFrameToHive(
  *         sparkSession,
  *         new PartitionedDataFrame(
  *             sparkSession.read.json("/path/to/json/data/mytable/2017/07/01/00"),
  *             HivePartition(
  *                 "mydatabase", "mytable", "/path/to/external/mytable",
  *                 ListMap("year" -> 2017, "month" -> "07", "day" -> "30", "hour" -> "00)
  *             )
  *         )
  *      )
  *
  * If mydb.mytable does not exist in Hive, it will be created based on the DataFrame schema.
  *
  * Later, after more data has been imported, you could run:
  *
  *    DataFrameToHive(
  *         sparkSession,
  *         new PartitionedDataFrame(
  *             sparkSession.read.json("/path/to/json/data/mytable/2017/07/01/01"),
  *             HivePartition(
  *                 "mydatabase", "mytable", "/path/to/external/mytable",
  *                 ListMap("year" -> 2017, "month" -> "07", "day" -> "30", "hour" -> "01")
  *             )
  *         )
  *    )
  *
  * If any new fields are encountered in this new input DataFrame, the now existent
  * mydatabase.mytable table in Hive will be altered to include these fields.
  *
  * If any type changes are encountered between the existent Hive table and the
  * new input data, an IllegalStateException Exception will be thrown.
  *
  */
@deprecated
object DataFrameToHive extends LogHelper {

    type TransformFunction = PartitionedDataFrame => PartitionedDataFrame

    /**
      * Creates or alters table in Hive to match any changes in the DataFrame schema
      * and then inserts the data into the table.
      *
      * @param spark              SparkSession
      *
      * @param inputPartDf        Input PartitionedDataFrame
      *
      * @param doneCallback       Function to call after a successful run
      *
      * @param transformFunctions Functions to do DataFrame transformations.
      *                           You should use these functions to do things like
      *                           geocoding, purging or de-duplicating records in your DataFrame.
      *
      * @return                   The transformed and reordered output DataFrame with Hive partition
      *                           columns that was inserted into the Hive Table.
      */
    def apply(
        spark: SparkSession,
        inputPartDf: PartitionedDataFrame,
        doneCallback: () => Unit = () => Unit,
        transformFunctions: Seq[TransformFunction] = Seq(),
        saveMode: String = "overwrite"
    ): PartitionedDataFrame = {

        // Set this so we can partition by fields in the DataFrame.
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

        // Add the Hive partition columns and apply any other configured transform functions,
        // and then normalize (lowercase top level fields names, widen certain types, etc.).
        val transformedPartDf = transformFunctions
          .foldLeft(inputPartDf.applyPartitions)((currPartDf, fn) => fn(currPartDf))

        val partDf = transformedPartDf.copy(df = transformedPartDf.df
              // Normalize field names (toLower, etc.)
              // and widen types that we can (e.g. Integer -> Long)
              .normalizeAndWiden()
              // Keep number of partitions to reset it after DataFrame API changes it
              // Since the spark context is used across multiple jobs, we don't want
              // to use a global setting.
              .repartitionAs(inputPartDf.df)
        )

        // Grab the partition name keys to use for Hive partitioning.
        val partitionNames = partDf.partition.keys

        try {
            // This will create the Hive table based on df.schema if it doesn't yet exist,
            // or it will alter an existent table adding fields so that it matches inputDf.schema.
            // When comparing and running DDL statements, all fields will be lowercased and
            // made nullable.
            HiveTableSchemaManager.hiveEvolveTable(
                spark,
                partDf.df.schema,
                partDf.partition.tableName,
                partDf.partition.location.get,
                partitionNames
            )
        } catch {
            case e: IllegalStateException =>
                log.fatal(
                    s"""Failed preparing Hive table ${partDf.partition.tableName} with
                       |input schema:\n${partDf.df.schema.treeString}""".stripMargin, e)
                throw e
        }

        // Now convert the df so that it matches the schema that the
        // Hive table has.  This ensures that columns that are not in df
        // will be set to null, and that columns that might have been placed
        // in the wrong order get re-ordered to match the Hive table schema.

        // Merge (and normalize) the table schema with the df schema.
        // This is the schema that we need df to look like for successful insertion into Hive.
        val compatibleSchema = spark.table(partDf.partition.tableName).schema
            .normalizeMerge(partDf.df.schema)

        // Recursively convert the df to match the Hive compatible schema.
        val outputDf = partDf.copy(df = partDf.df.convertToSchema(compatibleSchema))


        if (saveMode == "overwrite") {
            // First drop a previously existing partition in case the
            // partition's schema in the Hive metastore has changed.
            // NOTE: If the Hive table is managed by Hive, i.e. not EXTERNAL,
            // this will actually drop the data.  This should be ok,
            // as we are about to overwrite the partition location anyway.
            spark.sql(outputDf.partition.dropPartitionQL)
        }

        // Avoid using Hive Parquet writer by writing using Spark parquet.
        // This avoids bugs in Hive Parquet like https://issues.apache.org/jira/browse/HIVE-11625
        // If partition is dynamic, write using partitionBy, if not, write directly to
        // partition path (prevents to group-by partition columns when values are set.
        // Finally add the partition to the Hive table (either directly or through repair table).
        if (outputDf.partition.isDynamic) {
            log.info(
                s"Writing dynamically-partitioned DataFrame to ${outputDf.partition.location} with schema:\n${outputDf.df.schema.treeString}"
            )
            outputDf.df.write.partitionBy(outputDf.partition.keys:_*).mode(saveMode).parquet(outputDf.partition.location.get)
        } else {
            log.info(
                s"Writing DataFrame to ${outputDf.partition.path} with schema:\n${outputDf.df.schema.treeString}"
            )
            outputDf.df.write.mode(saveMode).parquet(outputDf.partition.path)
        }

        // Now that data has been written to HDFS, add the Hive partition.
        spark.sql(outputDf.partition.addPartitionQL)

        if (outputDf.partition.isDynamic) {
            log.info(s"Wrote dynamically-partitioned DataFrame to ${outputDf.partition.location} and repaired partitions")
        } else {
            log.info(s"Wrote DataFrame to ${outputDf.partition.path} and added partition ${outputDf.partition}")
        }

        // call doneCallback
        doneCallback()

        outputDf
    }

}
