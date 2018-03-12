package org.wikimedia.analytics.refinery.job.refine

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType
import org.wikimedia.analytics.refinery.core.HivePartition

import scala.util.control.Exception.{allCatch, ignoring}

import SparkSQLHiveExtensions._

// Import implicit StructType and StructField conversions.
// This allows us use these types with an extendend API
// that includes schema merging and Hive DDL statement generation.

/**
  * Converts A DataFrame to a partition in a Hive table by 'evolving' a Hive table to
  * add new fields as they are encountered in the DataFrame.
  *
  * Usage:
  *
  *     DataFrameToHive(
  *         hiveContext,
  *         hiveContext.read.json("/path/to/json/data/mytable/2017/07/01/00"),
  *         HivePartition(
  *             "mydatabase", "mytable", "/path/to/external/mytable",
  *             ListMap("year" -> 2017, "month" -> "07", "day" -> "30", "hour" -> "00)
  *         )
  *      )
  *
  * If mydb.mytable does not exist in Hive, it will be created based on the DataFrame schema.
  *
  * Later, after more data has been imported, you could run:
  *
  *    DataFrameToHive(
  *         hiveContext,
  *         hiveContext.read.json("/path/to/json/data/mytable/2017/07/01/01"),
  *         HivePartition(
  *             "mydatabase", "mytable", "/path/to/external/mytable",
  *             ListMap("year" -> 2017, "month" -> "07", "day" -> "30", "hour" -> "01")
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
object DataFrameToHive extends LogHelper {
    /**
      * Creates or alters tableName in Hive to match any changes in the DataFrame schema
      * and then inserts the data into the table.
      *
      * @param hiveContext        Spark HiveContext
      *
      * @param inputDf            Input DataFrame
      *
      *
      * @param partition          HivePartition.  This helper class contains
      *                           database and table name, as well as external location
      *                           and partition keys and values.
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
        hiveContext: HiveContext,
        inputDf: DataFrame,
        partition: HivePartition,
        doneCallback: () => Unit,
        transformFunctions: Seq[(DataFrame, HivePartition) => DataFrame] = Seq()
    ): DataFrame = {

        // Set this so we can partition by fields in the DataFrame.
        hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

        // Add the Hive partition columns and apply any other configured transform functions,
        // and then normalize (lowercase top level fields names, widen certain types, etc.).
        // (Note to non scala-ites: dataFrameWithHivePartitions _ instructs the compiler
        // to implicitly convert  dataFrameWithHivePartitions from a method to a function.)
        val df = (dataFrameWithHivePartitions _ +: transformFunctions)
            .foldLeft(inputDf)((currDf, fn) => fn(currDf, partition))
            .normalize()

        // If the resulting DataFrame is empty, just exit.
        try {
            df.head
        } catch {
            case e: java.util.NoSuchElementException =>
                log.info(
                    s"`${partition}` DataFrame is empty after transformations. " +
                    "No data will be written for this partition."
                )
                doneCallback()
                return df
        }

        // Grab the partition name keys to use for Hive partitioning.
        val partitionNames = partition.keys


        try {
            // This will create the Hive table based on df.schema if it doesn't yet exist,
            // or it will alter an existent table adding fields so that it matches inputDf.schema.
            // When comparing and running DDL statements, all fields will be lowercased and
            // made nullable.
            prepareHiveTable(
                hiveContext,
                df.schema,
                partition.tableName,
                partition.location,
                partitionNames
            )
        } catch {
            case e: IllegalStateException => {
                log.fatal(
                    s"""Failed preparing Hive table ${partition.tableName} with
                       |input schema:\n${df.schema.treeString}""".stripMargin, e)
                throw e
            }
        }

        // Now convert the df so that it matches the schema that the
        // Hive table has.  This ensures that columns that are not in df
        // will be set to null, and that columns that might have been placed
        // in the wrong order get re-ordered to match the Hive table schema.

        // Merge (and normalize) the table schema with the df schema.
        // This is the schema that we need df to look like for successful insertion into Hive.
        val compatibleSchema = hiveContext.table(partition.tableName).schema.merge(df.schema)

        // Recursively convert the df to match the Hive compatible schema.
        val outputDf = df.convertToSchema(compatibleSchema)

        log.info(
            s"""Inserting into `${partition.tableName}` DataFrame with schema:\n
               |${outputDf.schema.treeString} for partition $partition""".stripMargin
        )

        // Insert data into Hive table.
        outputDf.write.mode("overwrite")
            .partitionBy(partitionNames:_*)
            .insertInto(partition.tableName)

        log.info(
            s"Finished inserting into `${partition.tableName}` DataFrame, " +
            s"wrote to external location ${hivePartitionPath(hiveContext, partition)}."
        )

        // call doneCallback
        doneCallback()

        outputDf
    }


    /**
      * If tableName does not exist in Hive, this will create it with the schema newSchema.
      * Else, it will attempt to alter the table to add any fields in newSchema that are
      * not already in the Hive table.  If any fields change types, this will throw an
      * exception.
      *
      * @param hiveContext      Spark HiveContext
      *
      * @param newSchema        Spark schema representing the schema of the
      *                         table to be created or altered.
      *
      * @param tableName        Fully qualified (dotted) Hive table name.
      *
      * @param locationPath     Path to external table data.
      *
      * @param partitionNames   List of partition names.  These must be present as
      *                         fields in the newSchema and/or Hive table.
      *
      * @return
      */
    def prepareHiveTable(
        hiveContext: HiveContext,
        newSchema: StructType,
        tableName: String,
        locationPath: String = "",
        partitionNames: Seq[String] = Seq.empty
    ): Boolean = {
        val ddlStatements = getDDLStatements(
            hiveContext, newSchema, tableName, locationPath, partitionNames
        )

        // CREATE or ALTER the Hive table if we have a change to make.
        if (ddlStatements.nonEmpty) {
            ddlStatements.foreach { (s) =>
                log.info(s"Running Hive DDL statement:\n$s")
                ignoring(classOf[AlreadyExistsException]) {
                    hiveContext.sql(s)
                }
            }
            // Refresh Spark's metadata about this Hive table.
            hiveContext.refreshTable(tableName)
            true
        }
        else
            false
    }


    /**
      * If tableName does not exist in Hive, this will return a single element Seq
      * with a Hive CREATE TABLE statement to create a table that represents newSchema.
      * Else, if tableName does exist, it will return a Seq of Hive ALTER TABLE statements
      * required to update the table schema to match any new fields found in newSchema.
      *
      * @param hiveContext      Spark HiveContext
      *
      * @param newSchema        Spark schema representing the schema of the
      *                         table to be created or altered.
      *
      * @param tableName        Fully qualified (dotted) Hive table name.
      *
      * @param locationPath     Path to external table data.
      *
      * @param partitionNames   List of partition names.  These must be present as
      *                         fields in the newSchema and/or Hive table.
      *
      * @return
      */
    def getDDLStatements(
        hiveContext: HiveContext,
        newSchema: StructType,
        tableName: String,
        locationPath: String = "",
        partitionNames: Seq[String] = Seq.empty
    ): Iterable[String] = {
        // If the Hive table doesn't exist, get CREATE DDL to create it like newSchema.
        if (!hiveTableExists(hiveContext, tableName)) {
            Seq(newSchema.hiveCreateDDL(tableName, locationPath, partitionNames))
        }
        // Else get ALTER DDL statements to alter the existing table to add fields from new Schema.
        else {
            val tableSchema = hiveContext.table(tableName).schema
            val alterDDLs = tableSchema.hiveAlterDDL(tableName, newSchema)
            if (alterDDLs.nonEmpty) {
                log.info(s"""Found difference in schemas for Hive table `$tableName`
                            |Table schema:\n${tableSchema.treeString}
                            |Input schema:\n${newSchema.treeString}""".stripMargin
                )
            }

            alterDDLs
        }
    }


    /**
      * Returns true if the fully qualified tableName exists in Hive, else false.
      *
      * @param hiveContext Spark HiveContext
      *
      * @param tableName   Fully qualified Hive table name
      *
      * @return
      */
    def hiveTableExists(hiveContext: HiveContext, tableName: String): Boolean = {
        allCatch.opt(hiveContext.table(tableName)) match {
            case Some(_) => true
            case _       => false
        }
    }


    /**
      * Returns a new DataFrame with constant Hive partitions added as columns.  If any
      * column values are convertable to Ints, they will be added as an Int, otherwise String.
      *
      * @param df           Input DataFrame
      *
      * @param partition    HivePartition
      *
      * @return
      */
    def dataFrameWithHivePartitions(
        df: DataFrame,
        partition: HivePartition
    ): DataFrame = {
        // Add partitions to DataFrame.
        partition.keys.foldLeft(df) {
            case (currentDf, (key: String)) =>
                val value = partition.get(key).get
                // If the partition value looks like an Int, convert it,
                // else just use as a String.  lit() will convert the Scala
                // value (Int or String here) into a Spark Column type.
                currentDf.withColumn(key, lit(allCatch.opt(value.toLong).getOrElse(value)))
        }
    }

    def normalizeDataFrame(df: DataFrame, lowerCaseTopLevel: Boolean = false): DataFrame = {
        df.sqlContext.createDataFrame(df.rdd, df.schema.normalize(lowerCaseTopLevel))
    }


    /**
      * Runs a DESCRIBE FORMATTED query and extracts path to a
      * Hive table partition.
      *
      * @param hiveContext  HiveContext
      * @param partition    HivePartition
      * @return
      */
    def hivePartitionPath(
        hiveContext: HiveContext,
        partition: HivePartition
    ): String = {
        var q = s"DESCRIBE FORMATTED `${partition.tableName}`"
        if (partition.nonEmpty) {
            q += s" PARTITION (${partition.hiveQL})"
        }

        // This query will return a human readable block of text about
        // this table (partition).  We need to parse out the Location information.
        val locationLine: Option[String] = hiveContext.sql(q)
            .collect()
            // Each line is a Row[String], map it to Seq of Strings
            .map(_.toString())
            // Find the line with "Location"
            .find(_.contains("Location"))

        // If we found Location in the string, then extract just the location path.
        if (locationLine.isDefined) {
            locationLine.get.filterNot("[]".toSet).trim.split("\\s+").last
        }
        // Else throw an Exception.  This shouldn't happen, as the Hive query
        // will fail earlier if if the partition doesn't exist.
        else {
            throw new RuntimeException(s"Failed finding path of Hive table partition $partition")
        }
    }

}
