package org.wikimedia.analytics.refinery.core

import org.apache.log4j.LogManager

import scala.util.control.Exception.{allCatch, ignoring}

import org.apache.hadoop.fs.Path


import org.apache.hadoop.hive.metastore.api.AlreadyExistsException

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions.lit

// Import implicit StructType and StructField conversions.
// This allows us use these types with an extendend API
// that includes schema merging and Hive DDL statement generation.
import SparkSQLHiveExtensions._


/**
  * Converts arbitrary JSON to Hive Parquet by 'evolving' the Hive table to
  * add new fields as they are encountered in the JSON data.
  *
  * Usage:
  *
  *     SparkJsonToHive(
  *         hiveContext,
  *         "/path/to/input/data/MyCoolEvent/2017/07/01/00",
  *         HivePartition(
  *             "mydatabase", "mytable", "/path/to/external/mytable",
  *             ListMap("year" -> 2017, "month" -> "07", "day" -> "30", "hour" -> "00)
  *         )
  *      )
  *
  * If mydb.MyCoolEventTable does not exist in Hive, it will be created based on the JSON
  * records and Spark schemas infered by merging all fields from all records found in
  * the inputPath.
  *
  * Later, after more JSON data has been imported, you can run:
  *
  *    SparkJsonToHive(
  *         hiveContext,
  *         "/path/to/input/data/MyCoolEvent/2017/07/01/01",
  *         HivePartition(
  *             "mydatabase", "mytable", "/path/to/external/mytable",
  *             ListMap("year" -> 2017, "month" -> "07", "day" -> "30", "hour" -> "01")
  *         )
  *    )
  *
  * If any new fields are encountered in this new input data, the now existent
  * mydatabase.mytable table in Hive will be altered to include these fields.
  *
  * If any type changes are encountered between the existent Hive table and the
  * new input data, an IllegalStateException Exception will be thrown.
  *
  */
object SparkJsonToHive {
    private val log = LogManager.getLogger("SparkJsonToHive")


    /**
      * Reads inputPath as JSON data, creates or alters tableName in Hive to match the inferred
      * schema of the input JSON data, and then inserts the data into the table.
      *
      * @param hiveContext      Spark HiveContext
      *
      * @param inputPath        Path to JSON data
      *
      *
      * @param partition        HivePartition.  This helper class contains
      *                         database and table name, as well as external location
      *                         and partition keys and values.
      *
      * @param isSequenceFile   If true, inputPath is expected to contain JSON in
      *                         Hadoop Sequence Files, else JSON in text files.
      *
      * @param doneCallback     Function to call after a successful run
      *
      * @return                 The number of records refined
      */
    def apply(
        hiveContext: HiveContext,
        inputPath: String,
        partition: HivePartition,
        isSequenceFile: Boolean,
        doneCallback: () => Unit
    ): Long = {
        // Set this so we can partition by fields in the DataFrame.
        hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

        // Read the JSON data into a DataFrame and
        // add constant value partition columns.
        val inputDf = dataFrameWithHivePartitions(
            readJsonDataFrame(hiveContext.asInstanceOf[SQLContext], inputPath, isSequenceFile),
            partition
        )

        // Grab the partition name keys to use for Hive partitioning.
        val partitionNames = partition.keys

        try {
            // This will create the Hive table based on inputDf.schema if it doesn't yet exist,
            // or it will alter an existent table adding fields so that it matches inputDf.schema.
            // When comparing and running DDL statements, all fields will be lowercased and
            // made nullable.
            prepareHiveTable(
                hiveContext,
                inputDf.schema,
                partition.tableName,
                partition.location,
                partitionNames
            )
        } catch {
            case e: IllegalStateException => {
                log.fatal(
                    s"""Failed preparing Hive table ${partition.tableName} with
                       |input schema:\n${inputDf.schema.treeString}""".stripMargin, e)
                throw e
            }
        }

        // We need to re-read the JSON data with table schema merged with a schema
        // for all fields in the JSON data.  This will allow us to be sure that
        // A. fields we read in from JSON are in the same order as the Hive table,
        // and B. that fields not present in the JSON are set to null.
        //
        // NOTE: It should be possible to use inputDf with fields reordered to match
        // the Hive table schema field ordering exactly, rather than having to
        // re-read the JSON data a second time with the merged schema.  I haven't
        // been able to successfully do this, mainly because I didn't figure out how to use
        // the Spark DataFrame / StructType APIs to fully reorder and add fields.  We'd have
        // to recursively reorder and add missing fields to inputDf.
        // IF we can do this, then it should be possible to abstract away the 'JSON-ness' of
        // This code, and have it work with any DataFrame, since we could pre-load
        // the DataFrame before passing it to this object.  We wouldn't need the inputPath
        // or original data format (JSON) in order to infer the schema.


        val table = hiveContext.table(partition.tableName)

        // normalize=false means that top level fields will not be lowercased, but
        // they will always be made nullable.  We need this because we are about to use
        // this schema to re-read the json data, and here, case matters.  Any fields that
        // are in inputDf must have the same case in the schema we use to read the JSON.
        val nonNormalizedSchema = table.schema.merge(inputDf.schema, normalize=false)

        val mergedSchemaDf = dataFrameWithHivePartitions(
            // re-read the JSON data with the merged non normalized schema.  Any fields in this
            // schema that are not in the data (i.e. they are in the table, but not in this JSON),
            // will automatically be set to null by Spark.
            readJsonDataFrame(
                hiveContext.asInstanceOf[SQLContext],
                inputPath,
                isSequenceFile,
                Option(nonNormalizedSchema)
            ),
            partition
        )
        // mergedSchemaDf will still have non-normalized (e.g. with capital letter) field names,
        // but, that's ok.  We're only inserting into Hive now, and Spark HiveContext will do the
        // lower casing for us.

        log.info(
            s"""Inserting into `${partition.tableName}` DataFrame with schema:\n
               |${mergedSchemaDf.schema.treeString} for partition $partition""".stripMargin
        )
        // Insert data into Hive table.
        // TODO parameterize "overwrite" to allow "append"?
        mergedSchemaDf.write.mode("overwrite")
            .partitionBy(partitionNames:_*)
            .insertInto(partition.tableName)


        // Return the String path of the external partition we just inserted into.
        val partitionPath = hivePartitionPath(hiveContext, partition)
        log.info(
            s"Finished inserting into `${partition.tableName}` DataFrame, " +
            s"wrote to external location $partitionPath."
        )

        // call doneCallback
        doneCallback()

        mergedSchemaDf.count()
    }

    /**
      * Reads a JSON data set out of path.  If isSequenceFile is true, the data should
      * be in Hadoop Sequence file format, instead of text.
      *
      * @param sqlContext       Spark SQLContext
      *
      * @param path             Path to JSON data
      *
      * @param isSequenceFile   If true (default), path is expected to contain Hadoop
      *                         Sequence Files with JSON record strings as values, else
      *                         just JSON text files.
      *
      * @param schema           Optional schema to use for reading data.  If not given,
      *                         Spark's JSON reading logic will infer the schema by passing over
      *                         all data and examining fields in each record.
      *
      * @return
      */
    def readJsonDataFrame(
        sqlContext: SQLContext,
        path: String,
        isSequenceFile: Boolean = true,
        schema: Option[StructType] = None
    ): DataFrame = {
        // If we have a schema, then use it to read JSON data,
        // else just infer schemas while reading.
        val dfReader = if (schema.isDefined) {
            sqlContext.read.schema(schema.get)
        }
        else {
            sqlContext.read
        }

        if (isSequenceFile) {
            // Load DataFrame from JSON data in path
            dfReader.json(
                // Expect data to be SequenceFiles with JSON strings as values.
                sqlContext.sparkContext.sequenceFile[Long, String](path).map(t => t._2)
            )
        }
        else {
            dfReader.json(path)
        }
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
                log.info(s"Adding partition $key=$value")
                // If the partition value looks like an Int, convert it,
                // else just use as a String.  lit() will convert the Scala
                // value (Int or String here) into a Spark Column type.
                currentDf.withColumn(key, lit(allCatch.opt(value.toInt).getOrElse(value)))
        }
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
