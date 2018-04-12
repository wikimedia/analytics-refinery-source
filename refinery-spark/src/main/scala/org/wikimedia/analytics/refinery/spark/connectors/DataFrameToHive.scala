package org.wikimedia.analytics.refinery.spark.connectors

import java.sql.Connection
import java.util.Properties

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.apache.hive.jdbc.HiveDriver
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.wikimedia.analytics.refinery.core.{HivePartition, LogHelper}
// Import implicit StructType and StructField conversions.
// This allows us use these types with an extendend API
// that includes schema merging and Hive DDL statement generation.
import org.wikimedia.analytics.refinery.spark.sql.HiveExtensions._

import scala.util.control.Exception.{allCatch, ignoring}


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
      * @param spark              SparkSession
      *
      * @param hiveServerUrl      URL of the hive server to request to alter tables
      *                           See https://issues.apache.org/jira/browse/SPARK-14130
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
        spark: SparkSession,
        hiveServerUrl: String,
        inputDf: DataFrame,
        partition: HivePartition,
        doneCallback: () => Unit,
        transformFunctions: Seq[(DataFrame, HivePartition) => DataFrame] = Seq()
    ): DataFrame = {

        // Set this so we can partition by fields in the DataFrame.
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

        // Keep number of partitions  to reset it after DataFrame API changes it
        // Since the spark context is used accross multiple jobs, we don't want
        // to use a global setting.
        val originalPartitionNumber = inputDf.rdd.getNumPartitions

        // Add the Hive partition columns and apply any other configured transform functions,
        // and then normalize (lowercase top level fields names, widen certain types, etc.).
        // (Note to non scala-ites: dataFrameWithHivePartitions _ instructs the compiler
        // to implicitly convert  dataFrameWithHivePartitions from a method to a function.)
        val df = (dataFrameWithHivePartitions _ +: transformFunctions)
            .foldLeft(inputDf)((currDf, fn) => fn(currDf, partition))
            .normalize()
            .repartition(originalPartitionNumber)

        // If the resulting DataFrame is empty, just exit.
        try {
            df.head
        } catch {
            case e: java.util.NoSuchElementException =>
                log.info(
                    s"`$partition` DataFrame is empty after transformations. " +
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
                spark,
                hiveServerUrl,
                df.schema,
                partition.tableName,
                partition.location,
                partitionNames
            )
        } catch {
            case e: IllegalStateException =>
                log.fatal(
                    s"""Failed preparing Hive table ${partition.tableName} with
                       |input schema:\n${df.schema.treeString}""".stripMargin, e)
                throw e
        }

        // Now convert the df so that it matches the schema that the
        // Hive table has.  This ensures that columns that are not in df
        // will be set to null, and that columns that might have been placed
        // in the wrong order get re-ordered to match the Hive table schema.

        // Merge (and normalize) the table schema with the df schema.
        // This is the schema that we need df to look like for successful insertion into Hive.
        val compatibleSchema = spark.table(partition.tableName).schema.merge(df.schema)

        // Recursively convert the df to match the Hive compatible schema.
        val outputDf = df.convertToSchema(compatibleSchema)

        log.info(
            s"Writing DataFrame to ${partition.path} with schema:\n${outputDf.schema.treeString}"
        )

        // Avoid using Hive Parquet writer by writing using Spark parquet directly to
        // partition path, and then add the partition to the Hive table.
        // This avoids bugs in Hive Parquet like https://issues.apache.org/jira/browse/HIVE-11625
        outputDf.write.mode("overwrite").parquet(partition.path)
        spark.sql(partition.addPartitionQL)
        log.info(s"Wrote DataFrame to ${partition.path} and added partition $partition")

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
      * @param spark            SparkSession
      *
      * @param hiveServerUrl      URL of the hive server to request to alter tables
      *                           See https://issues.apache.org/jira/browse/SPARK-14130
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
        spark: SparkSession,
        hiveServerUrl: String,
        newSchema: StructType,
        tableName: String,
        locationPath: String = "",
        partitionNames: Seq[String] = Seq.empty
    ): Boolean = {
        val ddlStatements = getDDLStatements(
            spark, newSchema, tableName, locationPath, partitionNames
        )

        // CREATE or ALTER the Hive table if we have a change to make.
        if (ddlStatements.nonEmpty) {

            // Use a hive JDBC connection since Spark doesn't accept column change in SPARK-2
            // https://issues.apache.org/jira/browse/SPARK-14130
            // The connection must use the current user for HDFS file permissions
            try {
                val hiveDriver = new HiveDriver()
                val jdbcUser = System.getProperty("user.name")
                val jdbcUrl = s"jdbc:hive2://$hiveServerUrl/default;user=$jdbcUser;password="
                val connection: Connection = hiveDriver.connect(jdbcUrl, new Properties())
                val statement = connection.createStatement()
                ddlStatements.foreach { (s) =>
                    log.info(s"Running Hive DDL statement:\n$s")
                    ignoring(classOf[AlreadyExistsException]) {
                        statement.execute(s)
                    }
                }
                connection.close()
            } catch {
                case e: Throwable =>
                    log.error("Error executing Hive-DDL commands\n" + e.getMessage)
                    throw e
            }

            // Refresh Spark's metadata about this Hive table.
            spark.catalog.refreshTable(tableName)
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
      * @param spark            SparkSession
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
        spark: SparkSession,
        newSchema: StructType,
        tableName: String,
        locationPath: String = "",
        partitionNames: Seq[String] = Seq.empty
    ): Iterable[String] = {
        // If the Hive table doesn't exist, get CREATE DDL to create it like newSchema.
        if (!hiveTableExists(spark, tableName)) {
            Seq(newSchema.hiveCreateDDL(tableName, locationPath, partitionNames))
        }
        // Else get ALTER DDL statements to alter the existing table to add fields from new Schema.
        else {
            val tableSchema = spark.table(tableName).schema
            val alterDDLs = tableSchema.hiveAlterDDL(tableName, newSchema)
            if (alterDDLs.nonEmpty) {
                log.info(s"""Found difference in schemas for Hive table $tableName
                            |Table schema:\n${tableSchema.treeString}
                            |Input schema:\n${newSchema.treeString}""".stripMargin
                )
            }

            alterDDLs
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


    /**
      * Returns true if the fully qualified tableName exists in Hive, else false.
      *
      * @param spark       SparkSession
      *
      * @param tableName   Fully qualified Hive table name
      *
      * @return
      */
    def hiveTableExists(spark: SparkSession, tableName: String): Boolean = {
        allCatch.opt(spark.table(tableName)) match {
            case Some(_) => true
            case _       => false
        }
    }

}
