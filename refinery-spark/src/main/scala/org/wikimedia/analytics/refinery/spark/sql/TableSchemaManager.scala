package org.wikimedia.analytics.refinery.spark.sql

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.net.URI
import java.sql.Connection
import java.util.Properties
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.apache.hive.jdbc.HiveDriver

import scala.util.control.Exception.{allCatch, ignoring}


/**
 *
 * Generate DDLs statements to create or alter an Hive or an Iceberg table by first applying transform functions to a
 * Dataframe, and then creating or updating the table schema to add new fields as they are encountered in the DataFrame.
 *
 * If table does not exist in the current catalog (probably backed by the Hive-metastore in our case), it will be
 * created based on the schema.
 *
 * If any new fields are encountered in this new input DataFrame, the now existent table in Hive will be altered:
 *   - schema
 *   - table properties
 *
 * If any type changes are encountered between the existent Hive table and the new input data, an IllegalStateException
 * Exception will be thrown.
 */
object TableSchemaManager {

    object HiveTableSchemaManager {
        import org.wikimedia.analytics.refinery.spark.sql.HiveExtensions._

        /**
         * Generates Hive DDL statements for creating or altering a table based on a new schema.
         *
         * This function prepares DDL statements for Hive tables based on the provided new schema. If the table does not
         * exist, a CREATE TABLE statement is generated to create a new table matching the new schema. If the table
         * already exists, ALTER TABLE statements are prepared to update the table schema to include any new fields
         * found in the new schema. The function also handles time-based partitions by adding missing time partition
         * columns to the new schema before generating DDL statements.
         *
         * @param spark The SparkSession instance used for accessing Hive.
         * @param newSchema The new schema represented as a StructType, which the Hive table should conform to.
         * @param table The fully qualified Hive table name (e.g., "database.table").
         * @param location The location of the external table data, used when creating a new table. Defaults to an empty
         *                 string. e.g. "hdfs://path/to/external/mytable"
         * @param partitions A sequence of partition names that should be present in the new schema and/or the Hive
         *                   table. Defaults to an empty sequence. e.g. Seq("year", "month", "day", "hour")
         * @return An Iterable of strings, each representing a DDL statement for creating or altering the Hive table.
         */
        def hiveGetDDLStatements(
            spark: SparkSession,
            newSchema: StructType,
            table: String,
            location: String = "",
            partitions: Seq[String] = Seq.empty
        ): Iterable[String] = {
            val missingPartitions = partitions.filterNot(newSchema.fieldNames.contains)
            val newSchemaPrepared: StructType = newSchema
                .makeNullable()
                .addPartitionColumns(missingPartitions)

            // If the Hive table doesn't exist, get CREATE DDL to create it like newSchema.
            if (!tableExists(spark, table)) {
                Seq(newSchemaPrepared.hiveCreateDDL(table, location, partitions))
            }
            // Else get ALTER DDL statements to alter the existing table to add fields from new Schema.
            else {
                val tableSchema = spark.table(table).schema
                val alterDDLs = tableSchema.hiveAlterDDL(table, newSchemaPrepared)
                if (alterDDLs.nonEmpty) {
                    log.info(
                        s"""Found difference in schemas for Hive table $table
                           |Table schema:\n${tableSchema.treeString}
                           |Input schema:\n${newSchemaPrepared.treeString}
                           |Alter statements:\n${alterDDLs.mkString("\n")}""".stripMargin
                    )
                }

                alterDDLs
            }
            // TODO Add a step to add statements to add/update comments for Hive tables
        }

        /**
         * Evolves a Hive table schema based on a new schema.
         *
         * This method is responsible for evolving the schema of an existing Hive table to match a new schema. It first
         * retrieves the necessary DDL statements to either create the table (if it does not exist) or alter its schema
         * (if it does exist) to match the new schema. These DDL statements are then executed to apply the schema
         * changes. After executing the DDL statements, the method refreshes Spark's metadata cache for the table to
         * reflect the new schema. This is crucial for ensuring that subsequent operations on the table use the updated
         * schema.
         *
         * @param spark The SparkSession instance used for accessing Hive.
         * @param newSchema The new schema that the Hive table should evolve to. This schema is represented as a
         *                  StructType.
         * @param table The fully qualified name of the Hive table to evolve. e.g. "db.table"
         * @param location (Optional) The path to the external table data. This is used when creating a new table.
         *                 e.g. "hdfs://path/to/external/mytable"
         * @param partitions (Optional) A sequence of partition names that should be present in the new schema and/or
         *                   the Hive table.
         *                   e.g. Seq("year", "month", "day", "hour")
         * @return A boolean indicating whether any DDL statements were executed to evolve the table schema.
         */
        def hiveEvolveTable(
            spark: SparkSession,
            newSchema: StructType,
            table: String,
            location: String = "",
            partitions: Seq[String] = Seq.empty
        ): Boolean = {
            val ddlStatements = hiveGetDDLStatements(spark, newSchema, table, location, partitions)

            // CREATE or ALTER the Hive table if we have a change to make.
            if (ddlStatements.nonEmpty) {
                hiveExecDDLStatement(spark, ddlStatements)
                // Refresh Spark's metadata about this Hive table.
                spark.catalog.refreshTable(table)
                true
            }
            else
                false
        }

        /**
          * Executes a list of DDL statements on the Hive server.
          *
          * Use a hive JDBC connection because:
          *   - Spark doesn't accept type evolution (e.g. widening), and adding subfields,...
          *     https://github.com/apache/spark/blob/v3.5.1/sql/core/src/main/scala/org/apache/spark/sql/execution/command/ddl.scala#L373
          *   - When an Hive table has been modified by Spark, it becomes a V2 table. And a V2 table could be modified
          *     by Hive. But then, hive won't update the schema read by Spark.
          *
          * The connection must use the current user for HDFS file permissions
          *
          * @param spark          SparkSession
          * @param ddlStatements  List of DDL statements to execute.
          */
        def hiveExecDDLStatement(spark: SparkSession, ddlStatements: Iterable[String]) = {
            try {
                val jdbcUrl = s"jdbc:${hiveServerUrl(spark)}"
                log.info(s"Connecting to Hive over JDBC at ${jdbcUrl}")
                val hiveDriver = new HiveDriver()
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
        }

        /**
         * Returns a hive2:// (thrift) server url to the hiveserver2 that SparkSession uses.
         * This defaults to hive2://localhost:10000/default;user=$sparkUser;password=
         * if no options are set.  This will consider kerberos principal authentication
         * by looking at the configured hive.server2.authentication.kerberos.principal.
         * @param spark    SparkSession
         * @return
         */
        def hiveServerUrl(spark: SparkSession): String = {
            val hadoopConf = spark.sparkContext.hadoopConfiguration

            val hiveServerPort = hadoopConf.get("hive.server2.thrift.port", "10000")
            // assume that the hive server is on the same host as the (first) configured metastore
            val hiveMetastoreUris = hadoopConf.get("hive.metastore.uris", "thrift://localhost:9083")
            val hiveServerHost = new URI(hiveMetastoreUris.split(",").head).getHost

            val authParams = hadoopConf.get("hive.server2.authentication.kerberos.principal") match {
                case principal: String  => s"principal=${principal}"
                case null               => s"user=${spark.sparkContext.sparkUser};password="
            }
            s"hive2://${hiveServerHost}:${hiveServerPort}/default;${authParams}"
        }
    }

    object IcebergTableSchemaManager {
        import org.wikimedia.analytics.refinery.spark.sql.IcebergExtensions._

        // When maintaining table properties, we ignore these auto-generated keys.
        private val ignoredIcebergTableProperties = Set("transient_lastDdlTime", "current-snapshot-id", "format", "external")

        /**
          * If table does not exist in Iceberg, this will return a single element Seq
          * with a Iceberg CREATE TABLE statement to create a table that represents newSchema.
          * Else, if table does exist, it will return a Seq of Iceberg ALTER TABLE statements
          * required to update the table schema to match any new fields found in newSchema.
          *
          * The list of table alterations are here:
          * https://iceberg.apache.org/docs/latest/spark-ddl/
          *
          * @param spark SparkSession
          * @param newSchema Spark schema representing the schema of the
          *                  table to be created or altered.
          * @param tableName Fully qualified (dotted) Iceberg table name. This is expected to
          *                  be qualified using the iceberg catalog: catalog.db.table
          * @param locationPath Path to external table data.
          * @param tablePartitioningStatements List of partition Statement. Only for new tables.
          * @param tablePropertiesStatements Map of table properties. Only for new tables.
          * @throws IllegalStateException If there are type changes between existing and new
          *                               fields in the newSchema and/or Iceberg table.
          * @return
          */
        def icebergGetDDLStatements(
            spark: SparkSession,
            newSchema: StructType,
            tableName: String,
            locationPath: String,
            tablePartitioningStatements: Seq[String],
            tableProperties: Map[String, String]
        ): Iterable[String] = {
            // If the Iceberg table doesn't exist, get CREATE DDL statement.
            if (!tableExists(spark, tableName)) {
                Seq(
                    newSchema.icebergCreateDDL(
                        tableName,
                        locationPath,
                        tablePartitioningStatements,
                        tableProperties
                    )
                )
            }

            // Else get ALTER DDL statements to alter the existing table to add fields from new Schema and update its
            // properties. Hive DDL for alter table is expected to match Iceberg one.
            else {
                val tableSchema = spark.table(tableName).schema
                // Generate alterations about the schema.
                val alterDDLs = (tableSchema.icebergAlterSchemaDDLs(tableName, newSchema)
                        // And add updates to table properties
                        ++ IcebergTableSchemaManager.icebergGetTablePropertiesAlterDDLs(spark, tableName, tableProperties))
                        // TODO add alterations about partitioning
                if (alterDDLs.nonEmpty) {
                    log.info(s"""Found difference in schemas for Hive table $tableName
                                |Table schema:\n${tableSchema.treeString}
                                |Input schema:\n${newSchema.treeString}
                                |Transformed schema:\n${newSchema.treeString}
                                |Alter statements:\n${alterDDLs.mkString("\n")}""".stripMargin
                    )
                }
                alterDDLs
            }
        }

        /**
          * Returns a map of Iceberg specific table properties for the given table name
          *
          * @param spark     SparkSession
          * @param tableName Fully qualified Hive table name
         * @return
          */
        def icebergGetTableProperties(
            spark: SparkSession,
            tableName: String
        ): Map[String, String] = {
            spark.sql(s"SHOW TBLPROPERTIES $tableName")
                .collect()
                .map(row => (row.get(0).asInstanceOf[String], row.get(1).asInstanceOf[String]))
                .toMap[String, String]
                .filterKeys(!ignoredIcebergTableProperties.contains(_))
        }

        /**
          * Returns a sequence of Iceberg specific table properties update DDLs for the given table name
          *
          * @param spark         SparkSession
          * @param tableName     Fully qualified Hive table name
          * @param newProperties New properties to be set
          * @return A list of DDL statements to update the table properties.
          */
        def icebergGetTablePropertiesAlterDDLs(
            spark: SparkSession,
            tableName: String,
            newProperties: Map[String, String]
        ): Seq[String] = {
            val currentProperties = IcebergTableSchemaManager.icebergGetTableProperties(spark, tableName)
            val propertiesToRemove = currentProperties.filterKeys(!newProperties.contains(_))
            val propertiesToAdd = newProperties.filterKeys(!currentProperties.contains(_))
            val propertiesToUpdate = newProperties.filterKeys(currentProperties.contains)
                .filter { case (k, v) => currentProperties(k) != v }

            // Build the DDL statements
            val unsetStatements = if (propertiesToRemove.isEmpty) { Seq.empty } else {
                Seq(s"UNSET TBLPROPERTIES (${propertiesToRemove.keys.mkString(", ")})")
            }
            val formattedPropertiesToSet = (propertiesToAdd++propertiesToUpdate)
                .toList.sortBy(_._1)
                .map{ case (k, v) => s"$k = '$v'" }
            val setStatements = if (formattedPropertiesToSet.isEmpty) { Seq.empty } else {
                Seq(s"SET TBLPROPERTIES (${formattedPropertiesToSet.mkString(", ")})")
            }
            (unsetStatements ++ setStatements).map(action => s"ALTER TABLE $tableName $action;")
        }
    }


    /**
     * Returns true if the fully qualified table exists in store, else false.
     *
     * @param spark       SparkSession
     * @param tableName   Fully qualified table name
     * @return
     */
    def tableExists(spark: SparkSession, tableName: String): Boolean = {
        allCatch.opt(spark.table(tableName)) match {
            case Some(_) => true
            case _       => false
        }
    }
}
