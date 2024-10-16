package org.wikimedia.analytics.refinery.job.refine.cli

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.wikimedia.analytics.refinery.job.refine.{RawRefineDataReader, RefineHelper}
import org.wikimedia.analytics.refinery.job.refine.RefineHelper.{TransformFunction, applyTransforms}
import org.wikimedia.analytics.refinery.job.refine.WikimediaEventSparkSchemaLoader.BASE_SCHEMA_URIS_DEFAULT
import org.wikimedia.analytics.refinery.spark.sql.IcebergExtensions.IcebergStructTypeExtensions
import org.wikimedia.analytics.refinery.spark.sql.TableSchemaManager.IcebergTableSchemaManager
import org.wikimedia.analytics.refinery.tools.LogHelper
import org.wikimedia.analytics.refinery.tools.config._
import org.wikimedia.eventutilities.core.event.EventSchemaLoader

import java.net.URI
import scala.util.control.Exception.ignoring


/**
  * A simple CLI tool to manually update or create Iceberg tables from JSONSchema.
  */
object EvolveIcebergTable extends ConfigHelper with TransformFunctionsConfigHelper with LogHelper {

    case class Config(
        table: String,
        schema_uri: String,
        location: Option[String] = None,
        schema_base_uris: Seq[String] = BASE_SCHEMA_URIS_DEFAULT,
        transform_functions: Seq[TransformFunction] = Seq.empty[TransformFunction],
        partition_by: Seq[String] = Seq.empty[String],
        table_properties: Map[String, String] = Map.empty[String, String],
        dry_run: Boolean = true,
    )

    object Config {
        val usage: String = s"""
            |EvolveIcebergTable - Create or update an Iceberg table from a JSONSchema URI.
            |
            |Usage:
            |spark2-submit --class org.wikimedia.analytics.refinery.job.refine.tool.EvolveIcebergTable \\
            |    --table=<db.table> --schema_uri=/my/schema/latest \\
            |    --location=hdfs:///user/me/table --dry_run=false
            |
            |Options:
            |    --table=<full_table_name>
            |       fully qualified table name. e.g. database.table_name
            |
            |    --schema_uri=<relative_schema_uri>
            |       Schema URI relative to a URI in schema_base_uris to use to evolve the table.
            |       The last part of the URI is the schema version.
            |       E.g. /analytics/legacy/navigation_timing/latest
            |
            |    --location=<hdfs_path>
            |       Optional HDFS path of the Iceberg table.
            |       E.g. hdfs:///user/me/table
            |
            |    --schema_base_uris=<base_uri1,base_uri2>
            |       Comma separated list of base URIs to use when looking up schema_uri.
            |       E.g. file:///local/schema/repo,http://remote.schemarepo.org
            |       Default: ${BASE_SCHEMA_URIS_DEFAULT.mkString(",")}
            |
            |    --transform_functions=<transform_function1,transform_function2>
            |       Comma separated list of transform functions to apply to the schema.
            |
            |    --partition_by=<sql_strings>
            |       SQL fragments to partition the Iceberg table.
            |       EvolveIcebergTable will append those to the CREATE TABLE statement.
            |       See more info about partitioning Iceberg table here:
            |       https://iceberg.apache.org/docs/latest/spark-ddl/#partitioned-by
            |       As of today, the alteration of an Iceberg table partitioning scheme is not managed by this tool.
            |       Default is no partitioning scheme.
            |       e.g. 1: "wikidb" a column itself
            |       e.g. 2: "year(dt)" a function applied to a column
            |       e.g. 3: "month(meta.dt),wikidb" a comma separated list of columns and transformed columns
            |       e.g. 4: '["bucket(10, uuid)", "day(dt)"]' Defined as JSON to handle comma in the SQL string
            |
            |    --table_properties=<key1=value1,key2=value2>
            |       Set and unset the Iceberg table properties.
            |       Ordering strategy could be setup from those properties.
            |       e.g. 1: k1:v1,k2:v2
            |       e.g. 2: write.sorted-by:wiki_db,write.sort-order:ASC
            |
            |    --dry_run=<boolean> Only log Iceberg Table changes instead of executing them.
            |       Default: true
            """.stripMargin
    }

    def main(args: Array[String]): Unit = {
        if (args.isEmpty || args.contains("--help")) {
            println(Config.usage)
            sys.exit(0)
        }
        val config  = configureArgs[Config](args)

        val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
        adjustLoggingLevelToSparkMaster(spark)

        val tool = apply(config.schema_base_uris, spark)
        tool.evolveIcebergTableWithSchema(
            config.table,
            URI.create(config.schema_uri),
            config.transform_functions,
            config.location,
            config.partition_by,
            config.table_properties,
            config.dry_run
        )
    }

    /**
      * Helper apply to construct a EvolveIcebergTable instance using schema base URIs.
      * @param baseSchemaUris
      * @return
      */
    def apply(
        baseSchemaUris: Seq[String] = BASE_SCHEMA_URIS_DEFAULT,
        spark: SparkSession
    ): EvolveIcebergTable = {
        new EvolveIcebergTable(RefineHelper.buildEventSchemaLoader(baseSchemaUris), spark)
    }

}


/**
  * Class to create and/or update an Iceberg tables based on a JSONSchema.
  * @param schemaLoader
  * @param spark
  */
class EvolveIcebergTable(
    schemaLoader: EventSchemaLoader,
    spark: SparkSession
) extends LogHelper with TransformFunctionsConfigHelper {

    /**
     *
      * Looks up the JSONSchema at schemaUri and uses it to create or update the Iceberg table
      * by comparing it to the Iceberg table schema.
     *
     * @param table The target Iceberg table
      * @param sparkSchema The Spark StructType schema to compare to the Iceberg table schema.
      * @param transformFunctions A list of transform functions to apply to the schema.
      * @param location            The HDFS location of the external Hive table.
      * @param partitionBy
      * @param tableProperties
      * @param dryRun
      * @return true if there are schema changes, false otherwise.
      */
    def evolveIcebergTableWithSchema(
        table: String,
        sparkSchema: StructType,
        transformFunctions: Seq[TransformFunction],
        location: Option[String],
        partitionBy: Seq[String],
        tableProperties: Map[String, String],
        dryRun: Boolean
    ): Boolean = {

        val transformedSchema: StructType = applyTransforms(spark, sparkSchema, transformFunctions).makeNullable()

        val ddlStatements = IcebergTableSchemaManager.icebergGetDDLStatements(
            spark,
            transformedSchema,
            table,
            location,
            partitionBy,
            tableProperties
        )

        if (dryRun) {
            // DDL statements will be logged by TableSchemaManager.
            if (ddlStatements.nonEmpty) {
                log.info(s"dryRun mode. Would have evolve Iceberg table $table.\n\n${ddlStatements.mkString("\n")}\n\n")
            }
            else {
                log.info(s"dryRun mode. No changes to Iceberg table $table were needed.")
            }
        } else {
            try {
                ddlStatements.foreach { ddl =>
                    log.info(s"Altering Iceberg table $table:\n\n$ddl\n\n")
                    ignoring(classOf[AlreadyExistsException]) {
                        spark.sql(ddl)
                    }
                }
            } catch {
                case e: Throwable =>
                    log.error("Error executing Spark Iceberg DDL commands\n" + e.getMessage)
                    throw e
            }

            if (ddlStatements.isEmpty) {
                log.info(s"No changes to Iceberg table $table were made.")
            }
        }
        ddlStatements.nonEmpty
    }

    /**
     * Overloaded method that gets the spark schema from schemaUri
     */
    def evolveIcebergTableWithSchema(
        table:              String,
        schemaUri:          URI,
        transformFunctions: Seq[TransformFunction],
        location:           Option[String],
        partitionBy:        Seq[String],
        tableProperties:    Map[String, String],
        dryRun:             Boolean
    ): Boolean = {
        val sparkSchema = RefineHelper.loadSparkSchema(schemaLoader, schemaUri.toString)
        evolveIcebergTableWithSchema(
            table, sparkSchema, transformFunctions, location, partitionBy, tableProperties, dryRun
        )
    }
}
