package org.wikimedia.analytics.refinery.job.refine.tool

import java.net.URI
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.wikimedia.analytics.refinery.job.refine.EventSparkSchemaLoader
import org.wikimedia.analytics.refinery.spark.connectors.DataFrameToHive
import org.wikimedia.analytics.refinery.tools.LogHelper
import org.wikimedia.analytics.refinery.tools.config._
import org.wikimedia.eventutilities.core.event.EventSchemaLoader
import org.wikimedia.eventutilities.spark.sql.JsonSchemaSparkConverter

/**
  * A simple CLI tool to manually evolve (or create!) Hive tables from JSONSchemas.
  */
object EvolveHiveTable extends ConfigHelper {

    val BASE_SCHEMA_URIS_DEFAULT: Seq[String] = Seq(
        "https://schema.discovery.wmnet/repositories/primary/jsonschema",
        "https://schema.discovery.wmnet/repositories/secondary/jsonschema"
    )

    case class Config(
        table: String,
        schema_uri: String,
        schema_base_uris: Seq[String] = BASE_SCHEMA_URIS_DEFAULT,
        dry_run: Boolean = true,
        timestamps_as_strings: Boolean = true,
    )

    object Config {
        val usage: String = s"""
            |EvolveHiveTable - Evolves a Hive table from a JSONSchema or from JSON event data
            |
            |Usage:
            |spark2-submit --class org.wikimedia.analytics.refinery.job.refine.tool.EvolveHiveTable --table=<db.table> --schema_uri=/my/schema/latest --dry_run=false
            |
            |Options:
            |    --table=<full_table_name>
            |       database.table_name to evolve
            |
            |    --schema_uri=<relative_schema_uri>
            |       Schema URI relative to a URI in schema_base_uris to use to evolve the table.
            |       E.g. /my/schema/latest
            |
            |    --schema_base_uris=<base_uri1,base_uri2>
            |       Comma separated list of base URIs to use when looking up schema_uri. E.g.
            |         file:///local/schema/repo,http://remote.schemarepo.org
            |       Default: ${BASE_SCHEMA_URIS_DEFAULT.mkString(",")}
            |
            |    --dry_run=<boolean> Only log Hive Table changes instead of executing them.
            |       Default: true
            |
            |    --timestamps_as_strings=<boolean> If true, date-time fields will be converted
            |       as StringTypes, instead of TimestampTypes.
            |       See: https://phabricator.wikimedia.org/T278467
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
        spark.sparkContext.setLogLevel("WARN")

        val evolver = apply(config.schema_base_uris, spark)
        evolver.evolveHiveTableWithSchema(
            config.table,
            URI.create(config.schema_uri),
            config.dry_run,
            config.timestamps_as_strings,
        )
    }

    /**
      * Helper apply to construct a EvolveHiveTable instance using schema base URIs.
      * @param baseSchemaUris
      * @return
      */
    def apply(
        baseSchemaUris: Seq[String] = EvolveHiveTable.BASE_SCHEMA_URIS_DEFAULT,
        spark: SparkSession
    ): EvolveHiveTable = {
        new EvolveHiveTable(EventSparkSchemaLoader.buildEventSchemaLoader(baseSchemaUris), spark)
    }

}


/**
  * Class to create and/or evolve Hive tables based on JSONSchemas.
  * @param schemaLoader
  * @param spark
  */
class EvolveHiveTable(
    schemaLoader: EventSchemaLoader,
    spark: SparkSession
) extends LogHelper {

    /**
      * Given a Spark StructType schema, compare it to the Hive table schema and evolve it if needed.
      * @param table
      * @param sparkSchema
      * @param dryRun
      * @return true if there are schema changes, false otherwise.
      */
    def evolveHiveTableWithSchema(
        table: String,
        sparkSchema: StructType,
        dryRun: Boolean
    ): Boolean = {
        if (dryRun) {
            // DDL statements will be logged by DataFrameToHive.
            val ddl = DataFrameToHive.getDDLStatements(spark, sparkSchema, table)
            if (ddl.nonEmpty) {
                log.info(s"dryRun mode. Would have altered Hive table $table.\n${ddl.mkString("\n")}\n")
            }
            else {
                log.info(s"dryRun mode.  No changes to Hive table $table were needed.")
            }
            ddl.nonEmpty
        } else {
            // DDL statements will be logged by DataFrameToHive.
            val result = DataFrameToHive.prepareHiveTable(spark, sparkSchema, table)
            if (result) {
                log.info(s"Altered Hive table $table.")
            } else {
                log.info(s"No changes to Hive table $table were made.")
            }
            result
        }
    }

    /**
      * Converts jsonSchema to a Spark StructType schema and evolves the Hive table.
      * @param table
      * @param jsonSchema
      * @param dryRun
      * @param timestampsAsStrings
      * @return
      */
    def evolveHiveTableWithSchema(
        table: String,
        jsonSchema: ObjectNode,
        dryRun: Boolean,
        timestampsAsStrings: Boolean,
    ): Boolean = {
        val sparkSchema = JsonSchemaSparkConverter.toDataType(
            jsonSchema,
            timestampsAsStrings
        ).asInstanceOf[StructType]
        evolveHiveTableWithSchema(table, sparkSchema, dryRun)
    }

    /**
      * Looks up the JSONSchema at schemaUri and uses it to evolve the Hive table.
      * @param table
      * @param schemaUri
      * @param dryRun
      * @param timestampsAsStrings
      * @return
      */
    def evolveHiveTableWithSchema(
        table: String,
        schemaUri: URI,
        dryRun: Boolean,
        timestampsAsStrings: Boolean,
    ): Boolean = {
        // TODO: schemaLoader should return ObjectNodes
        val jsonSchema = schemaLoader.getSchema(schemaUri).asInstanceOf[ObjectNode]
        evolveHiveTableWithSchema(table, jsonSchema, dryRun, timestampsAsStrings)
    }

    /**
      * Given a JsonNode event, uses EventSchemaLoader to extract the schemaUri
      * from the event, look up the JSONSchema at that URI, and uses it to evolve the Hive table.
      * @param table
      * @param event
      * @param dryRun
      * @param timestampsAsStrings
      * @return
      */
    def evolveHiveTableFromEvent(
        table: String,
        event: JsonNode,
        dryRun: Boolean,
        timestampsAsStrings: Boolean,
    ): Boolean = {
        // TODO: schemaLoader should return ObjectNodes
        val jsonSchema = schemaLoader.getEventSchema(event).asInstanceOf[ObjectNode]
        evolveHiveTableWithSchema(table, jsonSchema, dryRun, timestampsAsStrings)
    }

    /**
      * Given an JSON string event, uses EventSchemaLoader to extract the schemaUri
      * from the event, look up the JSONSchema at that URI, and uses it to evolve the Hive table.
      * @param table
      * @param eventString
      * @param dryRun
      * @param timestampsAsStrings
      * @return
      */
    def evolveHiveTableFromEvent(
        table: String,
        eventString: String,
        dryRun: Boolean,
        timestampsAsStrings: Boolean,
    ): Boolean = {
        // TODO: schemaLoader should return ObjectNodes
        val jsonSchema = schemaLoader.getEventSchema(eventString).asInstanceOf[ObjectNode]
        evolveHiveTableWithSchema(table, jsonSchema, dryRun, timestampsAsStrings)
    }
}
