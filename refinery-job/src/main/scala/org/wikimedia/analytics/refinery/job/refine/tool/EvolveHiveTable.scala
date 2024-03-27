package org.wikimedia.analytics.refinery.job.refine.tool

import java.net.URI
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.wikimedia.analytics.refinery.job.refine.{EventSparkSchemaLoader, TransformFunctionsConfigHelper}
import com.fasterxml.jackson.databind.JsonNode
import org.wikimedia.analytics.refinery.job.refine.EventSparkSchemaLoader.BASE_SCHEMA_URIS_DEFAULT
import org.wikimedia.analytics.refinery.job.refine.RefineHelper.{TransformFunction, applyTransforms}
import org.wikimedia.analytics.refinery.spark.sql.TableSchemaManager.HiveTableSchemaManager
import org.wikimedia.analytics.refinery.tools.LogHelper
import org.wikimedia.analytics.refinery.tools.config._
import org.wikimedia.eventutilities.core.event.EventSchemaLoader
import org.wikimedia.eventutilities.spark.sql.JsonSchemaSparkConverter

/**
  * A simple CLI tool to manually evolve (or create!) Hive tables from JSONSchemas.
  */
object EvolveHiveTable extends ConfigHelper with TransformFunctionsConfigHelper with LogHelper {

    case class Config(
        table: String,
        schema_uri: String,
        schema_base_uris: Seq[String] = BASE_SCHEMA_URIS_DEFAULT,
        dry_run: Boolean = true,
        timestamps_as_strings: Boolean = true,
        location: String = "",
        transform_functions: Seq[TransformFunction] = Seq.empty[TransformFunction],
        partitions: Seq[String] = Seq.empty[String]
    )

    object Config {
        val usage: String = s"""
            |EvolveHiveTable - Evolves a Hive table from a JSONSchema or from JSON event data
            |
            |Usage:
            |spark3-submit --class org.wikimedia.analytics.refinery.job.refine.tool.EvolveHiveTable \\
            |  --table=<db.table> --schema_uri=/my/schema/latest --dry_run=false \\
            |  --schema_base_uris=file:///local/schema/repo,http://remote.schemarepo.org \\
            |  --timestamps_as_strings=true --location=hdfs:///user/me/table \\
            |  --partitions=datacenter,year,month,day,hour transform_functions=transform1,transform2
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
            |
            |    --location=<hdfs_path>
            |       Optional HDFS path of the Hive table.
            |
            |    --transform_functions=<transform_function1,transform_function2>
            |      Optional comma separated list of transform functions to apply to the schema.
            |
            |    --partitions=<sql_strings>
            |       Optional comma separated list of partition columns to create in the table.
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
            config.timestamps_as_strings,
            config.location,
            config.transform_functions,
            config.partitions,
            config.dry_run,
        )
    }

    /**
     * Helper apply to construct a EvolveHiveTable instance using schema base URIs.
     * @param baseSchemaUris The URIs to use when looking up schema_uri.
     * @return EvolveHiveTable
     */
    def apply(
        baseSchemaUris: Seq[String] = BASE_SCHEMA_URIS_DEFAULT,
        spark: SparkSession,
    ): EvolveHiveTable = {
        new EvolveHiveTable(EventSparkSchemaLoader.buildEventSchemaLoader(baseSchemaUris), spark)
    }
}

/**
 * Class to create and/or evolve Hive tables based on JSONSchemas.
 * @param schemaLoader The EventSchemaLoader to use to load JSONSchemas.
 * @param spark The SparkSession to use to interact with Hive.
 */
class EvolveHiveTable(schemaLoader: EventSchemaLoader, spark: SparkSession) extends LogHelper {

    /**
     * Given a Spark StructType schema, compare it to the Hive table schema and evolve it if needed.
     * @param table The target Hive table, fully qualified.
     *              E.g. "analytics.refinery_webrequest"
     * @param sparkSchema The Spark StructType schema to compare to the Hive table schema.
     * @param timestampsAsStrings If true, date-time fields will be converted
     * @param location The HDFS location of the external Hive table.
     * @param transformFunctions A list of transform functions to apply to the schema.
     * @param partitions A list of partition columns to create in the table.
     * @param dryRun If true, only log Hive Table changes instead of executing them.
     * @return true if there are schema changes, false otherwise.
     */
    def evolveHiveTableWithSchema(
        table: String,
        sparkSchema: StructType,
        location: String,
        transformFunctions: Seq[TransformFunction],
        partitions: Seq[String],
        dryRun: Boolean,
    ): Boolean = {

        val transformedSchema: StructType = applyTransforms(
            sparkSchema,
            spark,
            table,
            transformFunctions
        )

        if (dryRun) {
            // In case of dryrun, we are using hiveGetDDLStatements instead of hiveEvolveTable. It generates the DDL
            // statements that would be executed to evolve the Hive table, but does not execute them.
            // As the DDL statements are going to be logged by TableSchemaManager, we don't log them again here.
            val ddl = HiveTableSchemaManager.hiveGetDDLStatements(
                spark,
                transformedSchema,
                table,
                location,
                partitions
            )
            if (ddl.nonEmpty) {
                log.info(s"dryRun mode. Would have altered Hive table $table.\n${ddl.mkString("\n")}\n")
            }
            else {
                log.info(s"dryRun mode.  No changes to Hive table $table were needed.")
            }
            ddl.nonEmpty
        } else {
            // DDL statements will be generated, logged and executed by HiveTableSchemaManager.
            val result = HiveTableSchemaManager.hiveEvolveTable(
                spark,
                sparkSchema,
                table,
                location,
                partitions
            )
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
     * @param table The target Hive table, fully qualified.
     *              E.g. "analytics.refinery_webrequest"
     * @param jsonSchema The JSONSchema to use to evolve the table.
     * @param timestampsAsStrings If true, date-time fields will be converted
     * @param location The HDFS location of the external Hive table.
     * @param transformFunctions A list of transform functions to apply to the schema.
     * @param partitions A list of partition columns to create in the table.
     * @param dryRun If true, only log Hive Table changes instead of executing them.
     * @return true if there are schema changes, false otherwise.
     */
    def evolveHiveTableWithSchema(
        table: String,
        jsonSchema: ObjectNode,
        timestampsAsStrings: Boolean,
        location: String,
        transformFunctions: Seq[TransformFunction],
        partitions: Seq[String],
        dryRun: Boolean,
    ): Boolean = {
        val sparkSchema = JsonSchemaSparkConverter
            .toDataType(jsonSchema, timestampsAsStrings).asInstanceOf[StructType]  // Convert timestamps to strings
        evolveHiveTableWithSchema(
            table,
            sparkSchema,
            location,
            transformFunctions,
            partitions,
            dryRun
        )
    }

    /**
     * Looks up the JSONSchema at schemaUri and uses it to evolve the Hive table.
     * @param table The target Hive table, fully qualified.
     *              E.g. "analytics.refinery_webrequest"
     * @param schemaUri The URI of the JSONSchema to use to evolve the table.
     * @param timestampsAsStrings If true, date-time fields will be converted
     * @param location The HDFS location of the external Hive table.
     * @param transformFunctions A list of transform functions to apply to the schema.
     * @param partitions A list of partition columns to create in the table.
     * @param dryRun If true, only log Hive Table changes instead of executing them.
     * @return true if there are schema changes, false otherwise.
     */
    def evolveHiveTableWithSchema(
        table: String,
        schemaUri: URI,
        timestampsAsStrings: Boolean,
        location: String,
        transformFunctions: Seq[TransformFunction],
        partitions: Seq[String],
        dryRun: Boolean,
    ): Boolean = {
        // TODO: schemaLoader should return ObjectNodes
        val jsonSchema = schemaLoader.getSchema(schemaUri).asInstanceOf[ObjectNode]
        evolveHiveTableWithSchema(
            table,
            jsonSchema,
            timestampsAsStrings,
            location,
            transformFunctions,
            partitions,
            dryRun
        )
    }


    /**
     * Given a JsonNode event, uses EventSchemaLoader to extract the schemaUri
     * from the event, look up the JSONSchema at that URI, and uses it to evolve the Hive table.
     * @param table The target Hive table, fully qualified.
     * @param event The event to use to evolve the table.
     * @param timestampsAsStrings If true, date-time fields will be converted
     * @param location The HDFS location of the external Hive table.
     * @param transformFunctions A list of transform functions to apply to the schema.
     * @param partitions A list of partition columns to create in the table.
     * @param dryRun If true, only log Hive Table changes instead of executing them.
     * @return
     */
    def evolveHiveTableFromEvent(
        table: String,
        event: JsonNode,
        timestampsAsStrings: Boolean,
        location: String,
        transformFunctions: Seq[TransformFunction],
        partitions: Seq[String],
        dryRun: Boolean,
    ): Boolean = {
        // TODO: schemaLoader should return ObjectNodes
        val jsonSchema = schemaLoader.getEventSchema(event).asInstanceOf[ObjectNode]
        evolveHiveTableWithSchema(
            table,
            jsonSchema,
            timestampsAsStrings,
            location,
            transformFunctions,
            partitions,
            dryRun,
        )
    }

    /**
     * This method a variant of evolveHiveTableFromEvent that provides default arguments.
     */
    def evolveHiveTableFromEvent(
        table: String,
        event: JsonNode,
        timestampsAsStrings: Boolean,
        dryRun: Boolean,
    ): Boolean = {
        evolveHiveTableFromEvent(
            table,
            event,
            timestampsAsStrings,
            location = "",
            transformFunctions = Seq.empty[TransformFunction],
            partitions = Seq.empty[String],
            dryRun,
        )
    }

    /**
     * Given an JSON string event, uses EventSchemaLoader to extract the schemaUri
     * from the event, look up the JSONSchema at that URI, and uses it to evolve the Hive table.
     * @param table The target Hive table, fully qualified.
     * @param eventString The event to use to evolve the table, as a JSON string.
     * @param timestampsAsStrings If true, date-time fields will be converted
     * @param location The HDFS location of the external Hive table.
     * @param transformFunctions A list of transform functions to apply to the schema.
     * @param partitions A list of partition columns to create in the table.
     * @param dryRun If true, only log Hive Table changes instead of executing them.
     * @return
     */
    def evolveHiveTableFromEvent(
        table: String,
        eventString: String,
        timestampsAsStrings: Boolean,
        location: String = "",
        transformFunctions: Seq[TransformFunction] = Seq.empty[TransformFunction],
        partitions: Seq[String] = Seq.empty[String],
        dryRun: Boolean,
    ): Boolean = {
        // TODO: schemaLoader should return ObjectNodes
        val jsonSchema = schemaLoader.getEventSchema(eventString).asInstanceOf[ObjectNode]
        evolveHiveTableWithSchema(
            table,
            jsonSchema,
            timestampsAsStrings,
            location,
            transformFunctions,
            partitions,
            dryRun,
        )
    }

    /**
     * This method a variant of evolveHiveTableFromEvent that provides default arguments.
     */
    def evolveHiveTableFromEvent(
        table: String,
        eventString: String,
        timestampsAsStrings: Boolean,
        dryRun: Boolean,
    ): Boolean = {
        evolveHiveTableFromEvent(
            table,
            eventString,
            timestampsAsStrings,
            location = "",
            transformFunctions = Seq.empty[TransformFunction],
            partitions = Seq.empty[String],
            dryRun,
        )
    }
}