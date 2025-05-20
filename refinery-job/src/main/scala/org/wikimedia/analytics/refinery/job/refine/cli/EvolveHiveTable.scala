package org.wikimedia.analytics.refinery.job.refine.cli

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.wikimedia.analytics.refinery.job.refine.{RawRefineDataReader, RefineHelper, SparkEventSchemaLoader}
import org.wikimedia.analytics.refinery.job.refine.RefineHelper.{TransformFunction, applyTransforms}
import org.wikimedia.analytics.refinery.job.refine.WikimediaEventSparkSchemaLoader.BASE_SCHEMA_URIS_DEFAULT
import org.wikimedia.analytics.refinery.spark.sql.TableSchemaManager.HiveTableSchemaManager
import org.wikimedia.analytics.refinery.tools.LogHelper
import org.wikimedia.analytics.refinery.tools.config._
import org.wikimedia.eventutilities.core.event.EventSchemaLoader

import java.net.URI
import scala.collection.immutable.ListMap

/**
  * A simple CLI tool to manually evolve (or create!) Hive tables from JSONSchemas.
  */
object EvolveHiveTable extends ConfigHelper with TransformFunctionsConfigHelper with LogHelper {

    val DEFAULT_PARTITIONS: ListMap[String, String] = ListMap(
        "year" -> "long",
        "month" -> "long",
        "day" -> "long",
        "hour" -> "long"
    )

    case class Config(
        table: String,
        schema_uri: String,
        schema_base_uris: Seq[String] = BASE_SCHEMA_URIS_DEFAULT,
        dry_run: Boolean = true,
        timestamps_as_strings: Boolean = true,
        location: Option[String] = None,
        transform_functions: Seq[TransformFunction] = Seq.empty[TransformFunction],
        partition_columns: ListMap[String, String] = DEFAULT_PARTITIONS
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
            |  --partition_columns=datacenter:string,year:long,month:long,day:long,hour:long \\
            |  --transform_functions=transform1,transform2
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
            |    --partition_columns=<name:type>
            |       Optional comma separated list of partition columns to create in the table.
            |       The key is the column name and the value (e.g. datacenter) is the column type (e.g. string).
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

        val evolver = apply(spark, config.schema_base_uris)

        evolver.evolveHiveTableWithSchema(
            config.table,
            URI.create(config.schema_uri),
            config.timestamps_as_strings,
            config.location,
            config.transform_functions,
            config.partition_columns,
            config.dry_run,
        )
    }

    /**
     * Helper apply to construct a EvolveHiveTable instance using schema base URIs.
     * @param baseSchemaUris The URIs to use when looking up schema_uri.
     * @return EvolveHiveTable
     */
    def apply(spark: SparkSession, baseSchemaUris: Seq[String] = BASE_SCHEMA_URIS_DEFAULT): EvolveHiveTable = {
        new EvolveHiveTable(RefineHelper.buildEventSchemaLoader(baseSchemaUris), spark)
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
     * @param location The HDFS location of the external Hive table (optional).
     * @param transformFunctions A list of transform functions to apply to the schema.
     * @param partitionColumns A list of partition columns to create in the table.
     * @param dryRun If true, only log Hive Table changes instead of executing them.
     * @return true if there are schema changes, false otherwise.
     */
    def evolveHiveTableWithSchema(
        table:               String,
        sparkSchema:         StructType,
        location:            Option[String],
        transformFunctions:  Seq[TransformFunction],
        partitionColumns:    ListMap[String, String],
        dryRun:              Boolean,
    ): Boolean = {

        var transformedSchema = sparkSchema

        // Add partition columns if they are not already in the schema.
        partitionColumns.keys.filterNot(transformedSchema.fieldNames.contains).foreach(partitionName => {
            log.debug(s"Adding partition column $partitionName: ${partitionColumns(partitionName)} to schema")
            transformedSchema = transformedSchema.add(
                partitionName,
                partitionColumns(partitionName),
                nullable = true
            )
        })

        transformedSchema = applyTransforms(spark, transformedSchema, transformFunctions)

        log.debug(
            s"Schema after adding partitions and applying transform functions:\n${transformedSchema.treeString}"
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
                partitionColumns.keys.toList
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
                transformedSchema,
                table,
                location,
                partitionColumns.keys.toList
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
     * Looks up the JSONSchema at schemaUri and uses it to evolve the Hive table.
     * @param table The target Hive table, fully qualified.
     *              E.g. "analytics.refinery_webrequest"
     * @param schemaUri The URI of the JSONSchema to use to evolve the table.
     * @param timestampsAsStrings If true, date-time fields will be converted
     * @param location The HDFS location of the external Hive table.
     * @param transformFunctions A list of transform functions to apply to the schema.
     * @param partitionColumns A list of partition columns to create in the table.
     * @param dryRun If true, only log Hive Table changes instead of executing them.
     * @return true if there are schema changes, false otherwise.
     */
    def evolveHiveTableWithSchema(
        table:               String,
        schemaUri:           URI,
        timestampsAsStrings: Boolean,
        location:            Option[String],
        transformFunctions:  Seq[TransformFunction],
        partitionColumns:    ListMap[String, String],
        dryRun:              Boolean,
    ): Boolean = {

        val sparkSchemaLoader= SparkEventSchemaLoader(schemaLoader, timestampsAsStrings = timestampsAsStrings)
        // TODO: schemaLoader should return ObjectNodes
        val sparkSchema = sparkSchemaLoader.load(schemaUri)

        evolveHiveTableWithSchema(
            table,
            sparkSchema,
            location,
            transformFunctions,
            partitionColumns,
            dryRun
        )
    }

}