package org.wikimedia.analytics.refinery.job.refine

import org.wikimedia.analytics.refinery.core.jsonschema.EventSchemaLoader
import org.wikimedia.analytics.refinery.spark.sql.JsonSchemaConverter
import org.wikimedia.analytics.refinery.spark.sql.HiveExtensions._
import org.apache.spark.sql.types.StructType
import org.wikimedia.analytics.refinery.core.LogHelper
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import java.net.URL
import scala.collection.JavaConverters._

import scala.util.{Failure, Success, Try}

/**
  * Implementations of SparkSchemaLoader
  */

/**
  * A SparkSchemaLoader that uses refinery.core.jsonschema.EventSchemaLoader to load
  * schemas from example event data.  This class will get the first line out of a RefineTarget
  * and expect it to be a JSON event.  That event will be passed to the eventSchemaLoader
  * to get a JsonSchema for it.  Then JsonSchemaConverter.toSparkSchema will convert that JSONSchema
  * to a Spark (StructType) schema.
  *
  * This class should be instantiated and provided as RefineTarget's schemaLoader parameter.
  * @param eventSchemaLoader
  *     EventSchemaLoader implementation
  * @param loadLatest
  *     If true, will call eventSchemaLoader getLatestEventSchema instead of getEventSchema.
  */
class EventSparkSchemaLoader(
    eventSchemaLoader: EventSchemaLoader,
    loadLatest: Boolean = true
) extends SparkSchemaLoader with LogHelper {

    /**
      * Reads the first event out of the RefineTarget and converts its JSONSchema to a SparkSchema.
      * @param target RefineTarget to get the schema of
      * @return
      */
    def loadSchema(target: RefineTarget): Option[StructType] = {
        log.info(
            s"Loading JSONSchema for event data in ${target.inputPath} using ${eventSchemaLoader}"
        )

        // Get the first line out of the inputPath
        val firstLine = target.firstLine()

        firstLine match {
            // If no firstLine could be read
            case None =>
                log.warn(
                    s"JSONSchema for event data in ${target.inputPath} could not be loaded: " +
                        "the data path was empty"
                )
                None

            case Some(eventString) =>
                // Pass it to EventSchemaLoader to parse it into JsonNode event,
                // and to look up that event's schema.
                val jsonSchema = if (loadLatest) {
                    eventSchemaLoader.getLatestEventSchema(eventString)
                } else {
                    eventSchemaLoader.getEventSchema(eventString)
                }

                log.debug(s"Loaded ${if (loadLatest) "latest" else "" } JSONSchema for event data in ${target.inputPath}:\n$jsonSchema")

                val sparkSchema = JsonSchemaConverter.toSparkSchema(jsonSchema)

                // If the target Hive table exists, then go ahead and merge the
                // input JSONSchema into the Hive schema, keeping the casing on top
                // level field names where possible (since this schema will be used to
                // load JSON data). Because we only use the $schema for the first
                // event in the input target data, merging whatever that is with
                // the Hive schema will ensure that other events in the file
                // that have fields that Hive has, but that the first event's schema
                // doesn't have, will still be read.  Ideally this wouldn't matter,
                // since different schema versions should all be backwards compatible,
                // but is is very possible that the first event in the input data is
                // using an older schema.  Without merging, events with newer schemas
                // and new fields (that have already been evolved into the Hive schema)
                // would have their new fields nulled.
                // See also:
                // - https://phabricator.wikimedia.org/T227088
                // - https://phabricator.wikimedia.org/T226219
                val schema = if (target.tableExists()) {
                    val s = target.spark.table(target.tableName).schema.merge(sparkSchema, false)
                    log.debug(
                        s"Converted JSONSchema for event data in ${target.inputPath} " +
                        s"to Spark schema and merged with table ${target.tableName} schema:\n${s.treeString}"
                    )
                    s

                } else {
                    log.debug(
                        s"Converted JSONSchema for event data in ${target.inputPath} " +
                        s"to Spark schema:\n${sparkSchema.treeString}"
                    )
                    sparkSchema
                }
                Some(schema)
        }
    }
}

/**
  * Companion object for class EventSparkSchemaLoader to statically set
  * URL configuration to be able to load hdfs:// URIs.
  */
object EventSparkSchemaLoader {
    // Make sure that EventSchemaLoader can handle hdfs:// URIs.
    // NOTE: this can only be called once per JVM.
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory)

    /**
      * Helper constructor to create a EventSparkSchemaLoader using EventSchemaLoader from a
      * list of schemaBaseUris.
      *
      * @param schemaBaseUris
      * @param loadLatest
      * @return
      */
    def apply(
        schemaBaseUris: Seq[String],
        loadLatest: Boolean = true
    ): EventSparkSchemaLoader = {
        new EventSparkSchemaLoader(new EventSchemaLoader(schemaBaseUris.asJava), loadLatest)
    }
}


object WikimediaEventSparkSchemaLoader {
    /**
      * Returns an instance of EventSparkSchemaLoader using WMF's
      * remote event schema repository URLs by default.
      *
      * @param schemaBaseUris
      * @param loadLatest
      * @return
      */
    def apply(
        schemaBaseUris: Seq[String] = Seq(
            "https://schema.wikimedia.org/repositories/primary/jsonschema",
            "https://schema.wikimedia.org/repositories/secondary/jsonschema"
        ),
        loadLatest: Boolean = true
    ): EventSparkSchemaLoader = {
        EventSparkSchemaLoader(schemaBaseUris, loadLatest)
    }
}