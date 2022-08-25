package org.wikimedia.analytics.refinery.job.refine

import java.net.URL
import scala.collection.JavaConverters._
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.apache.spark.sql.types.StructType
import org.wikimedia.analytics.refinery.spark.sql.JsonSchemaConverter
import org.wikimedia.analytics.refinery.tools.LogHelper
import org.wikimedia.eventutilities.core.event.EventSchemaLoader
import org.wikimedia.eventutilities.core.json.JsonSchemaLoader
import org.wikimedia.eventutilities.core.util.ResourceLoader
import org.wikimedia.eventutilities.core.event.WikimediaDefaults

/**
  * Implementations of SparkSchemaLoader
  */

/**
  * A SparkSchemaLoader that uses org.wikimedia.eventutilities.core.event.EventSchemaLoader to load
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
            case None => {
                log.warn(
                    s"JSONSchema for event data in ${target.inputPath} could not be loaded: " +
                    "the data path was empty"
                )
                None
            }

            case Some(eventString) => {
                // Pass it to EventSchemaLoader to parse it into JsonNode event,
                // and to look up that event's schema.
                val jsonSchema = if (loadLatest) {
                    eventSchemaLoader.getLatestEventSchema(eventString)
                } else {
                    eventSchemaLoader.getEventSchema(eventString)
                }
                log.debug(
                    s"Loaded ${if (loadLatest) "latest" else ""} JSONSchema for event data in " +
                    s"${target.inputPath}:\n$jsonSchema"
                )

                val sparkSchema = JsonSchemaConverter.toSparkSchema(jsonSchema)
                log.debug(
                    s"Converted JSONSchema for event data in ${target.inputPath} " +
                    s"to Spark schema:\n${sparkSchema.treeString}"
                )
                Some(sparkSchema)
            }
        }
    }
}

/**
  * Companion object for class EventSparkSchemaLoader to statically set
  * URL configuration to be able to load hdfs:// URIs.
  */
object EventSparkSchemaLoader {
    // Make sure that EventSchemaLoader can handle hdfs:// URIs.
    // (This makes is possible for e.g. com.google.common.io.Resources
    // to load hdfs URIs.)
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory)

    /**
      * Helper constructor to create a EventSparkSchemaLoader using EventSchemaLoader from a
      * list of schemaBaseUris.
      */
    def apply(
        schemaBaseUris: Seq[String],
        loadLatest: Boolean = true,
        schemaField: Option[String] = None
    ): EventSparkSchemaLoader = {
        new EventSparkSchemaLoader(buildEventSchemaLoader(schemaBaseUris, schemaField), loadLatest)
    }

    /**
      * Builds a wikimedia event utilities EventSchemaLoader that loads
      * schemas from the given schemaBaseUris.
      */
    def buildEventSchemaLoader(
        schemaBaseUris: Seq[String],
        schemaField: Option[String] = None
    ): EventSchemaLoader = {
        val resourceLoader = ResourceLoader.builder()
            .setBaseUrls(ResourceLoader.asURLs(schemaBaseUris.asJava))
            .build()
        val eventSchemaLoaderBuilder = EventSchemaLoader.builder()
            .setJsonSchemaLoader(JsonSchemaLoader.build(resourceLoader))

        if (schemaField.isDefined) {
            eventSchemaLoaderBuilder.setSchemaField(schemaField.get)
        }
        eventSchemaLoaderBuilder.build()
    }
}


object WikimediaEventSparkSchemaLoader {
    /**
      * Returns an instance of EventSparkSchemaLoader using WMF's
      * remote event schema repository URLs.
      *
      * @param loadLatest
      * @return
      */
    def apply(loadLatest: Boolean = true): EventSparkSchemaLoader = {
        new EventSparkSchemaLoader(WikimediaDefaults.EVENT_SCHEMA_LOADER, loadLatest)
    }
}
