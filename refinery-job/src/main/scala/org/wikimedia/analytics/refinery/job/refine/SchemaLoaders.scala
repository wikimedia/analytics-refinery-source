package org.wikimedia.analytics.refinery.job.refine

import org.wikimedia.analytics.refinery.core.jsonschema.EventSchemaLoader
import org.wikimedia.analytics.refinery.spark.sql.JsonSchemaConverter
import org.apache.spark.sql.types.StructType
import org.wikimedia.analytics.refinery.core.LogHelper

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import java.net.URL

/**
  * Implementations of SparkSchemaLoader
  */


/**
  * A SparkSchemaLoader that uses refinery.core.jsonschema.EventSchemaLoaders to load
  * schemas from example event data.  This class will get the first line out of a RefineTarget
  * and expect it to be a JSON event.  That event will be passed to the eventSchemaLoader
  * to get a JsonSchema for it.  Then JsonSchemaConverter.toSparkSchema will convert that JSONSchema
  * to a Spark (StructType) schema.
  *
  * This class should be instantiated and provided as RefineTarget's schemaLoader parameter.
  * @param eventSchemaLoader EventSchemaLoader implementation
  */
class EventSparkSchemaLoader(eventSchemaLoader: EventSchemaLoader)
    extends SparkSchemaLoader with LogHelper {

    // Make sure that EventSchemaLoader can handle hdfs:// URIs.
    // NOTE: this can only be called once per JVM.
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory)

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
        val firstLine = target.inputFormat match {
            case "sequence_json" =>
                target.spark.sparkContext.sequenceFile[Long, String](target.inputPath.toString)
                    .map(t => t._2).take(1).headOption

            case "json" =>
                target.spark.sparkContext.textFile(target.inputPath.toString).take(1).headOption

            // If there is no data, we can't load a schema.
            case "empty" =>
                None

            case _ =>
                throw new RuntimeException(
                    s"Cannot use ${eventSchemaLoader} to load data with format ${target.inputFormat}. " +
                    "Must be one either 'sequence_json' or 'json'"
                )
        }

        firstLine match {
            // If no firstLine could be read
            case None =>
                log.warn(
                    s"JSONSchema for event data in ${target.inputPath} could not be loaded: " +
                        "the data path was empty"
                )
                None

            case Some(line) =>
                // Pass it to EventSchemaLoader to parse it into JsonNode event,
                // and to look up that event's schema.
                val jsonSchema = eventSchemaLoader.getEventSchema(firstLine.get)
                log.debug(s"Loaded JSONSchema for event data in ${target.inputPath}:\n${jsonSchema}")

                val sparkSchema = JsonSchemaConverter.toSparkSchema(jsonSchema)
                log.debug(
                    s"Converted JSONSchema for event data in ${target.inputPath} " +
                        s"to Spark schema:\n${sparkSchema.treeString}"
                )
                Some(sparkSchema)
        }
    }
}
