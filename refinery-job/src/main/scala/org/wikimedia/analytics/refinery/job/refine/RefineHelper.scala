package org.wikimedia.analytics.refinery.job.refine

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.wikimedia.analytics.refinery.core.HivePartition
import org.wikimedia.analytics.refinery.job.refine.RefineHelper.log
import org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame
import org.wikimedia.analytics.refinery.tools.LogHelper
import org.wikimedia.eventutilities.core.event.EventSchemaLoader
import org.wikimedia.eventutilities.core.json.JsonSchemaLoader
import org.wikimedia.eventutilities.core.util.ResourceLoader
import org.wikimedia.eventutilities.spark.sql.JsonSchemaSparkConverter

import java.net.URI
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap

object RefineHelper extends LogHelper {

  // Type for transform functions processing partitions to refine.
  type TransformFunction = PartitionedDataFrame => PartitionedDataFrame

  /**
   * Helper wrapper to construct an EventSchemaLoader form baseUris.
   *
   * @param schemaBaseUris
   * @return
   */
  def buildEventSchemaLoader(
                              schemaBaseUris: Seq[String],
                            ): EventSchemaLoader = {
    val resourceLoader = ResourceLoader.builder()
      .setBaseUrls(ResourceLoader.asURLs(schemaBaseUris.asJava))
      .build()
    val eventSchemaLoaderBuilder = EventSchemaLoader.builder()
      .setJsonSchemaLoader(JsonSchemaLoader.build(resourceLoader))

    eventSchemaLoaderBuilder.build()
  }

  /**
   * Applies a sequence of transformation functions to a schema, creating a DataFrame.
   *
   * This method initializes a DataFrame based on the provided schema and an empty RDD, simulating
   * an empty DataFrame with the specified schema. It then applies a sequence of transformation
   * functions to this DataFrame.
   *
   * @param schema The schema to apply to the newly created DataFrame.
   * @param spark The SparkSession instance used for DataFrame creation and transformations.
   * @param transformFunctions A sequence of functions that take a DataFrame and return a modified DataFrame.
   * @return A DataFrame that has been transformed by the provided sequence of functions.
   */
  def applyTransforms(spark: SparkSession, schema: StructType, transformFunctions: Seq[TransformFunction]): StructType = {
    val df: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    applyTransforms(df, transformFunctions).schema
  }

  /**
   * Applies a sequence of transformation functions to an existing DataFrame.
   *
   * This method is designed to apply a series of transformations to a DataFrame within the context of a specified table.
   * It first creates a fake HivePartition with no actual partition, leveraging the transformation functions that typically
   * operate on partitioned data. This approach allows for the reuse of partition-based transformation functions on DataFrames
   * that are not inherently partitioned. The transformation functions are applied sequentially, with each function receiving
   * the output DataFrame of the previous function as its input, culminating in a fully transformed DataFrame.
   *
   * @param dataframe The input DataFrame to be transformed.
   * @param transformFunctions A sequence of functions that take a PartitionedDataFrame and return a modified PartitionedDataFrame.
   * @return A DataFrame that has been transformed by the provided sequence of functions.
   */
  def applyTransforms(dataframe: DataFrame, transformFunctions: Seq[TransformFunction]): DataFrame = {
    // Creating a partitioned-dataframe with no partition to abuse transform-functions
    val fakeHivePartition = HivePartition("db.table", ListMap.empty[String, String]).copy(location = Some(""))
    val inputPartDf = new PartitionedDataFrame(dataframe, fakeHivePartition)
    transformFunctions
      .foldLeft(inputPartDf)((currPartDf, fn) => fn(currPartDf))
      .df
  }

}

/**
 * Given an EventSchemaLoader and a schemaUri, loads the JSON schema
 * and converts it to a Spark StructType schema.
 *
 * @param eventSchemaLoader The EventSchemaLoader to use to load the schema
 * @param loadLatest True to load the latest schema
 * @param timestampsAsStrings True to convert timestamps to strings
 */
case class SparkEventSchemaLoader(eventSchemaLoader: EventSchemaLoader,
                             loadLatest: Boolean = false,
                             timestampsAsStrings: Boolean = false) {

  /**
   * @param schemaUri The URI of the schema to load. e.g. "/event/1.0.0"
   */
  def load(schemaUri: URI): StructType = {
    log.info(
      s"Loading JSONSchema for schemaUri $schemaUri using $eventSchemaLoader"
    )

    val jsonSchema = if (loadLatest) {
      eventSchemaLoader.getLatestSchema(schemaUri)
    } else {
      eventSchemaLoader.getSchema(schemaUri)
    }

    log.debug(
      s"Loaded ${if (loadLatest) "latest" else ""} JSONSchema for URI $schemaUri:\n$jsonSchema"
    )

    val sparkSchema = JsonSchemaSparkConverter.toDataType(
      // TODO: eventSchemaLoader should return ObjectNodes
      jsonSchema.asInstanceOf[ObjectNode],
      timestampsAsStrings
    ).asInstanceOf[StructType]

    log.debug(
      s"Converted JSONSchema for URI $schemaUri " +
        s"to Spark schema:\n${sparkSchema.treeString}"
    )
    sparkSchema
  }

}
