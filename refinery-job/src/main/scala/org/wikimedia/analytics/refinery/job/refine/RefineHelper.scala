package org.wikimedia.analytics.refinery.job.refine

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.wikimedia.analytics.refinery.core.HivePartition
import org.wikimedia.analytics.refinery.job.refine.WikimediaEventSparkSchemaLoader.BASE_SCHEMA_URIS_DEFAULT
import org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame
import org.wikimedia.analytics.refinery.tools.LogHelper
import org.wikimedia.eventutilities.core.util.ResourceLoader
import org.wikimedia.eventutilities.core.event.EventSchemaLoader
import org.wikimedia.eventutilities.core.json.JsonSchemaLoader
import org.wikimedia.eventutilities.spark.sql.JsonSchemaSparkConverter

import java.net.URI
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap

object RefineHelper extends LogHelper {

    // Type for transform functions processing partitions to refine.
    type TransformFunction = PartitionedDataFrame => PartitionedDataFrame

    /**
      * Reads input DataFrames from specified paths
      *
      * This method utilizes `RefineTarget` for each input path to read data frames according to the SparkSchemaLoader,
      *  input format, and reader options. It then merges these data frames into one.
      *
      * @param spark SparkSession The Spark session to use for data frame operations.
      * @param inputPaths Seq[String] A sequence of strings representing the paths to input data.
      * @param inputSchemaLoader A SparkSchemaLoader implementation that returns a Spark StructType schema.
      * @param inputFormat String (default "JSON") The format of the input data. Defaults to JSON.
      * @param dataframeReaderOptions Map[String, String] Options to pass to the DataFrameReader.
      * @param corruptRecordFailureThreshold Integer The threshold for failing on corrupt records.
      * @return DataFrame A merged data frame from all input paths, with the specified schema and options applied.
      */
    def readInputDataFrame(
        spark: SparkSession,
        inputPaths: Seq[String],
        inputSchemaLoader: SparkSchemaLoader,
        inputFormat: String = "json",
        dataframeReaderOptions: Map[String, String] = Map(),
        corruptRecordFailureThreshold: Integer = 1
    ): DataFrame = {
        log.info(s"Reading schema from $inputSchemaLoader with format $inputFormat")

        val refineTargets = inputPaths.map(inputPath => new RefineTarget(
            spark,
            new Path(inputPath),
            HivePartition("a/b/c/d/p=0"),  // We are only using RefineTarget for input here
            schemaLoader = inputSchemaLoader,
            inputFormatOpt = Some(inputFormat),
            providedDfReaderOptions = dataframeReaderOptions,
        ))

        refineTargets
            .map(Refine.getInputPartitionedDataFrame(_, corruptRecordFailureThreshold).df)
            .reduce(_ union _)
    }

    /**
     * Reads an inputDataFrame using the schema loaded by searching
     * inputSchemaBaseURIs for inputSchemaURI.
     *
     * @param spark      SparkSession The Spark session to use for data frame operations.
     * @param inputPaths Seq[String] A sequence of strings representing the paths to input data.
     * @param inputSchemaURI Relative path at which to look up the event schema
     * @param inputSchemaBaseURIs Base URIS in which to look for the event schema
     * @param inputFormat                   String (default "JSON") The format of the input data. Defaults to JSON.
     * @param dataframeReaderOptions        Map[String, String] Options to pass to the DataFrameReader.
     * @param corruptRecordFailureThreshold Integer The threshold for failing on corrupt records.
     * @return
     */
    def readInputDataFrameWithSchemaURI(
        spark: SparkSession,
        inputPaths: Seq[String],
        inputSchemaURI: String,
        inputSchemaBaseURIs: Seq[String] = BASE_SCHEMA_URIS_DEFAULT,
        inputFormat: String = "json",
        dataframeReaderOptions: Map[String, String] = Map(),
        corruptRecordFailureThreshold: Integer = 1,
        loadLatest: Boolean = false,
        timestampsAsStrings: Boolean = false,
    ): DataFrame = {

        val eventSchemaLoader: EventSchemaLoader = buildEventSchemaLoader(inputSchemaBaseURIs)

        val sparkSchema = loadSparkSchema(
            eventSchemaLoader,
            inputSchemaURI,
            loadLatest=loadLatest,
            timestampsAsStrings=timestampsAsStrings,
        )

        // We've already looked up the schema from the inputSchemaURI,
        // so use an ExplicitSchemaLoader to always return that schema.
        val explicitSparkSchemaLoader = ExplicitSchemaLoader(Some(sparkSchema))
        readInputDataFrame(
            spark,
            inputPaths,
            explicitSparkSchemaLoader,
            inputFormat,
            dataframeReaderOptions,
            corruptRecordFailureThreshold,
        )
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
     * Given an EventSchemaLoader and a schemaUri, loads the JSON schema
     * and converts it to a Spark StructType schema.
     * @param eventSchemaLoader The EventSchemaLoader to use to load the schema
     * @param schemaUri The URI of the schema to load. e.g. "/event/1.0.0"
     * @param loadLatest True to load the latest schema
     * @param timestampsAsStrings True to convert timestamps to strings
     * @return
     */
    def loadSparkSchema(
        eventSchemaLoader: EventSchemaLoader,
        schemaUri: String,
        loadLatest: Boolean = false,
        timestampsAsStrings: Boolean = false
    ): StructType = {

        log.info(
            s"Loading JSONSchema for schemaUri $schemaUri using $eventSchemaLoader"
        )

        val jsonSchema = if (loadLatest) {
            eventSchemaLoader.getLatestSchema(URI.create(schemaUri))
        } else {
            eventSchemaLoader.getSchema(URI.create(schemaUri))
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

    /**
     * Get's the table's location path via the Spark catalog API.
     * @param spark
     * @param tableName Qualified table name, e.g. db.table
     * @return
     */
    def tableLocation(spark: SparkSession, tableName: String): String = {
        val Array(database, table) = tableName.split("\\.")
        val tableIdentifier = new TableIdentifier(table, Some(database))
        spark.sessionState.catalog.getTableMetadata(tableIdentifier).location.toString
    }
}