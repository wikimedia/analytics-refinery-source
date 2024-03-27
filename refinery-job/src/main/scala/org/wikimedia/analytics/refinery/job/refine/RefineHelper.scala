package org.wikimedia.analytics.refinery.job.refine

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.wikimedia.analytics.refinery.job.refine.RefineHiveDataset.log
import org.wikimedia.analytics.refinery.core.HivePartition
import org.wikimedia.analytics.refinery.job.refine.EventSparkSchemaLoader.BASE_SCHEMA_URIS_DEFAULT
import org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame

import scala.collection.immutable.ListMap

object RefineHelper {

    // Type for transform functions processing partitions to refine.
    type TransformFunction = PartitionedDataFrame => PartitionedDataFrame

    /**
     * Reads input DataFrames from specified paths
     *
     * This method utilizes `RefineTarget` for each input path to read data frames according to the specified schema
     * URI, input format, and reader options. It then merges these data frames into one.
     *
     * @param spark SparkSession The Spark session to use for data frame operations.
     * @param inputPaths Seq[String] A sequence of strings representing the paths to input data.
     * @param inputSchemaURI String The URI of the input schema to apply to the data frames.
     * @param inputFormat String (default "JSON") The format of the input data. Defaults to JSON.
     * @param dataframeReaderOptions Map[String, String] Options to pass to the DataFrameReader.
     * @param inputSchemaBaseURIs Seq[String] Base URIs for schema loading.
     * @param corruptRecordFailureThreshold Integer The threshold for failing on corrupt records.
     * @return DataFrame A merged data frame from all input paths, with the specified schema and options applied.
     */
    def readInputDataFrame(
        spark: SparkSession,
        inputPaths: Seq[String],
        inputSchemaURI: String,
        inputFormat: String = "JSON",
        inputSchemaLoader: Option[SparkSchemaLoader] = None,
        dataframeReaderOptions: Map[String, String] = Map(),
        inputSchemaBaseURIs: Seq[String] = BASE_SCHEMA_URIS_DEFAULT,
        corruptRecordFailureThreshold: Integer = 1
    ): DataFrame = {
        log.info(s"Reading schema from $inputSchemaURI with format $inputFormat")
        val loader = if (inputSchemaLoader.isDefined) inputSchemaLoader.get
            else EventSparkSchemaLoader(inputSchemaBaseURIs)

        val refineTargets = inputPaths.map(inputPath => new RefineTarget(
            spark,
            new Path(inputPath),
            HivePartition("a/b/c/d/p=0"),  // We are only using RefineTarget for input here
            schemaLoader = loader,
            inputFormatOpt = Some(inputFormat),
            providedDfReaderOptions = dataframeReaderOptions,
        ))

        refineTargets
            .map(Refine.getInputPartitionedDataFrame(_, corruptRecordFailureThreshold).df)
            .reduce(_ union _)
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
     * @param table The name of the table, used within transformation functions for context or metadata.
     * @param transformFunctions A sequence of functions that take a DataFrame and return a modified DataFrame.
     * @return A DataFrame that has been transformed by the provided sequence of functions.
     */
    def applyTransforms(
        schema: StructType,
        spark: SparkSession,
        table: String,
        transformFunctions: Seq[TransformFunction]
    ): StructType = {
        val df: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
        applyTransforms(df, table, transformFunctions).schema
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
     * @param table The name of the table, used to create a fake HivePartition for context in transformation functions.
     * @param transformFunctions A sequence of functions that take a PartitionedDataFrame and return a modified PartitionedDataFrame.
     * @return A DataFrame that has been transformed by the provided sequence of functions.
     */
    def applyTransforms(
        dataframe: DataFrame,
        table: String,
        transformFunctions: Seq[TransformFunction]
    ): DataFrame = {
        // Creating a partitioned-dataframe with no partition to abuse transform-functions
        val fakeHivePartition = HivePartition(table, ListMap.empty[String, String]).copy(location = Some(""))
        val inputPartDf = new PartitionedDataFrame(dataframe, fakeHivePartition)
        transformFunctions
            .foldLeft(inputPartDf)((currPartDf, fn) => fn(currPartDf))
            .df
    }
}