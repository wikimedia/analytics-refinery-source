package org.wikimedia.analytics.refinery.job.refine

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.wikimedia.analytics.refinery.core.HivePartition
import org.wikimedia.analytics.refinery.tools.LogHelper

case class RawRefineDataReader(spark: SparkSession,
                               sparkSchema: StructType,
                               inputFormat: String = "json",
                               dataframeReaderOptions: Map[String, String] = Map(),
                               corruptRecordFailureThreshold: Integer = 1,
                               ignoreMissingPaths: Boolean = false) extends LogHelper {

    /**
     * Get the table's location path via the Spark catalog API.
     *
     * @param tableName Qualified table name, e.g. db.table
     */
    def tableLocation(tableName: String): String = {
        val Array(database, table) = tableName.split("\\.")
        val tableIdentifier = new TableIdentifier(table, Some(database))
        spark.sessionState.catalog.getTableMetadata(tableIdentifier).location.toString
    }

    /**
     * Reads an inputDataFrame using the schema loaded by searching
     * inputSchemaBaseURIs for inputSchemaURI.
     *
     * @param inputPaths Seq[String] A sequence of strings representing the paths to input data.
     */
    def readInputDataFrameWithSchemaURI(inputPaths: Seq[String]): DataFrame = {
        // We've already looked up the schema from the inputSchemaURI,
        // so use an ExplicitSchemaLoader to always return that schema.
        val explicitSparkSchemaLoader = ExplicitSchemaLoader(Some(sparkSchema))

        log.info(s"Reading schema from $explicitSparkSchemaLoader with format $inputFormat")

        val refineTargets = inputPaths.map(inputPath => new RefineTarget(
            spark,
            new Path(inputPath),
            HivePartition("a/b/c/d/p=0"),  // We are only using RefineTarget for input here
            schemaLoader = explicitSparkSchemaLoader,
            inputFormatOpt = Some(inputFormat),
            providedDfReaderOptions = dataframeReaderOptions,
        ))

        refineTargets
            .map( refineTarget => {
                if (!refineTarget.inputExists() && ignoreMissingPaths) {
                    log.warn(s"Path ${refineTarget.inputPath} does not exist, ignoring")
                    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], refineTarget.schema.get)
                } else {
                    // Next line will throw an exception if the path does not exist.
                    Refine.getInputPartitionedDataFrame(refineTarget, corruptRecordFailureThreshold).df
                }
            })
            .reduce(_ union _)
    }
}
