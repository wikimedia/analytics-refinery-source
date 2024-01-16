package org.wikimedia.analytics.refinery.spark.deequ

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, MapType, StringType, StructField, StructType, TimestampType}
import com.amazon.deequ.repository.AnalysisResult
import org.wikimedia.analytics.refinery.core.HivePartition

import java.sql.Timestamp


/** Exports a Deequ AnalysisResult (e.g. a repository as list of metrics) to
  * the common Wikimedia Data Quality Metrics table format.
  *
  *  This code is WIP/experimental and subject to change. If finalized,
  *  it should move to a dedicated namespace.
 *
  * @param analyzers
  * @param datasetName
  * @param partition
  * @param runId
  */
class DeequAnalyzersToDataQualityMetrics(spark: SparkSession)(
    analyzers: Seq[AnalysisResult],
    partition: HivePartition,
    runId: String
){
    // Default: store data in the user's database.
    // In production the database is configured by the job orchestrator.
    private val MetricsOutputTable: String = "dataquality_metrics"

    private object WikimediaColumns {
        val SourceTable = "source_table"
        val PipelineRunId = "pipeline_run_id"
        val PartitionId = "partition_id"
        val PartitionTimestamp = "partition_ts"
    }

    /**
     * Naming convention enforce by Deequ. We keep it for compatibility,
     * and to be explicit to Deequ end users.
     */
    private object DeequColumns {
        val DatasetDate = "dataset_date"
        val Tags = "tags"
        val Instance = "instance"
        val Name = "name"
        val Entity = "entity"
        val Value = "value"
    }

    /**
     * Recommended columns to partition by if you are inserting into a Table.
     */
    val partitionColumnNames: Seq[String] = Seq(WikimediaColumns.PartitionTimestamp)

    /**
      *  Wikimedia's Data Quality Metrics data model.
      */
    val outputSchema: StructType =
        StructType( // TODO: semantics. Fields should be non nullable?
            Seq(
                StructField(DeequColumns.DatasetDate, LongType, nullable = true)
                  .withComment("AWS Deequ metric repo: key insertion time"),
                StructField(
                    DeequColumns.Tags,
                    MapType(StringType, StringType, valueContainsNull = true),
                    nullable = true
                ).withComment("AWS Deequ metric repo: key tags"),
                StructField(DeequColumns.Entity, StringType, nullable = true)
                  .withComment("AWS Deequ metric repo: the type of entity " +
                    "the metric is recorded against." +
                    "e.g. A column, dataset, or multicolumn."),
                StructField(DeequColumns.Instance, StringType, nullable = true)
                  .withComment("AWS Deequ repo: information about this instance of the metric." +
                    "For example this could be the column name the metric is" +
                    "operating on."),
                StructField(DeequColumns.Value, DoubleType, nullable = true)
                  .withComment("AWS Deequ repo: the value of the metric at a point in time."),
                StructField(DeequColumns.Name, StringType, nullable = true)
                  .withComment("AWS Deequ repo: the name for the type of metric."),
                StructField(WikimediaColumns.SourceTable, StringType, nullable = true)
                  .withComment("The table metrics are computed on."),
                StructField(
                    WikimediaColumns.PipelineRunId,
                    StringType,
                    nullable = true
                ).withComment("A unique identifier of the orchestrator that generated the metric." +
                  "e.g. this could an Airflow run_id."),
                StructField(WikimediaColumns.PartitionId, StringType, nullable = true)
                  .withComment(f"Identifier of the partition of ${WikimediaColumns.SourceTable}" +
                    f"the metrics are computed on. e.g. year=2024/month=1/day=1."),
                StructField(
                    WikimediaColumns.PartitionTimestamp,
                    TimestampType,
                    nullable = true
                ).withComment(f"A Timestamp representation of ${WikimediaColumns.PartitionId}")
            )
        )

    private val metricsAsRows: Seq[Row] = for {
        analyzer <- analyzers
            resultKey = analyzer.resultKey
            deequMetric <- analyzer.analyzerContext.allMetrics
    } yield {
        Row(
            resultKey.dataSetDate,
            resultKey.tags,
            deequMetric.entity.toString, // E.g. Column,
            deequMetric.instance, // E.g. column_name,
            deequMetric.value.getOrElse(null), // TODO: Fields should be non nullable?
            deequMetric.name, // E.g. ApproxCountDistinct
            partition.tableName,
            runId,
            partition.relativePath,
            new Timestamp(partition.dt.get.getMillis)
        )
    }

    /**
      * Returns deequ metrics as a Spark DataFrame with
      * Wikimedia's Data Quality Metrics schema.
      *
      * @param spark
      * @return
      */
    def getAsDataFrame: DataFrame = {
        spark.createDataFrame(
            spark.sparkContext.parallelize(metricsAsRows),
            outputSchema
        )
    }



    // write only to iceberg targers
    private val writer: DataQualitytWriter = DataQualitytWriter(IcebergWriter(MetricsOutputTable, getAsDataFrame, partitionColumnNames))

    /**
     * Returns a lazy writer for this class.
     * Allows for overriding default configs (output table, output path).
     *
     * */
    def write: DataQualitytWriter = writer
}

object DeequAnalyzersToDataQualityMetrics {
    def apply(spark: SparkSession)(
        analyzers: Seq[AnalysisResult],
        partition: HivePartition,
        runId: String
    ) = new DeequAnalyzersToDataQualityMetrics(spark)(analyzers, partition, runId)
}
