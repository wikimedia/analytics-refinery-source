package org.wikimedia.analytics.refinery.spark.deequ

import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.analyzers.{ApproxCountDistinct, Size}
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.metrics.Entity.{Column, Dataset}
import com.amazon.deequ.repository.{AnalysisResult, ResultKey}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.scalatest.FlatSpec
import org.wikimedia.analytics.refinery.core.HivePartition

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}
import scala.collection.immutable.ListMap
import scala.util.Success

class TestDeequAnalyzersToDataQualityMetrics extends FlatSpec with DataFrameSuiteBase {
    private val dataSetDate = 1701081890176L
    private val dataSetTags = Map("key1" -> "val1", "key2" -> "val2")

    private val analysisResult = new AnalysisResult(
        ResultKey(dataSetDate, dataSetTags),
        new AnalyzerContext(
            Map(
                Size(None) -> DoubleMetric( // count(*)
                    Dataset,
                    "Size",
                    "*",
                    Success(100000.0)
                ),
                ApproxCountDistinct("col1", None) -> DoubleMetric(
                    Column,
                    "ApproxCountDistinct",
                    "col1",
                    Success(100000.0)
                ),
                ApproxCountDistinct("col2", None) -> DoubleMetric(
                    Column,
                    "ApproxCountDistinct",
                    "col2",
                    Success(111.0)
                )
            )
        )
    )
    private val results: Seq[AnalysisResult] = Seq(analysisResult)

    it should "be possible to export metrics to Wikimedia's Data Quality Metrics schema " in {
        val tableName = "testDataset" // source table name
        val runId = "someId" // airflow (or other orchestrator) pipeline run id

        val partition = new HivePartition(
            "database_name", tableName, None, ListMap(
                "source_key" -> Some("testDataset"),
                "year" -> Some("2023"),
                "month" -> Some("11"),
                "day" -> Some("7"),
                "hour" -> Some("0")
            )
        )

        val partitionId = partition.relativePath
        val partitionTs = new Timestamp(
            LocalDateTime.of(2023, 11, 7, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli
        )

        val metrics = new DeequAnalyzersToDataQualityMetrics(spark)(results, partition, runId)
        val expectedMetricRows = Seq(
            Row(
                dataSetDate,
                dataSetTags,
                "Dataset",
                "*",
                100000.0,
                "Size",
                partition.tableName,
                runId,
                partitionId,
                partitionTs
            ),
            Row(
                dataSetDate,
                dataSetTags,
                "Column",
                "col1",
                100000.0,
                "ApproxCountDistinct",
                partition.tableName,
                runId,
                partitionId,
                partitionTs
            ),
            Row(
                dataSetDate,
                dataSetTags,
                "Column",
                "col2",
                111.0,
                "ApproxCountDistinct",
                partition.tableName,
                runId,
                partitionId,
                partitionTs
            )
        )
        val expectedMetricsDataFrame = spark.createDataFrame(
            sc.parallelize(expectedMetricRows),
            metrics.outputSchema
        )
        val metricsDataFrame = metrics.getAsDataFrame

        assertDataFrameEquals(expectedMetricsDataFrame, metricsDataFrame)

        val expectedOutputTable = "someOtherOutputTable"
        val writer = metrics.write.iceberg.output(expectedOutputTable)
        assert(writer.output == expectedOutputTable)
    }
}
