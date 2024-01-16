package org.wikimedia.analytics.refinery.spark.deequ;

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FlatSpec
import org.wikimedia.analytics.refinery.core.HivePartition

import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}
import scala.collection.immutable.ListMap

class TestDeequVerificationSuiteToDataQualityAlerts extends FlatSpec with DataFrameSuiteBase {

    it should "be possible to generate alerts on Wikimedia's Data Quality Metrics " in {

        val schema = StructType(
            Seq(
                StructField("id", IntegerType, nullable = false),
                StructField("productName", StringType, nullable = true),
                StructField("description", StringType, nullable = true),
                StructField("priority", StringType, nullable = true),
                StructField("numViews", IntegerType, nullable = false)
            )
        )

        val rdd = spark.sparkContext.parallelize(Seq(
            Row(1, "Thingy A", "awesome thing.", "high", 0),
            Row(2, "Thingy B", "available at http://thingb.com", null, 0),
            Row(3, null, null, "low", 5),
            Row(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
            Row(5, "Thingy E", null, "high", 12)))

        val data = spark.createDataFrame(rdd, schema)

        val verificationResult = VerificationSuite()
          .onData(data)
          .addCheck(Check(CheckLevel.Error, "unit testing data")
            .hasSize(_ == 6))// we expect 6 rows, but got 5. Fire an alert.
          .run()

        val resultsForAllConstraints = verificationResult.checkResults

        val tableName = "testDataset" // source table name
        val runId = "someId" // airflow (or other orchestrator) pipeline run id

        val partition = new HivePartition(
            "database_name", tableName, None, ListMap(
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

        val status = "Failure"
        val severityLevel = "Error"
        val value = 5.0
        val constraintId = "SizeConstraint(Size(None))"
        val errorMessage = "Value: 5 does not meet the constraint requirement!"
        val outputPath = "/some/path"

        val expectedAlertRows = Seq(
          Row(
              partition.tableName,
              partitionId,
              partitionTs,
              status,
              severityLevel,
              value,
              constraintId,
              errorMessage,
              runId
          )
        )

        val expectedAlertText =
            s"""
                |table=${partition.tableName}
                |status=${status}
                |severity_level=${severityLevel}
                |partition=${partition.relativePath}
                |constraint=${constraintId}
                |value=${value}
                |message=${errorMessage}
                |run_id=${runId}""".stripMargin

        val alerts = DeequVerificationSuiteToDataQualityAlerts(spark)(resultsForAllConstraints, partition, runId)
        val expectedAlertDataFrame = spark.createDataFrame(spark.sparkContext.parallelize(expectedAlertRows), alerts.outputSchema)

        assertDataFrameEquals(expectedAlertDataFrame, alerts.getAsDataFrame)
        val writer = alerts.write.text.output(outputPath)
        assert(writer.output == outputPath)
        assert(alerts.getFailureAsText.mkString == expectedAlertText)
    }
}
