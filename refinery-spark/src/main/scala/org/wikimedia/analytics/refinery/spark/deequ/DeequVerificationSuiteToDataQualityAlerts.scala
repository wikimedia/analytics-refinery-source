package org.wikimedia.analytics.refinery.spark.deequ

import com.amazon.deequ.checks.{Check, CheckResult}
import com.amazon.deequ.constraints.{ConstraintResult, ConstraintStatus}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, TimestampType}
import org.wikimedia.analytics.refinery.core.HivePartition

import java.sql.Timestamp



/** Exports a Deequ validation suite (e.g. a collection of data checks and their validation
 *  results) to the a SparkDataFrame or text. This can be used for alerting and logging.
 *
 * verificationSute is a Map[Check, CheckResult]. Both are Deequ types. `Check` is the declaration of a constraint,
 * including its severity value. `CheckResult` contains the result of `Check`s applied to a target dataset.
 *
 * We need both to determine an alert's security level, as well as to extract the constraint verification result.
 *
 * @param verificationSuite a Deequ VerificationSuite result
 * @param partition an HivePartition
 * @param runId unique identifier for this verification suite. typically is a UUID generated by a job orchestrator.
 */
class DeequVerificationSuiteToDataQualityAlerts(spark: SparkSession)(verificationSuite: Map[Check, CheckResult],
                                                                     partition: HivePartition,
                                                                     runId: String){

    /**
     * A convenience class to manipulate Deequ verification suites.
     *
     * We use to make access to Map[Check, CheckResult] key/values more explicit in
     * SerDe operations.
     */
    private case class Constraint(check: Check, result: ConstraintResult)

    // flatten a Map[Check, CheckResult] in a sequence of (Check, CheckResult) pairs,
    // aliased by a Constraint class.
    private val constraints: Seq[Constraint] = verificationSuite
      .flatMap { case (check, checkResult) =>
          checkResult.constraintResults.map(constraint => Constraint(check, constraint))
      }.toSeq
    private object ColumnNames {
        val SourceTable = "table"
        val PipelineRunId = "pipeline_run_id"
        val PartitionId = "partition_id"
        val PartitionTimestamp = "partition_ts"
        val Value = "value"
        val Status = "status"
        val SeverityLevel = "severity_level"
        val ConstraintId = "constraint"
        val ErrorMessage = "error_message"
    }

    /**
     * Recommended columns to partition by if you are inserting into a Table.
     */
    val partitionColumnNames: Seq[String] = Seq(ColumnNames.PartitionTimestamp)

    /**
     * Constraint result logging schema.
     */
    val outputSchema = StructType(Seq(
        StructField(ColumnNames.SourceTable, StringType)
          .withComment("The table metrics are computed on."),
        StructField(ColumnNames.PartitionId, StringType)
          .withComment(f"Identifier of the partition of ${ColumnNames.SourceTable}" +
            f"the metrics are computed on. e.g. year=2024/month=1/day=1."),
        StructField(ColumnNames.PartitionTimestamp, TimestampType)
          .withComment(f"A Timestamp representation of ${ColumnNames.PartitionId}"),
        StructField(ColumnNames.Status, StringType)
          .withComment("AWS Deequ validation suite: constraint check status. e.g. one of Success or Failure"),
        StructField(ColumnNames.SeverityLevel, StringType)
          .withComment("AWS Deequ validation suite: constraint check severity level." +
            "e.g. one of Error or Warning"),
        StructField(ColumnNames.Value, DoubleType)
          .withComment("AWS Deequ validation suite: constraint value"),
        StructField(ColumnNames.ConstraintId, StringType)
          .withComment("AWS Deequ validation suite: identifier of this constraint type." +
            "e.g. MaximumConstraint(...) "),
        StructField(ColumnNames.ErrorMessage, StringType)
          .withComment("AWS Deequ validation suite: error message generated by deequ. "),
        StructField(ColumnNames.PipelineRunId, StringType)
          .withComment ("A unique identifier of the orchestrator that generated the metric." +
            "e.g. this could an Airflow run_id.")
    ))

    private val rows: Seq[Row] = for {
        constraint <- constraints
    } yield {
        Row(
            partition.tableName,
            partition.relativePath,
            new Timestamp(partition.dt.get.getMillis),
            constraint.result.status.toString,
            // Severity level of the Check. Can be "Warning" or "Error".
            // It will be set both for Success and Failure statuses.
            constraint.check.level.toString,
            constraint.result.metric.get.value.getOrElse(null),
            constraint.result.constraint.toString,
            constraint.result.message.orNull,
            runId
        )
    }

    // Default: store data in the user's database and home dir.
    // In production the database is configured by the job orchestrator.
    // TODO: maybe default behaviour should be to log alerts, instead of writing them to iceberg/hdfs. We could trigger writes
    // only if table / path are provided by users.
    private val AlertOutputTable: String = "dataquality_alerts"
    private val AlertOutputPath: String = s"dataquality_alerts_${partition.tableName}_${partition.relativePath}"

    /**
     * Returns Deequ constraint checks as a Spark DataFrame.
     *
     * @return
     */
    def getAsDataFrame: DataFrame = {
        spark.createDataFrame(
            spark.sparkContext.parallelize(rows),
            outputSchema
        )
    }

    /**
     * Returns Deequ constraint checks as plain text.
     *
     * By default, returns both success and failures. A `filterPredicate`  can be passed
     * to select targets.
     *
     * @param filterPredicate
     * @param delimiter
     * @return
     */
    def getText(filterPredicate: ConstraintResult => Boolean = _ => true, delimiter: String = "\n" ): String =
        constraints
          .filter { constraint => filterPredicate(constraint.result) }
          .map { constraint =>
              s"""
                  |table=${partition.tableName}
                  |status=${constraint.result.status}
                  |severity_level=${constraint.check.level.toString}
                  |partition=${partition.relativePath}
                  |constraint=${constraint.result.constraint}
                  |value=${constraint.result.metric.get.value.get}
                  |message=${constraint.result.message.orNull}
                  |run_id=${runId}""".stripMargin
          }.mkString(delimiter)

    /**
     * Returns Deequ constraint checks failures as plain text.
     * @return
     */
    def getFailureAsText: String = getText(_.status != ConstraintStatus.Success)

    /**
     * Return an IcebergWriter instance
     *
     * @return
     */
    private val writer: DataQualitytWriter = DataQualitytWriter(
        IcebergWriter(AlertOutputTable, getAsDataFrame, partitionColumnNames),
        HdfsTextWriter(AlertOutputPath, getFailureAsText, spark))

    /**
     * Returns a lazy writer for this class.
     * Allows for overriding default configs (output table, output path).
     *
     * */
    def write: DataQualitytWriter = this.writer
}

object DeequVerificationSuiteToDataQualityAlerts {
    def apply(spark: SparkSession)(
               constraints: Map[Check, CheckResult],
               partition: HivePartition,
               runId: String
             ) = new DeequVerificationSuiteToDataQualityAlerts(spark)(constraints, partition, runId)
}