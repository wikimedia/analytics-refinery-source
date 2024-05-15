package org.wikimedia.analytics.refinery.job.mediawikihistory

import com.amazon.deequ.analyzers.Compliance
import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.metrics.DoubleMetric
import org.apache.spark.sql.DataFrame
import org.wikimedia.analytics.refinery.tools.LogHelper

import scala.util.{Failure, Success, Try}

/**
 * trait defining a methods for performing
 * analysis on the column of a dataframe
 */
trait DeequColumnAnalysis extends LogHelper{

    /**
     *
     * @param data Dataframe to perform analysis on.
     * @param sqlPredicate sql condition to apply per row
     * @param instance Description of what the analysis is done for.
     * @param colName List of columns to run pre condition check!
     * @return Double Metric value. This is the ratio of rows that satisfies
     *         the sqlPredicate condition.
     */
    def columnComplianceAnalysis(data: DataFrame,
                                 sqlPredicate: String,
                                 instance: String,
                                 colName: List[String]=List("growths")
                                ): Double = {

        // Check for only one metric: Compliance
        // The Analysis Result is expected to only have one value
        // Since only one metric(Compliance) is computed.
        val compliance = Compliance(
            instance,
            sqlPredicate,
            None,
            colName
        )
        val userErrorAnalysisResult = AnalysisRunner
          .onData(data)
          .addAnalyzer(compliance)
          .run()

        // Deequ is a bit loose with types.
        // userErrorAnalysisResult.metric(compliance) returns an instance of Metric[_],
        // which in case of Compliance analyzers is expected to be a DoubleMetric.
        // collect() the metric and type check to guard against runtime errors.
        // The nested Try[Try[Double]] is a bit verbose, so we flatten it to simplify
        // pattern matching
        val metric: Try[Double] = Try {
            userErrorAnalysisResult
              .metric(compliance)
              .collect {
                  case doubleMetric: DoubleMetric => doubleMetric.value // Not exhaustive, but we only support DoubleMetric instances.
              }
              .getOrElse(throw new NoSuchElementException("Metric not found or not a DoubleMetric for Compliance analyzer"))
        }.flatten

        metric match {
            case Success(value) => value
            case Failure(err) => throw new RuntimeException(s"Error occurred while fetching metric: ${err.getMessage}", err)
        }
    }
}
