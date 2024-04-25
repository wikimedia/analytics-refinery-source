package org.wikimedia.analytics.refinery.job.mediawikihistory

import com.amazon.deequ.analyzers.Compliance
import com.amazon.deequ.analyzers.runners.AnalysisRunner
import org.apache.spark.sql.DataFrame

/**
 * trait defining a methods for performing
 * analysis on the column of a dataframe
 */
trait DeequColumnAnalysis {

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

        // flatten to get metric as Double
        userErrorAnalysisResult
          .metricMap(compliance)
          .flatten()
          .map(_.value.get)
          .head
    }

}
