package org.wikimedia.analytics.refinery.job.dataquality

import com.criteo.rsvd._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}

class TestRSVDAnomalyDetection extends FlatSpec with Matchers with DataFrameSuiteBase {

    it should "correctly transform a time series to a block matrix" in {
        val rsvd = new RSVDAnomalyDetection(spark)

        val timeSeries = Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0)
        val matrixHeight = 3
        val matrix = rsvd.timeSeriesToBlockMatrix(
            timeSeries,
            matrixHeight,
            RSVDConfig()
        ).toLocalMatrix

        for (i <- 0 to (matrixHeight - 1)) {
            for (j <- 0 to (timeSeries.size / matrixHeight - 1)) {
                assert(matrix(i, j) == timeSeries(j * matrixHeight + i))
            }
        }
    }

    it should "correctly get last data point deviation" in {
        val rsvd = new RSVDAnomalyDetection(spark)

        // Regular case.
        val timeSeries1 = Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0)
        val deviation1 = rsvd.getLastDataPointDeviation(timeSeries1)
        assert(deviation1 == 2.0)

        // Last data point equals median case.
        val timeSeries2 = Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 6.0)
        val deviation2 = rsvd.getLastDataPointDeviation(timeSeries2)
        assert(deviation2 == 0.0)

        // Deviation unit equals zero case.
        val timeSeries3 = Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 6.0, 6.0, 6.0, 6.0, 6.0, 7.0)
        val deviation3 = rsvd.getLastDataPointDeviation(timeSeries3)
        assert(deviation3 == 100.0)
    }
}
