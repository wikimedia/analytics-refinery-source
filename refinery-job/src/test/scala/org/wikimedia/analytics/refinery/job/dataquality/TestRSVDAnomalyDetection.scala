package org.wikimedia.analytics.refinery.job.dataquality

import com.criteo.rsvd._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}

class TestRSVDAnomalyDetection extends FlatSpec with Matchers with DataFrameSuiteBase {

    it should "correctly transform a time series to a block matrix" in {
        val rsvd = new RSVDAnomalyDetection(spark, RSVDAnomalyDetection.Params())

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
        val rsvd = new RSVDAnomalyDetection(spark, RSVDAnomalyDetection.Params())

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

    it should "fill in gaps in incomplete time series" in {
        val gapsTimeSeries = Array(
            ("2020-01-01T00:00:00Z", 1.0),
            ("2020-01-02T00:00:00Z", 2.0),
            ("2020-01-04T00:00:00Z", 3.0)
        )
        val params = RSVDAnomalyDetection.Params(
            granularity = Granularity.withName("daily"),
            lastDataPointDt = "2020-01-05T00:00:00Z",
            defaultFiller = 0.0
        )
        val rsvd = new RSVDAnomalyDetection(spark, params)

        val filledTimeSeries = rsvd.fillInGaps(gapsTimeSeries)

        assert(filledTimeSeries.length == 5)
        assert(filledTimeSeries(0) == ("2020-01-01T00:00:00Z", 1.0))
        assert(filledTimeSeries(1) == ("2020-01-02T00:00:00Z", 2.0))
        assert(filledTimeSeries(2) == ("2020-01-03T00:00:00Z", 0.0))
        assert(filledTimeSeries(3) == ("2020-01-04T00:00:00Z", 3.0))
        assert(filledTimeSeries(4) == ("2020-01-05T00:00:00Z", 0.0))
    }

    it should "not analyze too short timeseries" in {
        val shortTimeSeries = Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0)
        val params = RSVDAnomalyDetection.Params(
            granularity = Granularity.withName("daily"),
            defaultFiller = 0.0,
            maxSparsity = 0.2,
            rsvdDimensions = 1,
            rsvdOversample = 2
        )
        val rsvd = new RSVDAnomalyDetection(spark, params)
        // time series length is 8, but should be 9 or more
        val canBeAnalyzed = rsvd.timeSeriesCanBeAnalyzed(shortTimeSeries)
        assert(canBeAnalyzed == false)
    }

    it should "not analyze too sparse timeseries" in {
        val shortTimeSeries = Array(1.0, 0.0, 3.0, 4.0, 5.0, 0.0, 7.0, 8.0, 9.0)
        val params = RSVDAnomalyDetection.Params(
            granularity = Granularity.withName("hourly"),
            defaultFiller = 0.0,
            maxSparsity = 0.2,
            rsvdDimensions = 1,
            rsvdOversample = 2
        )
        val rsvd = new RSVDAnomalyDetection(spark, params)
        // time series sparsity is 0.22, but should be 0.2 or less
        val canBeAnalyzed = rsvd.timeSeriesCanBeAnalyzed(shortTimeSeries)
        assert(canBeAnalyzed == false)
    }
}
