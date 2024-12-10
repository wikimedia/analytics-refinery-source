package org.wikimedia.analytics.refinery.job.mediawikidumper

import java.sql.Timestamp

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.scalatest.MustMatchers.convertToAnyMustWrapper
import org.wikimedia.analytics.refinery.tools.LogHelper

class RowKeySpec
    extends FlatSpec
    with Matchers
    with BeforeAndAfterEach
    with DataFrameSuiteBase
    with LogHelper {

    val baselineTime: Long = 1740783600L

    val baseline: RowKey = {
        new RowKey(pageId = 100, new Timestamp(baselineTime), revisionId = 1000)
    }

    private val lt: Seq[RowKey] = Seq(
      RowKey(99, new Timestamp(baselineTime), 1000),
      RowKey(100, new Timestamp(baselineTime - 1), 1000),
      RowKey(100, new Timestamp(baselineTime), 999)
    )

    private val gt: Seq[RowKey] = Seq(
      RowKey(101, new Timestamp(baselineTime), 1000),
      RowKey(100, new Timestamp(baselineTime + 1), 1000),
      RowKey(100, new Timestamp(baselineTime), 1001)
    )

    lt.foreach { case (key) =>
        s"$key" should s"should be lower than reference" in {
            key must be < (baseline)
        }
        "baseline" should s"should be greater than $key" in {
            baseline must be > (key)
        }
    }

    gt.foreach { case (key) =>
        s"$key" should s"should be greater than reference" in {
            key must be > (baseline)
        }
        "baseline" should s"should be less than $key" in {
            baseline must be < (key)
        }
    }

}
