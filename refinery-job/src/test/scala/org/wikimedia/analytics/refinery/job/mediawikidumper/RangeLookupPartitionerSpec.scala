package org.wikimedia.analytics.refinery.job.mediawikidumper

import scala.language.postfixOps

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.tools.LogHelper

// -Dsuites="org.wikimedia.analytics.refinery.job.mediawikidumper.LookupTreePartitionerSpec"
class RangeLookupPartitionerSpec
    extends FlatSpec
    with Matchers
    with BeforeAndAfterEach
    with DataFrameSuiteBase
    with LogHelper {

    "LookupTreePartitioner" should
        "should assign partitions based on max values" in {
            val partitioner = RangeLookupPartitioner[Int, Int](
              Seq(100, 200, 300),
              (i: Int) => i
            )
            partitioner.getPartition(0) should equal(0)
            partitioner.getPartition(99) should equal(0)
            partitioner.getPartition(101) should equal(1)
        }

    "SubRangeLookupPartitioner" should
        "assign partitions based on sub-ranges if applicable" in {
            val partitioner = RangeLookupPartitioner[R, Int](
              Map(1 -> Seq(10, 20, 30), 5 -> Seq(40, 80)),
              (r: R) => r.r,
              Seq(100, 200, 300),
              (r: R) => r.p
            )
            partitioner.getPartition(R(1)) should equal(0)
            partitioner.getPartition(R(1, 11)) should equal(1)
            partitioner.getPartition(R(1, 21)) should equal(2)
            partitioner.getPartition(R(5, 80)) should equal(4)

            partitioner.getPartition(R(0)) should equal(5)
            partitioner.getPartition(R(100)) should equal(5)
            partitioner.getPartition(R(101)) should equal(6)

        }

}

case class R(p: Int, r: Int = 0) {}
