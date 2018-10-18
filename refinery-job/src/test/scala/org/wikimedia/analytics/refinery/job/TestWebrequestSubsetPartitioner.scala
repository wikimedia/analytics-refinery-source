package org.wikimedia.analytics.refinery.job

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.core.HivePartition
import org.wikimedia.analytics.refinery.job.refine.TestData
import org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame

import scala.collection.immutable.ListMap


case class WebrequestSubsetTags(
                                 webrequest_tags: Array[String],
                                 webrequest_subset: String
                                 )

class TestWebrequestSubsetPartitioner extends FlatSpec
with Matchers with DataFrameSuiteBase {

  val fakeData = TestData()
  val fakeHivePartition = new HivePartition(database = "testDb", t = "testTable", location = "/fake/location", ListMap("year" -> Some("2018")))

  it should "join a dataframe to webrequest_subset_tags on tags" in {
    val subsetTags = Seq(
      WebrequestSubsetTags(Array("tag1"), "subset1"),
      WebrequestSubsetTags(Array("tag3"), "subset2"),
      WebrequestSubsetTags(Array("tag1", "tag2"), "subset3")
    )
    spark.createDataFrame(sc.parallelize(subsetTags)).createOrReplaceTempView("test_subset_tags")
    val partDf = new PartitionedDataFrame(spark.createDataFrame(sc.parallelize(Seq(fakeData))), fakeHivePartition)
    val transformedDf = WebrequestSubsetPartitioner.joinWebrequestSubsetTags("test_subset_tags")(partDf)
    val transformedValues = transformedDf.df.collect()
    val transformedPartition = transformedDf.partition
    // The single row is has 2 subsets --> it gets duplicated
    transformedValues.length should equal(2)
    // subset is expected to be at 6th position in row
    transformedValues.map(_.getString(5)) should contain("subset1")
    transformedValues.map(_.getString(5)) should contain("subset3")
    // Partition should have 2 elements, subset being last and without value (dynamic)
    transformedPartition.partitions.size should equal(2)
    transformedPartition.partitions("subset") should equal(None)
  }

}
