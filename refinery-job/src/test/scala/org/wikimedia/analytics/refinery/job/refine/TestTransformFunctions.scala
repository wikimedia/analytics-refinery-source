package org.wikimedia.analytics.refinery.job.refine

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.core.HivePartition
import org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame

import scala.collection.immutable.ListMap


/**
  * Created by otto on 201802/09/.
  */

case class SubObject(
    id: String = "12df525497025713b1edb4f51982e05d"
)

case class TestData(
    greeting: String = "hi",
    uuid: String = "2a9de856da6a5fdc82a7f19f2b5cf99c",
    meta: SubObject = SubObject(),
    //IPv4 addresses taken from MaxMind's test suite
    ip: String = "81.2.69.160",
    tags: Array[String] = Array("tag1", "tag2")
)

case class WebrequestSubsetTags(
    webrequest_tags: Array[String],
    webrequest_subset: String
)

class TestTransformFunctions extends FlatSpec
    with Matchers with DataFrameSuiteBase {

    val data1 = TestData()
    val data2 = TestData(uuid="b0f7848f973f5e90ac531055b45bfeba")
    val data3 = TestData(meta=SubObject(id="f6770c245746515089a6237f9fa6536b"))

    val fakeHivePartition = new HivePartition(database = "testDb", t = "testTable", location = "/fake/location", ListMap("year" -> Some("2018")))

    it should "deduplicate_eventlogging based on uuid" in {
        val partDf = new PartitionedDataFrame(spark.createDataFrame(sc.parallelize(Seq(data1, data1, data2))), fakeHivePartition)
        val transformedDf = deduplicate_eventlogging(partDf)
        transformedDf.df.count should equal(2)
    }

    it should "deduplicate_eventbus based on meta.id" in {
        val partDf = new PartitionedDataFrame(spark.createDataFrame(sc.parallelize(Seq(data1, data3, data3))), fakeHivePartition)
        val transformedDf = deduplicate_eventbus(partDf)
        transformedDf.df.count should equal(2)
    }

    it should "geocode_ip `ip` field" in {
        val partDf = new PartitionedDataFrame(spark.createDataFrame(sc.parallelize(Seq(data1))), fakeHivePartition)
        val transformedDf = geocode_ip(partDf)
        transformedDf.df.columns.contains("geocoded_data") should equal(true)

        transformedDf.df.select("geocoded_data.continent").take(1).head.getString(0) should equal("Europe")
    }

    it should "join a dataframe to webrequest_subset_tags on tags" in {
        val subsetTags = Seq(
            WebrequestSubsetTags(Array("tag1"), "subset1"),
            WebrequestSubsetTags(Array("tag3"), "subset2"),
            WebrequestSubsetTags(Array("tag1", "tag2"), "subset3")
        )
        spark.createDataFrame(sc.parallelize(subsetTags)).createOrReplaceTempView("test_subset_tags")
        val partDf = new PartitionedDataFrame(spark.createDataFrame(sc.parallelize(Seq(data1))), fakeHivePartition)
        val pws = partition_webrequest_subset
        pws.webrequestSubsetTagsTable = "test_subset_tags"
        val transformedDf = pws(partDf)
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
