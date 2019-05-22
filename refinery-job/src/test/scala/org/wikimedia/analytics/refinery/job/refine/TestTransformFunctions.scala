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
    tags: Array[String] = Array("tag1", "tag2"),
    webHost: String = "en.wikipedia.org"
)

class TestTransformFunctions extends FlatSpec
    with Matchers with DataFrameSuiteBase {

    val data1 = TestData()
    val data2 = TestData(uuid="b0f7848f973f5e90ac531055b45bfeba")
    val data3 = TestData(meta=SubObject(id="f6770c245746515089a6237f9fa6536b"))
    val data4 = TestData(webHost="invalid.hostname.org")

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

    it should "filter_out_non_wiki_hostname" in {
        val partDf = new PartitionedDataFrame(
            spark.createDataFrame(
                sc.parallelize(Seq(data2, data3, data4)
            )
        ), fakeHivePartition)
        val transformedDf = filter_out_non_wiki_hostname(partDf)
        transformedDf.df.count should equal(2)
        transformedDf.df.select("webHost").collect.foreach(
            r => r.getString(0) should equal("en.wikipedia.org")
        )
    }
}
