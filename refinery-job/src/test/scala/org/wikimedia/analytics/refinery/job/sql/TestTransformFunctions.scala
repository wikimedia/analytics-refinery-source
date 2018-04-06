package org.wikimedia.analytics.refinery.job.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.spark.sql.HivePartition

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
    ip: String = "81.2.69.160"
)

class TestTransformFunctions extends FlatSpec
    with Matchers with DataFrameSuiteBase {

    val data1 = TestData()
    val data2 = TestData(uuid="b0f7848f973f5e90ac531055b45bfeba")
    val data3 = TestData(meta=SubObject(id="f6770c245746515089a6237f9fa6536b"))

    val partition: HivePartition = HivePartition(
        "my_db", "my_table", "/path/to/table", ListMap("year" -> "2018")
    )

    it should "deduplicate_eventlogging based on uuid" in {
        val df = spark.createDataFrame(sc.parallelize(Seq(data1, data1, data2)))
        val transformedDf = deduplicate_eventlogging(df, partition)
        transformedDf.count should equal(2)
    }

    it should "deduplicate_eventbus based on meta.id" in {
        val df = spark.createDataFrame(sc.parallelize(Seq(data1, data3, data3)))
        val transformedDf = deduplicate_eventbus(df, partition)
        transformedDf.count should equal(2)
    }

    it should "geocode_ip `ip` field" in {
        val df = spark.createDataFrame(sc.parallelize(Seq(data1)))
        val transformedDf = geocode_ip(df, partition)
        transformedDf.columns.contains("geocoded_data") should equal(true)

        transformedDf.select("geocoded_data.continent").take(1).head.getString(0) should equal("Europe")
    }

}
