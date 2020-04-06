package org.wikimedia.analytics.refinery.job.refine

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.core.HivePartition
import org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame

import scala.collection.immutable.ListMap

case class MetaSubObject(
    id: String = "0VRKNOINdAWYe5HetukWcxhjLEqb1BoD",
    dt: String = "2020-01-01T00:00:00Z",
    domain: String = "test.wikipedia.org"
)

case class HttpSubObject(
    client_ip: String,
    request_headers: Map[String, String] = Map(
        "user-agent" -> "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:19.0) Gecko/20100101 Firefox/19.0"
    )
)

case class LegacyUserAgentStruct(
    browser_family: String = "-",
    browser_major: String = "-",
    browser_minor: String = "-",
    device_family: String = "-",
    is_bot: String = "-",
    is_mediawiki: String = "-",
    os_family: String = "-",
    os_major: String = "-",
    os_minor: String = "-",
    wmf_app_version: String = "-"
)
case class TestLegacyEventLoggingEvent(
    uuid: String,
    webHost: String,
    ip: String,
    user_agent_header: String = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:19.0) Gecko/20100101 Firefox/19.0",
    greeting: String = "hi",
    tags: Array[String] = Array("tag1", "tag2"),
    useragent: LegacyUserAgentStruct = LegacyUserAgentStruct()
)

case class TestEvent(
    meta: MetaSubObject,
    http: HttpSubObject,
    greeting: String = "hi",
    tags: Array[String] = Array("tag1", "tag2")
)

case class NoWebHostEvent(
    id: String
)

class TestTransformFunctions extends FlatSpec with Matchers with DataFrameSuiteBase {
    val id1 = "IDWONBROLhQYpNlqWiyQosrpfanQx51M"
    val id2 = "Wglv2Jv9hflaFtDdZSNXKFv9WwwoD6gX"

    val dt1 = "2020-04-01T00:00:00Z"
    val dt2 = "2020-04-02T00:00:00Z"

    val internalDomain = "test.wikipedia.org"
    val allowedExternalDomain = "translate.google.com"
    val unallowedExternalDomain = "invalid.hostname.org"
    
    //IPv4 addresses taken from MaxMind's test suite
    val ip1: String = "81.2.69.160"

    val legacyEventLoggingEvent1 = TestLegacyEventLoggingEvent(id1, internalDomain, ip1)
    val legacyEventLoggingEvent2 = TestLegacyEventLoggingEvent(id2, internalDomain, ip1)

    val event1 = TestEvent(
        MetaSubObject(id1, dt1, internalDomain),
        HttpSubObject(ip1)
    )
    val event2 = TestEvent(
        MetaSubObject(id2, dt2, internalDomain),
        HttpSubObject(ip1)
    )

    val fakeHivePartition = new HivePartition(database = "testDb", t = "testTable", location = "/fake/location", ListMap("year" -> Some("2018")))

    it should "deduplicate based on `uuid`" in {
        val events = Seq(
            legacyEventLoggingEvent1, legacyEventLoggingEvent1, legacyEventLoggingEvent2
        )
        val df = spark.createDataFrame(sc.parallelize(events))
        val partDf = PartitionedDataFrame(df, fakeHivePartition)
        val transformedDf = deduplicate(partDf)
        transformedDf.df.count should equal(2)
    }

    it should "deduplicate based on `meta.id`" in {
        val events = Seq(event1, event1, event2)
        val df = spark.createDataFrame(sc.parallelize(events))
        val partDf = PartitionedDataFrame(df, fakeHivePartition)
        val transformedDf = deduplicate(partDf)
        transformedDf.df.count should equal(2)
    }

    it should "filter_allowed_domains using `webHost`" in {
        val events = Seq(
            TestLegacyEventLoggingEvent(id1, internalDomain, ip1),
            TestLegacyEventLoggingEvent(id1, internalDomain, ip1),
            TestLegacyEventLoggingEvent(id1, unallowedExternalDomain, ip1)
        )

        val df = spark.createDataFrame(sc.parallelize(events))
        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
        val transformedDf =  filter_allowed_domains(partDf)
        transformedDf.df.count should equal(2)
        transformedDf.df.select("webHost").collect.foreach(
            r => r.getString(0) should equal(internalDomain)
        )
    }

    it should "filter_allowed_domains using `webHost` set to null" in {
        val events = Seq(
            TestLegacyEventLoggingEvent(id1, null, ip1)
        )
        val df = spark.createDataFrame(sc.parallelize(events))
        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
        val transformedDf =  filter_allowed_domains(partDf)
        transformedDf.df.count should equal(1)
    }

    it should "filter_allowed_domains using `webHost` set to external domain value in whitelist" in {
        val events = Seq(
            TestLegacyEventLoggingEvent(id1, allowedExternalDomain, ip1)
        )
        val df = spark.createDataFrame(sc.parallelize(events))
        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
        val transformedDf =  filter_allowed_domains(partDf)
        transformedDf.df.count should equal(1)
    }

    it should "filter_allowed_domains if `webHost` and `meta.domain` don't exist" in {
        val events = Seq(
            NoWebHostEvent("1234")
        )
        val df = spark.createDataFrame(sc.parallelize(events))
        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
        val transformedDf =  filter_allowed_domains(partDf)
        transformedDf.df.count should equal(1)
    }

    // These tests don't seem to work.  Apparently the test SparkSession doesn't
    // have Hive UDF support? Getting:
    //   No handler for UDAF 'org.wikimedia.analytics.refinery.hive.GetUAPropertiesUDF'. Use sparkSession.udf.register(...) instead
//
//    it should "geocode_ip `ip`" in {
//        val events = Seq(legacyEventLoggingEvent1)
//        val df = spark.createDataFrame(sc.parallelize(events))
//        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
//        val transformedDf = geocode_ip(partDf)
//        transformedDf.df.columns.contains("geocoded_data") should equal(true)
//        transformedDf.df.select("geocoded_data.continent").take(1).head.getString(0) should equal("Europe")
//    }
//
//    it should "geocode_ip `http.client_ip`" in {
//        val events = Seq(event1)
//        val df = spark.createDataFrame(sc.parallelize(events))
//        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
//        val transformedDf = geocode_ip(partDf)
//        transformedDf.df.columns.contains("geocoded_data") should equal(true)
//        transformedDf.df.select("geocoded_data.continent").take(1).head.getString(0) should equal("Europe")
//    }
//
//    it should "parse_user_agent using `http.request_headers['user-agent']`" in {
//        val events = Seq(event1)
//        val df = spark.createDataFrame(sc.parallelize(events))
//        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
//        val transformedDf =  parse_user_agent(partDf)
//        transformedDf.df.select("user_agent_map.browser_family").take(1).head.getString(0) should equal("Firefox")
//    }
//
//    it should "parse_user_agent using `http.request_headers['user-agent']` and add legacy `useragent` struct" in {
//        val events = Seq(legacyEventLoggingEvent1)
//        val df = spark.createDataFrame(sc.parallelize(events))
//        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
//        val transformedDf =  parse_user_agent(partDf)
//        transformedDf.df.select("user_agent_map.browser_family").take(1).head.getString(0) should equal("Firefox")
//        transformedDf.df.select("useragent.browser_family").take(1).head.getString(0) should equal("Firefox")
//        transformedDf.df.select("useragent.is_mediawiki").take(1).head.getBoolean(0) should equal(false)
//        transformedDf.df.select("useragent.is_bot").take(1).head.getBoolean(0) should equal(false)
//    }

}
