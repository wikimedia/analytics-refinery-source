package org.wikimedia.analytics.refinery.job.refine

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.core.HivePartition
import org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame
import org.wikimedia.analytics.refinery.spark.sql.HiveExtensions._

import scala.collection.immutable.ListMap

case class MetaSubObject(
    id: Option[String] = Some("0VRKNOINdAWYe5HetukWcxhjLEqb1BoD"),
    dt: Option[String] = Some("2020-01-01T00:00:00Z"),
    domain: Option[String] = Some("test.wikipedia.org")
)

case class HttpSubObject(
    client_ip: Option[String] = None,
    request_headers: Option[Map[String, String]] = Some(Map(
        "user-agent" -> "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:19.0) Gecko/20100101 Firefox/19.0"
    ))
)

case class LegacyUserAgentStruct(
    browser_family: Option[String] = Some("-"),
    browser_major: Option[String] = Some("-"),
    browser_minor: Option[String] = Some("-"),
    device_family: Option[String] = Some("-"),
    is_bot: Option[Boolean] = Some(false),
    is_mediawiki: Option[Boolean] = Some(false),
    os_family: Option[String] = Some("-"),
    os_major: Option[String] = Some("-"),
    os_minor: Option[String] = Some("-"),
    wmf_app_version: Option[String] = Some("-")
)

case class TestLegacyEventLoggingEvent(
    uuid: Option[String] = None,
    webHost: Option[String] = None,
    ip: Option[String] = None,
    greeting: Option[String] = Some("hi"),
    tags: Option[Array[String]] = Some(Array("tag1", "tag2")),
    useragent: Option[LegacyUserAgentStruct] = Some(LegacyUserAgentStruct())
)

case class TestEvent(
    meta: Option[MetaSubObject] = None,
    http: Option[HttpSubObject] = None,
    greeting: Option[String] = Some("hi"),
    tags: Option[Array[String]] = Some(Array("tag1", "tag2"))
)

// As EventLogging events are migrated from eventlogging-processor to EventGate,
// Some events will have transform source values in EventLogging legacy fields,
// and others in MEP fields.
case class TestLegacyEventLoggingMigrationEvent(
    meta: Option[MetaSubObject] = None,
    http: Option[HttpSubObject] = None,
    uuid: Option[String] = None,
    webHost: Option[String] = None,
    ip: Option[String] = None,
    greeting: Option[String] = Some("hi"),
    tags: Option[Array[String]] = Some(Array("tag1", "tag2")),
    useragent: Option[LegacyUserAgentStruct] = Some(LegacyUserAgentStruct())
)


case class NoWebHostEvent(
    id: Option[String] = None
)

class TestTransformFunctions extends FlatSpec with Matchers with DataFrameSuiteBase {
    val id1 = Some("IDWONBROLhQYpNlqWiyQosrpfanQx51M")
    val id2 = Some("Wglv2Jv9hflaFtDdZSNXKFv9WwwoD6gX")

    val dt1 = Some("2020-04-01T00:00:00Z")
    val dt2 = Some("2020-04-02T00:00:00Z")

    val internalDomain = Some("test.wikipedia.org")
    val allowedExternalDomain = Some("translate.google.com")
    val unallowedExternalDomain = Some("invalid.hostname.org")
    
    //IPv4 addresses taken from MaxMind's test suite
    val ip1 = Some("81.2.69.160")

    val legacyEventLoggingEvent1 = TestLegacyEventLoggingEvent(id1, internalDomain, ip1)
    val legacyEventLoggingEvent2 = TestLegacyEventLoggingEvent(id2, internalDomain, ip1)
    
    val event1 = TestEvent(
        Some(MetaSubObject(id1, dt1, internalDomain)),
        Some(HttpSubObject(ip1))
    )
    val event2 = TestEvent(
        Some(MetaSubObject(id2, dt2, internalDomain)),
        Some(HttpSubObject(ip1))
    )

    val migratedLegacyEventLoggingEvent = TestLegacyEventLoggingMigrationEvent(
        Some(MetaSubObject(id1, dt1, internalDomain)),
        Some(HttpSubObject(ip1)),
        None,
        None,
        None,
        Some("hello migrated"),
        Some(Array("migrated", "tag")),
        None
    )
    val unmigratedLegacyEventLoggingEvent = TestLegacyEventLoggingMigrationEvent(
        // no meta or http
        None,
        None,
        id1,
        internalDomain,
        ip1,
        Some("hello unmigrated"),
        Some(Array("unmigrated", "tag")),
        Some(LegacyUserAgentStruct(
              Some("PreParsedUserAgent Browser")
        ))
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

    it should "deduplicate based on `meta.id` or `uuid`" in {
        val events = Seq(
            migratedLegacyEventLoggingEvent, migratedLegacyEventLoggingEvent,
            unmigratedLegacyEventLoggingEvent, unmigratedLegacyEventLoggingEvent
        )
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
            r => r.getString(0) should equal(internalDomain.get)
        )
    }

    it should "filter_allowed_domains using `webHost` set to null" in {
        val events = Seq(
            TestLegacyEventLoggingEvent(id1, None, ip1)
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
            NoWebHostEvent(Some("1234"))
        )
        val df = spark.createDataFrame(sc.parallelize(events))
        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
        val transformedDf =  filter_allowed_domains(partDf)
        transformedDf.df.count should equal(1)
    }

    it should "filter_allowed_domains using `meta.domain` or `webHost`" in {
        val events = Seq(
            // internalDomain in meta.domain, keep
            migratedLegacyEventLoggingEvent,
            // internalDomain in webHost, keep
            unmigratedLegacyEventLoggingEvent,
            // Null domain in meta.domain, keep
            TestLegacyEventLoggingMigrationEvent(
                Some(MetaSubObject(id1, dt1, None)),
                Some(HttpSubObject(ip1))
            ),
            // Null domain in webHost, keep
            TestLegacyEventLoggingMigrationEvent(None, None, id1, None, ip1),
            // whitelisted external meta.domain, keep
            TestLegacyEventLoggingMigrationEvent(
                Some(MetaSubObject(id1, dt1, allowedExternalDomain)),
                Some(HttpSubObject(ip1))
            ),
            // whitelisted external webHost, keep
            TestLegacyEventLoggingMigrationEvent(None, None, id1, allowedExternalDomain, ip1),
            // blacklisted external meta.domain, discard
            TestLegacyEventLoggingMigrationEvent(
                Some(MetaSubObject(id1, dt1, unallowedExternalDomain)),
                Some(HttpSubObject(ip1))
            ),
            // blacklisted external webHost, discard
            TestLegacyEventLoggingMigrationEvent(None, None, id1, unallowedExternalDomain, ip1)
        )

        val df = spark.createDataFrame(sc.parallelize(events))
        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
        val transformedDf = filter_allowed_domains(partDf)
        transformedDf.df.count should equal(6)
    }


    // These tests don't seem to work.  Apparently the test SparkSession doesn't
    // have Hive UDF support? Getting:
    //   No handler for UDAF 'org.wikimedia.analytics.refinery.hive.GetUAPropertiesUDF'. Use sparkSession.udf.register(...) instead
//
//    // import org.wikimedia.analytics.refinery.job.refine._
//    it should "geocode_ip `ip`" in {
//        val events = Seq(legacyEventLoggingEvent1)
//        val df = spark.createDataFrame(sc.parallelize(events))
//        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
//        val transformedDf = geocode_ip(partDf)
//        transformedDf.df.columns.contains("geocoded_data") should equal(true)
//        transformedDf.df.select("geocoded_data.continent").take(1).head.getString(0) should equal("Europe")
//        // make sure ip field remains untouched
//        transformedDf.df.select("ip").take(1).head.getString(0) should equal(legacyEventLoggingEvent1.ip.get)
//
//    }
//
//    it should "geocode_ip `http.client_ip`" in {
//        val events = Seq(event1)
//        val df = spark.createDataFrame(sc.parallelize(events))
//        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
//        val transformedDf = geocode_ip(partDf)
//        transformedDf.df.columns.contains("geocoded_data") should equal(true)
//        transformedDf.df.select("geocoded_data.continent").take(1).head.getString(0) should equal("Europe")
//        // Make sure legacy ip column was not added as it was not in the source data schema
//        transformedDf.df.hasColumn("ip") should equal(false);
//    }
//
//    it should "geocode_ip `http.client_ip` and set legacy `ip` column" in {
//        val events = Seq(migratedLegacyEventLoggingEvent)
//        val df = spark.createDataFrame(sc.parallelize(events))
//        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
//        val transformedDf = geocode_ip(partDf)
//        transformedDf.df.columns.contains("geocoded_data") should equal(true)
//        transformedDf.df.select("geocoded_data.continent").take(1).head.getString(0) should equal("Europe")
//        // ip field should be set to value of http.client_ip
//        transformedDf.df.select("ip").take(1).head.getString(0) should equal(migratedLegacyEventLoggingEvent.http.get.client_ip.get)
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
//        val events = Seq(migratedLegacyEventLoggingEvent)
//        val df = spark.createDataFrame(sc.parallelize(events))
//        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
//        val transformedDf =  parse_user_agent(partDf)
//        transformedDf.df.select("user_agent_map.browser_family").take(1).head.getString(0) should equal("Firefox")
//        transformedDf.df.select("useragent.browser_family").take(1).head.getString(0) should equal("Firefox")
//        transformedDf.df.select("useragent.is_mediawiki").take(1).head.getBoolean(0) should equal(false)
//        transformedDf.df.select("useragent.is_bot").take(1).head.getBoolean(0) should equal(false)
//    }
//
//    it should "parse_user_agent using `http.request_headers['user-agent']` and add legacy `useragent` struct, and also keep preparsed `useragent` struct" in {
//        val events = Seq(migratedLegacyEventLoggingEvent, unmigratedLegacyEventLoggingEvent)
//        val df = spark.createDataFrame(sc.parallelize(events))
//        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
//        val transformedDf =  parse_user_agent(partDf)
//        val browserFamiliesDf = transformedDf.df.selectExpr("user_agent_map.browser_family as uam_family", "useragent.browser_family as uas_family")
//
//        val uaMapBrowserFamily = transformedDf.df.select("user_agent_map.browser_family").collect()
//        val uaStructBrowserFamily = transformedDf.df.select("useragent.browser_family").collect()
//
//        uaMapBrowserFamily(0).getString(0) should equal("Firefox")
//        uaStructBrowserFamily(0).getString(0) should equal("Firefox")
//
//        uaMapBrowserFamily(1).getString(0) should equal(null)
//        uaStructBrowserFamily(1).getString(0) should equal("PreParsedUserAgent Browser")
//    }

}
