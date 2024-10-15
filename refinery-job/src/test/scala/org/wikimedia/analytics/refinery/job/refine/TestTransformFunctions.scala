package org.wikimedia.analytics.refinery.job.refine

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.core.HivePartition
import org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame

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
    override def conf: SparkConf = super.conf.set(CATALOG_IMPLEMENTATION.key, "hive")

    val id1 = Some("IDWONBROLhQYpNlqWiyQosrpfanQx51M")
    val id2 = Some("Wglv2Jv9hflaFtDdZSNXKFv9WwwoD6gX")

    val dt1 = Some("2020-04-01T00:00:00Z")
    val dt2 = Some("2020-04-02T00:00:00Z")

    val internalDomain = Some("test.wikipedia.org")
    val allowedExternalDomain = Some("translate.google.com")
    val unallowedExternalDomain = Some("invalid.hostname.org")
    val canaryDomain = Some("canary")
    val mDotDomain = Some("en.m.wikipedia.org")

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
    val canaryEvent1 = TestEvent(
        Some(MetaSubObject(id1, dt1, canaryDomain)),
        Some(HttpSubObject(ip1))
    )
    val mDotEvent = TestEvent(
        Some(MetaSubObject(id1, dt1, mDotDomain)),
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
        id2,
        internalDomain,
        ip1,
        Some("hello unmigrated"),
        Some(Array("unmigrated", "tag")),
        Some(LegacyUserAgentStruct(
            Some("PreParsedUserAgent Browser")
        ))
    )

    val migratedLegacyEventLoggingMDotEvent = TestLegacyEventLoggingMigrationEvent(
        Some(MetaSubObject(id1, dt1, mDotDomain)),
        Some(HttpSubObject(ip1)),
        None,
        None,
        None,
        Some("hello migrated"),
        Some(Array("migrated", "tag")),
        None
    )
    val unmigratedLegacyEventLoggingMDotEvent = TestLegacyEventLoggingMigrationEvent(
        // no meta or http
        None,
        None,
        id1,
        mDotDomain,
        ip1,
        Some("hello unmigrated"),
        Some(Array("unmigrated", "tag")),
        Some(LegacyUserAgentStruct(
            Some("PreParsedUserAgent Browser")
        ))
    )


    val fakeHivePartition = new HivePartition(database = "testDb", t = "testTable", location = Some("/fake/location"), ListMap("year" -> Some("2018")))

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


    it should "deduplicate based on `meta.id`, not removing rows with null `meta.id`" in {
        val eventWithNullId = event2.copy(
            meta = Some(MetaSubObject(None, Some("2020-01-01T00:00:00Z"), Some("test.wikipedia.org")))
        )

        val events = Seq(event1, event1, eventWithNullId, eventWithNullId, eventWithNullId)
        val df = spark.createDataFrame(sc.parallelize(events))
        val partDf = PartitionedDataFrame(df, fakeHivePartition)
        val transformedDf = deduplicate(partDf)
        transformedDf.df.count should equal(4)
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

    it should "deduplicate over a sorted DF to keep a single line in a deterministic manner" in {
        // We have 3 duplicated events, only differing from the `dt` field
        val dt1: Some[String] = Some("2020-04-01T00:00:01Z")
        val dt2: Some[String] = Some("2020-04-01T00:00:02Z")
        val dt3: Some[String] = Some("2020-04-01T00:00:03Z")
        val events = Seq(
            TestEvent(Some(MetaSubObject(id1, dt2, internalDomain)), Some(HttpSubObject(ip1))),
            TestEvent(Some(MetaSubObject(id1, dt1, internalDomain)), Some(HttpSubObject(ip1))), // Keep this one
            TestEvent(Some(MetaSubObject(id1, dt3, internalDomain)), Some(HttpSubObject(ip1)))
        )
        val df = spark.createDataFrame(sc.parallelize(events))
        val partDf = PartitionedDataFrame(df, fakeHivePartition)
        val transformedDf = deduplicate(partDf)
        // Keep the one with the earliest `dt`
        transformedDf.df.select("meta.dt").collect().map(_(0)) should contain allElementsOf Seq(dt3.get)
    }

    it should "filter_allowed_domains using `webHost`" in {
        val events = Seq(
            TestLegacyEventLoggingEvent(id1, internalDomain, ip1),
            TestLegacyEventLoggingEvent(id1, internalDomain, ip1),
            TestLegacyEventLoggingEvent(id1, unallowedExternalDomain, ip1)
        )

        val df = spark.createDataFrame(sc.parallelize(events))
        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
        val transformedDf = filter_allowed_domains(partDf)
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
        val transformedDf = filter_allowed_domains(partDf)
        transformedDf.df.count should equal(1)
    }

    it should "filter_allowed_domains using `webHost` set to external domain value in whitelist" in {
        val events = Seq(
            TestLegacyEventLoggingEvent(id1, allowedExternalDomain, ip1)
        )
        val df = spark.createDataFrame(sc.parallelize(events))
        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
        val transformedDf = filter_allowed_domains(partDf)
        transformedDf.df.count should equal(1)
    }

    it should "filter_allowed_domains if `webHost` and `meta.domain` don't exist" in {
        val events = Seq(
            NoWebHostEvent(Some("1234"))
        )
        val df = spark.createDataFrame(sc.parallelize(events))
        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
        val transformedDf = filter_allowed_domains(partDf)
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

    it should "add_is_wmf_domain using `meta.domain` or `webHost`" in {
        val events = Seq(
            // internalDomain in meta.domain, true
            migratedLegacyEventLoggingEvent,
            // internalDomain in webHost, true
            unmigratedLegacyEventLoggingEvent,
            // Null domain in meta.domain, false
            TestLegacyEventLoggingMigrationEvent(
                Some(MetaSubObject(id1, dt1, None)),
                Some(HttpSubObject(ip1))
            ),
            // Null domain in webHost, false
            TestLegacyEventLoggingMigrationEvent(None, None, id1, None, ip1),
            // external meta.domain, false
            TestLegacyEventLoggingMigrationEvent(
                Some(MetaSubObject(id1, dt1, unallowedExternalDomain)),
                Some(HttpSubObject(ip1))
            ),
            // external webHost, false
            TestLegacyEventLoggingMigrationEvent(None, None, id1, unallowedExternalDomain, ip1)
        )

        val df = spark.createDataFrame(sc.parallelize(events))
        val partDf = new PartitionedDataFrame(df, fakeHivePartition)

        val transformedDf = add_is_wmf_domain(partDf)
        transformedDf.df.schema("is_wmf_domain") should equal(StructField("is_wmf_domain", BooleanType, false))
        val isWMFDomainResults = transformedDf.df.select("is_wmf_domain").collect
        // internalDomain in meta.domain, true
        isWMFDomainResults(0)(0) should equal(true)
        // internalDomain in webHost, true
        isWMFDomainResults(1)(0) should equal(true)
        // Null domain in meta.domain, false
        isWMFDomainResults(2)(0) should equal(false)
        // Null domain in webHost, false
        isWMFDomainResults(3)(0) should equal(false)
        // external meta.domain, false
        isWMFDomainResults(4)(0) should equal(false)
        // external webHost, false
        isWMFDomainResults(5)(0) should equal(false)
    }


    it should "remove_canary_events using `meta.domain`" in {
        val events = Seq(
            event1,
            event2,
            // Null domain in meta.domain, keep
            TestEvent(
                Some(MetaSubObject(id1, dt1, None)),
                Some(HttpSubObject(ip1))
            ),
            // This event will be removed
            canaryEvent1
        )

        val df = spark.createDataFrame(sc.parallelize(events))
        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
        val transformedDf = remove_canary_events(partDf)
        transformedDf.df.count should equal(3)
    }

    it should "get normalized_host using `meta.domain`" in {
        val events = Seq(mDotEvent)
        val df = spark.createDataFrame(sc.parallelize(events))
        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
        val transformedDf = add_normalized_host(partDf)

        transformedDf.df.columns.contains("normalized_host") should equal(true)

        transformedDf.df.schema("normalized_host") should equal(StructField("normalized_host", StructType(Seq(
            StructField("project_class", StringType, true),
            StructField("project", StringType, true),
            StructField("qualifiers", ArrayType(StringType), true),
            StructField("tld", StringType, true),
            StructField("project_family", StringType, true)
        )), true))

        transformedDf.df.select("normalized_host.project_class").take(1).head.getString(0) should equal("wikipedia")
        transformedDf.df.select("normalized_host.project_family").take(1).head.getString(0) should equal("wikipedia")
        transformedDf.df.select("normalized_host.project").take(1).head.getString(0) should equal("en")
        transformedDf.df.select("normalized_host.qualifiers").take(1).head.getSeq[String](0) should equal(Array("m"))
        transformedDf.df.select("normalized_host.tld").take(1).head.getString(0) should equal("org")
    }

    it should "get normalized_host from `webHost` for migrated or unmigrated legacy events" in {
        val events = Seq(migratedLegacyEventLoggingMDotEvent, unmigratedLegacyEventLoggingMDotEvent)
        val df = spark.createDataFrame(sc.parallelize(events))
        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
        val transformedDf = add_normalized_host(partDf)

        transformedDf.df.columns.contains("normalized_host") should equal(true)

        transformedDf.df.schema("normalized_host") should equal(StructField("normalized_host", StructType(Seq(
            StructField("project_class", StringType, true),
            StructField("project", StringType, true),
            StructField("qualifiers", ArrayType(StringType), true),
            StructField("tld", StringType, true),
            StructField("project_family", StringType, true)
        )), true))

        val projectClass = transformedDf.df.select("normalized_host.project_class").collect()
        projectClass(0).getString(0) should equal("wikipedia")
        projectClass(1).getString(0) should equal("wikipedia")

        val projectFamily = transformedDf.df.select("normalized_host.project_family").collect()
        projectFamily(0).getString(0) should equal("wikipedia")
        projectFamily(1).getString(0) should equal("wikipedia")

        val project = transformedDf.df.select("normalized_host.project").collect()
        project(0).getString(0) should equal("en")
        project(1).getString(0) should equal("en")

        val qualifiers = transformedDf.df.select("normalized_host.qualifiers").collect()
        qualifiers(0).getSeq[String](0) should equal(Array("m"))
        qualifiers(1).getSeq[String](0) should equal(Array("m"))

        val tld = transformedDf.df.select("normalized_host.tld").collect()
        tld(0).getString(0) should equal("org")
        tld(1).getString(0) should equal("org")
    }

    it should "geocode_ip `ip`" in {
        val events = Seq(legacyEventLoggingEvent1)
        val df = spark.createDataFrame(sc.parallelize(events))
        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
        val transformedDf = geocode_ip(partDf)
        transformedDf.df.columns.contains("geocoded_data") should equal(true)
        transformedDf.df.select("geocoded_data.continent").take(1).head.getString(0) should equal("Europe")
        // make sure ip field remains untouched
        transformedDf.df.select("ip").take(1).head.getString(0) should equal(legacyEventLoggingEvent1.ip.get)

    }

    it should "geocode_ip `http.client_ip`" in {
        val events = Seq(event1)
        val df = spark.createDataFrame(sc.parallelize(events))
        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
        val transformedDf = geocode_ip(partDf)
        transformedDf.df.columns.contains("geocoded_data") should equal(true)
        transformedDf.df.select("geocoded_data.continent").take(1).head.getString(0) should equal("Europe")
        // Make sure legacy ip column was not added as it was not in the source data schema
        transformedDf.df.columns.contains("ip") should equal(false);
    }

    it should "geocode_ip `http.client_ip` and set legacy `ip` column" in {
        val events = Seq(migratedLegacyEventLoggingEvent)
        val df = spark.createDataFrame(sc.parallelize(events))
        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
        val transformedDf = geocode_ip(partDf)
        transformedDf.df.columns.contains("geocoded_data") should equal(true)
        transformedDf.df.select("geocoded_data.continent").take(1).head.getString(0) should equal("Europe")
        // ip field should be set to value of http.client_ip
        transformedDf.df.select("ip").take(1).head.getString(0) should equal(migratedLegacyEventLoggingEvent.http.get.client_ip.get)
    }

    it should "parse_user_agent using `http.request_headers['user-agent']`" in {
        val events = Seq(event1)
        val df = spark.createDataFrame(sc.parallelize(events))
        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
        val transformedDf = parse_user_agent(partDf)
        transformedDf.df.select("user_agent_map.browser_family").take(1).head.getString(0) should equal("Firefox")
    }

    it should "parse_user_agent using `http.request_headers['user-agent']` and add legacy `useragent` struct" in {
        val events = Seq(migratedLegacyEventLoggingEvent)
        val df = spark.createDataFrame(sc.parallelize(events))
        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
        val transformedDf = parse_user_agent(partDf)
        transformedDf.df.select("user_agent_map.browser_family").take(1).head.getString(0) should equal("Firefox")
        transformedDf.df.select("useragent.browser_family").take(1).head.getString(0) should equal("Firefox")
        transformedDf.df.select("useragent.is_mediawiki").take(1).head.getBoolean(0) should equal(false)
        transformedDf.df.select("useragent.is_bot").take(1).head.getBoolean(0) should equal(false)
    }

    it should "parse_user_agent using `http.request_headers['user-agent']` and add legacy `useragent` struct, and also keep preparsed `useragent` struct" in {
        val events = Seq(migratedLegacyEventLoggingEvent, unmigratedLegacyEventLoggingEvent)
        val df = spark.createDataFrame(sc.parallelize(events))
        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
        val transformedDf = parse_user_agent(partDf)

        val uaMapBrowserFamily = transformedDf.df.select("user_agent_map.browser_family").collect()
        val uaStructBrowserFamily = transformedDf.df.select("useragent.browser_family").collect()

        uaMapBrowserFamily(0).getString(0) should equal("Firefox")
        uaStructBrowserFamily(0).getString(0) should equal("Firefox")

        uaMapBrowserFamily(1).getString(0) should equal(null)
        uaStructBrowserFamily(1).getString(0) should equal("PreParsedUserAgent Browser")
    }

    it should "normalizeFieldNamesAndWidenTypes" in {
        val eventsSeq = Seq("""{"UPPER_CASE_FIELD": "fake_value", "number_field": 1}""")
        import spark.implicits._
        val inputSchema = StructType(Seq(
            StructField("UPPER_CASE_FIELD", StringType, nullable = true),
            StructField("number_field", IntegerType, nullable = true)
        ))
        val df = spark.read.schema(inputSchema).json(spark.createDataset[String](sc.parallelize(eventsSeq, 1)))
        df.schema.fields(0) shouldEqual (StructField("UPPER_CASE_FIELD", StringType, nullable = true))
        df.schema.fields(1) shouldEqual (StructField("number_field", IntegerType, nullable = true))

        val resDf = normalizeFieldNamesAndWidenTypes(new PartitionedDataFrame(df, fakeHivePartition)).df

        resDf.schema.fields(0) shouldEqual (StructField("upper_case_field", StringType, nullable = true))
        resDf.schema.fields(1) shouldEqual (StructField("number_field", LongType, nullable = true))
    }

    it should "fail to parse timestamps in case of wrong config" in {
        assertThrows[IllegalStateException] {
            val eventsSeq = Seq("""{"dt": "2024-02-19T18:00:00Z", "client_dt": "2024-02-19T18:00:00Z", "meta": {"dt": "2024-02-19T19:00:00.000Z"}}""")
            import spark.implicits._
            val df = spark.read.json(spark.createDataset[String](sc.parallelize(eventsSeq, 1)))
            val partDf = new PartitionedDataFrame(df, fakeHivePartition)

            val resDf = parseTimestampFields(partDf).df
        }
    }

    it should "parse timestamps" in {
        val eventsSeq = Seq("""{"dt": "2024-02-19T18:00:00Z", "client_dt": "2024-02-19T18:00:00Z", "meta": {"dt": "2024-02-19T19:00:00.000Z"}}""")
        import spark.implicits._
        spark.conf.set(parseTimestampFields.FieldsToParseParameterName, "dt,client_dt,meta.dt")
        val df = spark.read.json(spark.createDataset[String](sc.parallelize(eventsSeq, 1)))
        val partDf = new PartitionedDataFrame(df, fakeHivePartition)
        val expectedSchema = StructType(Seq(
            StructField("dt", TimestampType, nullable = true),
            StructField("client_dt", TimestampType, nullable = true),
            StructField("meta", StructType(Seq(StructField("dt", TimestampType, nullable = true))), nullable = true)
        ))


        val resDf = parseTimestampFields(partDf).df
        resDf.schema.equals(expectedSchema)
        val resRow = resDf.collect().head
        resRow.getTimestamp(0).getTime shouldEqual (1708365600000L)
        resRow.getTimestamp(1).getTime shouldEqual (1708365600000L)
        resRow.getStruct(2).getTimestamp(0).getTime shouldEqual (1708369200000L)
    }
}
