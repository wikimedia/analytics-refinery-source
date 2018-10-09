package org.wikimedia.analytics.refinery.job

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}


class TestEventLoggingToDruid extends FlatSpec
    with Matchers with BeforeAndAfterEach with DataFrameSuiteBase {

    var testDf: DataFrame = null.asInstanceOf[DataFrame]

    override def beforeEach(): Unit = {
        val testSchema: StructType = (new StructType)
            .add("event", (new StructType)
                .add("pageId", StringType)
                .add("action", StringType)
                .add("category", IntegerType)
                .add("milliseconds", IntegerType)
                .add("clicks", IntegerType))
            .add("dt", StringType)
            .add("wiki", StringType)
            .add("webhost", StringType)
            .add("useragent", StringType)
            .add("year", IntegerType)

        val testRDD = sc.parallelize(Seq(
            Row(Row("page1", "edit", 1, 10), "2017-01-01T00:00:00", "enwiki", "en.wikipedia.org", "UA1", 2017),
            Row(Row("page1", "read", 2, 20), "2017-01-01T00:00:01", "enwiki", "en.wikipedia.org", "UA2", 2017),
            Row(Row("page2", "edit", 3, 30), "2017-01-01T00:00:02", "enwiki", "es.wikipedia.org", "UA3", 2017),
            Row(Row("page3", "read", 4, 40), "2017-01-01T00:00:03", "enwiki", "es.wikipedia.org", "UA4", 2017),
            Row(Row("page3", "read", 5, 50), "2017-01-01T00:00:04", "enwiki", "es.wikipedia.org", "UA5", 2017)
        ))

        testDf = spark.createDataFrame(testRDD, testSchema)
    }

    it should "check that all selected fields are allowed" in {
        val config1 = EventLoggingToDruid.Config(
            dimensions=Seq("event_action"),
            time_measures=Seq("event_milliseconds"),
            metrics=Seq("event_clicks")
        )
        assert(EventLoggingToDruid.validFieldLists(config1) == true)

        val config2 = EventLoggingToDruid.Config(
            dimensions=Seq("clientIp"),
            time_measures=Seq("year"),
            metrics=Seq("schema")
        )
        assert(EventLoggingToDruid.validFieldLists(config2) == false)
    }

    it should "flatten the event part of the schema" in {
        val flatColumns = EventLoggingToDruid.getFlatColumns(testDf.schema)
        val flatFields = testDf.select(flatColumns:_*).schema.map(f => f.name)

        assert(flatFields.contains("event_pageId"))
        assert(flatFields.contains("event_action"))
        assert(flatFields.contains("event_category"))
        assert(flatFields.contains("event_milliseconds"))
    }

    it should "correct case for the capsule fields" in {
        val flatColumns = EventLoggingToDruid.getFlatColumns(testDf.schema)
        val flatFields = testDf.select(flatColumns:_*).schema.map(f => f.name)

        assert(flatFields.contains("webHost"))
        assert(flatFields.contains("userAgent"))
    }

    it should "select dimensions properly" in {
        val flatColumns = EventLoggingToDruid.getFlatColumns(testDf.schema)
        val flatDf = testDf.select(flatColumns:_*)
        val dimensions = Seq("wiki", "webHost", "event_action", "event_category")
        val config = EventLoggingToDruid.Config(dimensions=dimensions)
        val cleanColumns = EventLoggingToDruid.getCleanColumns(flatDf.schema, config)
        val cleanFields = flatDf.select(cleanColumns:_*).schema.map(f => f.name)

        assert(cleanFields.length == 5)
        assert(cleanFields.contains("wiki"))
        assert(cleanFields.contains("webHost"))
        assert(cleanFields.contains("event_action"))
        assert(cleanFields.contains("event_category"))
        assert(cleanFields.contains("dt"))
    }

    it should "select and bucketize time measures properly" in {
        val flatColumns = EventLoggingToDruid.getFlatColumns(testDf.schema)
        val flatDf = testDf.select(flatColumns:_*)
        val timeMeasures = Seq("event_milliseconds")
        val config = EventLoggingToDruid.Config(time_measures=timeMeasures)
        val cleanColumns = EventLoggingToDruid.getCleanColumns(flatDf.schema, config)
        val cleanDf = flatDf.select(cleanColumns:_*)
        val bucketizedColumns = EventLoggingToDruid.getBucketizedColumns(cleanDf.schema, config)
        val bucketizedFields = cleanDf.select(bucketizedColumns:_*).schema

        assert(bucketizedFields.length == 2)
        assert(bucketizedFields(0).name == "event_milliseconds_buckets")
        assert(bucketizedFields(0).dataType == StringType)
    }

    it should "select metrics properly" in {
        val flatColumns = EventLoggingToDruid.getFlatColumns(testDf.schema)
        val flatDf = testDf.select(flatColumns:_*)
        val metrics = Seq("event_clicks")
        val config = EventLoggingToDruid.Config(metrics=metrics)
        val cleanColumns = EventLoggingToDruid.getCleanColumns(flatDf.schema, config)
        val cleanFields = flatDf.select(cleanColumns:_*).schema.map(f => f.name)

        assert(cleanFields.length == 2)
        assert(cleanFields.contains("event_clicks"))
    }

}
