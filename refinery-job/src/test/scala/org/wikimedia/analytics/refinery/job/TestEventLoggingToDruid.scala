package org.wikimedia.analytics.refinery.job

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StringType, IntegerType, LongType}
import org.scalatest.{FlatSpec, Matchers, BeforeAndAfterEach}


class TestEventLoggingToDruid extends FlatSpec
    with Matchers with BeforeAndAfterEach with SharedSparkContext {

    var testDf: DataFrame = null.asInstanceOf[DataFrame]

    override def beforeEach(): Unit = {
        val testSchema: StructType = (new StructType)
            .add("event", (new StructType)
                .add("pageId", StringType)
                .add("action", StringType)
                .add("seconds", IntegerType))
            .add("dt", StringType)
            .add("webhost", StringType)
            .add("useragent", StringType)
            .add("year", IntegerType)

        val testRDD = sc.parallelize(Seq(
            Row(Row("page1", "edit", 10), "2017-01-01T00:00:00", "en.wikimedia.org", "UA1", 2017),
            Row(Row("page1", "read", 20), "2017-01-01T00:00:01", "en.wikimedia.org", "UA2", 2017),
            Row(Row("page2", "edit", 30), "2017-01-01T00:00:02", "es.wikimedia.org", "UA3", 2017),
            Row(Row("page3", "read", 40), "2017-01-01T00:00:03", "es.wikimedia.org", "UA4", 2017),
            Row(Row("page3", "read", 50), "2017-01-01T00:00:04", "es.wikimedia.org", "UA5", 2017)
        ))

        val sqlContext = new SQLContext(sc)
        testDf = sqlContext.createDataFrame(testRDD, testSchema)
    }


    it should "flatten the event part of the schema" in {
        val flatColumns = EventLoggingToDruid.getFlatColumns(testDf.schema)
        val flatFields = testDf.select(flatColumns:_*).schema.map(f => f.name)

        assert(flatFields.contains("event_pageId"))
        assert(flatFields.contains("event_action"))
        assert(flatFields.contains("event_seconds"))
    }

    it should "correct case for the capsule fields" in {
        val flatColumns = EventLoggingToDruid.getFlatColumns(testDf.schema)
        val flatFields = testDf.select(flatColumns:_*).schema.map(f => f.name)

        assert(flatFields.contains("webHost"))
        assert(flatFields.contains("userAgent"))
    }

    it should "blacklist whole columns properly" in {
        val blacklist = Seq("event", "webHost")
        val flatColumns = EventLoggingToDruid.getFlatColumns(testDf.schema)
        val flatDf = testDf.select(flatColumns:_*)
        val cleanColumns = EventLoggingToDruid.getCleanColumns(flatDf.schema, blacklist)
        val cleanFields = flatDf.select(cleanColumns:_*).schema.map(f => f.name)

        assert(cleanFields.length == 1)
        assert(cleanFields.contains("dt"))
    }

    it should "blacklist struct column subfields properly" in {
        val blacklist = Seq("event_pageId", "event_seconds")
        val flatColumns = EventLoggingToDruid.getFlatColumns(testDf.schema)
        val flatDf = testDf.select(flatColumns:_*)
        val cleanColumns = EventLoggingToDruid.getCleanColumns(flatDf.schema, blacklist)
        val cleanFields = flatDf.select(cleanColumns:_*).schema.map(f => f.name)

        assert(cleanFields.length == 3)
        assert(cleanFields.contains("event_action"))
        assert(cleanFields.contains("dt"))
        assert(cleanFields.contains("webHost"))
    }

    it should "select dimensions and metrics properly" in {
        val blacklist = Seq("event_pageId")
        val flatColumns = EventLoggingToDruid.getFlatColumns(testDf.schema)
        val flatDf = testDf.select(flatColumns:_*)
        val cleanColumns = EventLoggingToDruid.getCleanColumns(flatDf.schema, blacklist)
        val cleanDf = flatDf.select(cleanColumns:_*)

        val (dimensionFields, metricFields) = EventLoggingToDruid.getDimensionsAndMetrics(
            cleanDf.schema,
            (f) => f.dataType match {
                case IntegerType => true
                case _ => false
            }
        )

        assert(dimensionFields.length == 2)
        assert(dimensionFields.contains("event_action"))
        assert(dimensionFields.contains("webHost"))

        assert(metricFields.length == 1)
        assert(metricFields.contains("event_seconds"))
    }
}
