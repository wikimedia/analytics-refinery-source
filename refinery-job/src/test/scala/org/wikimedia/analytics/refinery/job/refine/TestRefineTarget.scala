package org.wikimedia.analytics.refinery.job.refine

import com.github.nscala_time.time.Imports.{DateTime, DateTimeFormat}
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.DateTimeZone
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.util.matching.Regex


class TestRefineTarget extends FlatSpec
    with Matchers with DataFrameSuiteBase with BeforeAndAfterAll {

    case class Record(
        greeting: Option[String] = Some("hi"),
        tags: Option[Array[String]] = Some(Array("tag1", "tag2"))
    )



    val since = new DateTime(
        2021, 3, 22, 10, 0, DateTimeZone.UTC
    )
    // until should be exclusive, so the eqiad_table_a hour 20 should not be included.
    val until = new DateTime(
        2021, 3, 22, 20, 0, DateTimeZone.UTC
    )

    it should "get table names" in {
        val records = Seq(Record())
        val df = spark.createDataFrame(sc.parallelize(records))
        df.createOrReplaceTempView("table_a")

        val tableNames = RefineTarget.getTableNames(spark, "default")
        tableNames should equal (Seq("table_a"))
    }

    it should "determine if a string should be included or excluded" in {
        val s = "table_a"

        RefineTarget.shouldInclude(s) should equal (true)
        RefineTarget.shouldInclude(s, Some("table_b".r)) should equal(false)
        RefineTarget.shouldInclude(s, None, Some("table_a".r)) should equal(false)
        RefineTarget.shouldInclude(s, Some("table_a".r), Some("table_a".r)) should equal(false)
    }
//
//    it should "get partition paths from filesystem" in {
//        // TODO: This is not working as expected...can't get correct path to test resources???
//        val resourcesPath = getClass.getResource("/")
//        val paths = RefineTarget.getPartitionPathsFromFS(
//            spark,
//            new Path(s"$resourcesPath/data/raw/event"),
//            DateTimeFormat.forPattern("'hourly'/yyyy/MM/dd/HH"),
//            new Regex(
//                "(eqiad|codfw)_(table)/hourly/(\\d+)/(\\d+)/(\\d+)/(\\d+)",
//                "datacenter", "table", "year", "month", "day", "hour"
//            ),
//            since,
//            until
//        )
//
//        val expected = Seq(
//            s"$resourcesPath/data/raw/event/eqiad_table_a/hourly/2021/03/22/19",
//            s"$resourcesPath/data/raw/event/eqiad_table_b/hourly/2021/03/22/19"
//        ).map(new Path(_))
//
//        paths should equal(expected)
//    }

}
