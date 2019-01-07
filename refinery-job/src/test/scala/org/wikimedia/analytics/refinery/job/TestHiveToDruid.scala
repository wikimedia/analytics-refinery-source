package org.wikimedia.analytics.refinery.job

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}
import scala.collection.immutable.ListMap
import org.joda.time.DateTime


class TestHiveToDruid extends FlatSpec with Matchers {

    it should "get threshold condition for <" in {
        val result = HiveToDruid.getThresholdCondition(
            ListMap("year" -> 2019, "month" -> 10, "day" -> 5),
            "<"
        )
        val expected = "(year < 2019 OR year = 2019 AND (month < 10 OR month = 10 AND day < 5))"
        assert(result == expected)
    }

    it should "get threshold condition for >" in {
        val result = HiveToDruid.getThresholdCondition(
            ListMap("year" -> 2019, "month" -> 10, "day" -> 5),
            ">"
        )
        val expected = "(year > 2019 OR year = 2019 AND (month > 10 OR month = 10 AND day >= 5))"
        assert(result == expected)
    }
    
    it should "fail when calling getBetweenCondition with unmatching key sets" in {
        intercept[IllegalArgumentException] {
            HiveToDruid.getBetweenCondition(
                ListMap("year" -> 2019, "month" -> 10, "day" -> 5),
                ListMap("month" -> 10, "day" -> 5, "hour" -> 3)
            )
        }
    }
    
    it should "fail when calling getBetweenCondition with since > until" in {
        intercept[IllegalArgumentException] {
            HiveToDruid.getBetweenCondition(
                ListMap("year" -> 2019, "month" -> 10, "day" -> 6),
                ListMap("year" -> 2019, "month" -> 10, "day" -> 5)
            )
        }
    }
    
    it should "fail when calling getBetweenCondition with since = until for last partition" in {
        intercept[IllegalArgumentException] {
            HiveToDruid.getBetweenCondition(
                ListMap("year" -> 2019),
                ListMap("year" -> 2019)
            )
        }
    }
    
    it should "get between condition with recursive calls" in {
        val result = HiveToDruid.getBetweenCondition(
            ListMap("year" -> 2019, "month" -> 10, "day" -> 5),
            ListMap("year" -> 2019, "month" -> 10, "day" -> 15)
        )
        val expected = "year = 2019 AND month = 10 AND day >= 5 AND day < 15"
        assert(result == expected)
    }
    
    it should "get between condition with calls to getThresholdCondition" in {
        val result = HiveToDruid.getBetweenCondition(
            ListMap("year" -> 2018, "month" -> 10),
            ListMap("year" -> 2019, "month" -> 10)
        )
        val expected = (
            "(year > 2018 OR year = 2018 AND month >= 10) AND " +
            "(year < 2019 OR year = 2019 AND month < 10)"
        )
        assert(result == expected)
    }
    
    it should "get a DateTime map for all time partitions" in {
        val result = HiveToDruid.getDateTimeMap(
            new DateTime(2019, 1, 1, 0, 0),
            Seq("month", "day", "hour", "year")
        )
        val expected = ListMap("year" -> 2019, "month" -> 1, "day" -> 1, "hour" -> 0)
        assert(result == expected)
    }
    
    it should "get a DateTime map for some time partitions" in {
        val result = HiveToDruid.getDateTimeMap(
            new DateTime(2019, 1, 1, 0, 0),
            Seq("month", "year")
        )
        val expected = ListMap("year" -> 2019, "month" -> 1)
        assert(result == expected)
    }
}
