package org.wikimedia.analytics.refinery.core

import org.scalatest.{FlatSpec, Matchers}
import com.github.nscala_time.time.Imports.{DateTime, DateTimeZone}

import scala.collection.immutable.ListMap
import scala.util.matching.Regex


class TestHivePartition extends FlatSpec with Matchers {

    val database = "testdb"
    val table = "Test-Table"
    val location = "/path/to/test_table"
    val partitions: ListMap[String, Option[String]] = ListMap(
        "datacenter" -> Some("dc1"), "year" -> Some("2017"), "month" -> Some("07")
    )
    val partition: HivePartition = HivePartition(database, table, Some(location), partitions)
    val dynamicPartition: HivePartition  = partition.copy(partitions = partitions + ("month" -> None))

    it should "normalized fully qualified table name" in {
        partition.tableName should equal(s"`$database`.`${table.replace("-", "_").toLowerCase}`")
    }

    it should "have partition keys" in {
        partition.keys should equal(partition.keys.toSeq)
    }

    it should "have correct hive QL representation" in {
        partition.hiveQL should equal("""datacenter="dc1",year=2017,month=7""")
    }

    it should "have correct SQL predicate representation" in {
        partition.sqlPredicate should equal("""datacenter="dc1" AND year=2017 AND month=7""")
    }

    it should "have correct hive QL representation for dynamic partitions" in {
        dynamicPartition.hiveQL should equal("""datacenter="dc1",year=2017,month""")
    }

    it should "have correct hive relative path representation" in {
        partition.relativePath should equal("""datacenter=dc1/year=2017/month=7""")
    }

    it should "throw an IllegalStateException for dynamic partition hive relative path representation" in {
        assertThrows[IllegalStateException] {
            dynamicPartition.relativePath
        }
    }

    it should "have correct hive full path" in {
        partition.path should equal(s"""$location/datacenter=dc1/year=2017/month=7""")
    }

    it should "construct with regex and path" in {

        val regex = new Regex(
            "hourly/(.+)_(.+)/(\\d{4})/(\\d{2})",
            "datacenter", "table", "year", "month"
        )

        val baseLocation = "/path/to/external/tables"
        val p = HivePartition(
            database, baseLocation, s"/path/to/raw/hourly/dc2_Test-Table2/2015/05", regex
        )

        p.tableName should equal(s"`$database`.`test_table2`")
        p.path should equal(s"$baseLocation/${p.table}/datacenter=dc2/year=2015/month=5")

        val partitionShouldBe = ListMap(
            "datacenter" -> Some("dc2"), "year" -> Some("2015"), "month" -> Some("05")
        )
        p.partitions should equal(partitionShouldBe)
    }

    it should "construct from with partition path in Hive format" in {
        val baseLocation = "/path/to/database1"
        val p            = HivePartition(
            "database1", "my_table", baseLocation, "datacenter=dc2/year=2015/month=5"
        )

        p.tableName should equal(s"`database1`.`my_table`")
        p.path should equal(s"$baseLocation/${p.table}/datacenter=dc2/year=2015/month=5")

        val partitionShouldBe = ListMap(
            "datacenter" -> Some("dc2"), "year" -> Some("2015"), "month" -> Some("5")
        )
        p.partitions should equal(partitionShouldBe)
    }


    it should "construct from with partition path in Hive format with extra slashes" in {
        val baseLocation = "/path/to/database1"
        val p            = HivePartition(
            "database1", "my_table", baseLocation, "//datacenter=dc2/year=2015/month=5/"
        )

        p.tableName should equal(s"`database1`.`my_table`")
        p.path should equal(s"$baseLocation/${p.table}/datacenter=dc2/year=2015/month=5")

        val partitionShouldBe = ListMap(
            "datacenter" -> Some("dc2"), "year" -> Some("2015"), "month" -> Some("5")
        )
        p.partitions should equal(partitionShouldBe)
    }

    it should "construct from full partition path" in {
        val fullPartitionPath = "/path/to/database1/my_table/year=2020/month=7/day=1/hour=0"
        val p                 = HivePartition(fullPartitionPath)

        p.tableName should equal(s"`database1`.`my_table`")
        p.path should equal(fullPartitionPath)

        val partitionShouldBe = ListMap(
            "year" -> Some("2020"), "month" -> Some("7"), "day" -> Some("1"), "hour" -> Some("0")
        )
        p.partitions should equal(partitionShouldBe)
    }

    it should "convert a DateTime to a ListMap" in {
        // 2021-03-22T19:30:10.000Z
        val dt = new DateTime(
            2021,
            3,
            22,
            19,
            30,
            10,
            DateTimeZone.UTC
        )

        HivePartition.dateTimeToMap(dt, Seq("year")) should equal(ListMap("year" -> Some("2021")))

        HivePartition.dateTimeToMap(dt) should equal(ListMap(
            "year" -> Some("2021"),
            "month" -> Some("3"),
            "day" -> Some("22"),
            "hour" -> Some("19")
        ))
    }

    it should "convert a partition ListMap to a DateTime" in {
        HivePartition.mapToDateTime(
            ListMap("year" -> Some("2015"), "month" -> Some("5"))
        ) should equal(
            Some(new DateTime(2015, 5, 1, 0, 0, DateTimeZone.UTC))
        )

        HivePartition.mapToDateTime(
            ListMap("year" -> Some("2015"), "month" -> Some("05"), "day" -> Some("2"), "hour" -> Some("12"))
        ) should equal(
            Some(new DateTime(2015, 5, 2, 12, 0, DateTimeZone.UTC))
        )

        // Has no dtKeys, should be None.
        HivePartition.mapToDateTime(
            ListMap("datacenter" -> Some("eqiad"))
        ) should equal(
            None
        )

        // Has invalid hierarchy of dtKeys, should be None.
        HivePartition.mapToDateTime(
            // month without year is not valid.
            ListMap("datacenter" -> Some("eqiad"), "month" -> Some("12"))
        ) should equal(
            None
        )

        // Has dynamic partition, should be None.
        HivePartition.mapToDateTime(
            ListMap("datacenter" -> Some("eqiad"), "year" -> Some("2021"), "month" -> None)
        ) should equal(
            None
        )
    }

    it should "convert a partition ListMap to Hive QL" in {
        val partitionMap = ListMap(
            "year" -> Some("2015"), "month" -> Some("5")
        )

        HivePartition.mapToHiveQL(partitionMap) should equal("year=2015,month=5")

        HivePartition.mapToHiveQL(partitionMap, " AND ", ">=") should equal(
            "year>=2015 AND month>=5"
        )
    }

    it should "get threshold condition for <" in {
        val result = HivePartition.getThresholdCondition(
            ListMap("year" -> 2019, "month" -> 10, "day" -> 5),
            "<"
        )
        val expected = "(year < 2019 OR year = 2019 AND (month < 10 OR month = 10 AND day < 5))"
        assert(result == expected)
    }

    it should "get threshold condition for >" in {
        val result = HivePartition.getThresholdCondition(
            ListMap("year" -> 2019, "month" -> 10, "day" -> 5),
            ">"
        )
        val expected = "(year > 2019 OR year = 2019 AND (month > 10 OR month = 10 AND day >= 5))"
        result should equal(expected)
    }

    it should "fail when calling getBetweenCondition with unmatching key sets" in {
        intercept[IllegalArgumentException] {
            HivePartition.getBetweenCondition(
                ListMap("year" -> 2019, "month" -> 10, "day" -> 5),
                ListMap("month" -> 10, "day" -> 5, "hour" -> 3)
            )
        }
    }

    it should "fail when calling getBetweenCondition with since > until" in {
        intercept[IllegalArgumentException] {
            HivePartition.getBetweenCondition(
                ListMap("year" -> 2019, "month" -> 10, "day" -> 6),
                ListMap("year" -> 2019, "month" -> 10, "day" -> 5)
            )
        }
    }

    it should "fail when calling getBetweenCondition with since = until for last partition" in {
        intercept[IllegalArgumentException] {
            HivePartition.getBetweenCondition(
                ListMap("year" -> 2019),
                ListMap("year" -> 2019)
            )
        }
    }

    it should "get between condition with recursive calls" in {
        val result = HivePartition.getBetweenCondition(
            ListMap("year" -> 2019, "month" -> 10, "day" -> 5),
            ListMap("year" -> 2019, "month" -> 10, "day" -> 15)
        )
        val expected = "year = 2019 AND month = 10 AND day >= 5 AND day < 15"
        result should equal(expected)
    }

    it should "get between condition with calls to getThresholdCondition" in {
        val result = HivePartition.getBetweenCondition(
            ListMap("year" -> 2018, "month" -> 10),
            ListMap("year" -> 2019, "month" -> 10)
        )
        val expected = (
            "(year > 2018 OR year = 2018 AND month >= 10) AND " +
                "(year < 2019 OR year = 2019 AND month < 10)"
            )
        result should equal(expected)
    }

    it should "get snapshot condition for weekly snapshot" in {
        val result = HivePartition.getSnapshotCondition(
            new DateTime(2023, 5, 1, 0, 0),
            new DateTime(2023, 5, 8, 0, 0)
        )
        val expected = "snapshot = '2023-05-01'"
        assert(result == expected)
    }

    it should "get snapshot condition for monthly snapshot" in {
        val result = HivePartition.getSnapshotCondition(
            new DateTime(2023, 1, 1, 0, 0),
            new DateTime(2023, 2, 1, 0, 0)
        )
        val expected = "snapshot = '2023-01'"
        assert(result == expected)
    }

    it should "get snapshot condition for value-defined snapshot" in {
        val result = HivePartition.getSnapshotCondition("test_snapshot")
        val expected = "snapshot = 'test_snapshot'"
        assert(result == expected)
    }

}
