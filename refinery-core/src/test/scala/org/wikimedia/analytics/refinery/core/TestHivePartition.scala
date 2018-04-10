package org.wikimedia.analytics.refinery.core

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.ListMap
import scala.util.matching.Regex


class TestHivePartition extends FlatSpec with Matchers {

    val database = "testdb"
    val table = "Test-Table"
    val location = "/path/to/test_table"
    val partitions: ListMap[String, String] = ListMap(
        "datacenter" -> "dc1", "year" -> "2017", "month" -> "07"
    )
    val partition = HivePartition(
        database, table, location, partitions
    )

    it should "normalized fully qualified table name" in {
        partition.tableName should equal(s"`$database`.`${table.replace("-", "_")}`")
    }

    it should "have partition keys" in {
        partition.keys should equal(partition.keys.toSeq)
    }

    it should "have correct hive QL representation" in {
        partition.hiveQL should equal("""datacenter="dc1",year=2017,month=7""")
    }


    it should "have correct hive relative path representation" in {
        partition.relativePath should equal("""datacenter=dc1/year=2017/month=7""")
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

        p.tableName should equal(s"`$database`.`Test_Table2`")
        p.path should equal(s"$baseLocation/${p.table}/datacenter=dc2/year=2015/month=5")

        val partitionShouldBe = ListMap(
            "datacenter" -> "dc2", "year" -> "2015", "month" -> "05"
        )
        p.partitions should equal(partitionShouldBe)
    }

}
