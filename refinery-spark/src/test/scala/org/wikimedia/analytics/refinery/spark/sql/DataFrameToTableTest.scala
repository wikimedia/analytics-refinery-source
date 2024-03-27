package org.wikimedia.analytics.refinery.spark.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.lit
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.core.HivePartition
import org.wikimedia.analytics.refinery.spark.sql.HiveExtensions._

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}

class DataFrameToTableTest extends FlatSpec with Matchers with DataFrameSuiteBase with BeforeAndAfterEach {

    val dummy_transform: PartitionedDataFrame => PartitionedDataFrame = partDf => {
        val newPartDf = partDf.df.withColumn("dummy", lit("dummy"))
        PartitionedDataFrame(newPartDf, partDf.partition)
    }

    var tempDir: Path = _

    override def afterEach(): Unit = {
        spark.sql("DROP DATABASE IF EXISTS db CASCADE")

        // Delete the temporary directory after each test
        if (tempDir != null && Files.exists(tempDir)) {
            Files.walkFileTree(tempDir, new SimpleFileVisitor[Path]() {
                override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
                    Files.delete(file)
                    FileVisitResult.CONTINUE
                }

                override def postVisitDirectory(dir: Path, exc: java.io.IOException): FileVisitResult = {
                    Files.delete(dir)
                    FileVisitResult.CONTINUE
                }
            })
        }
    }

    def setupTable(): DataFrame = {
        tempDir = Files.createTempDirectory("DataFrameToTableTest_db_table")
        spark.sql("create database db;")
        spark.sql(s"create table db.table (wiki string, year int, month int) USING PARQUET location '$tempDir' partitioned by (year, month)")
    }

    "hiveInsertOverwritePartition" should "replace data into a single partition" in {
        setupTable()
        // Prepare existing data in partition
        spark.createDataFrame(Seq(
            ("enwiki_not_replaced", 2024, 1),
            ("enwiki_replaced", 2024, 2),
            ("dewiki_replaced", 2024, 2)
        )).toDF("wiki", "year", "month")
            .write
            .mode("overwrite")
            .insertInto("db.table")

        val df = spark.createDataFrame(Seq(
            Tuple1("enwiki"),
            Tuple1("dewiki")
        )).toDF("wiki")

        // Here we are just using HivePartition for nice toString and logging purposes.
        val hivePartition = HivePartition(
            "db",
            "table",
            "/fake/location", // location not really needed here, but it is nice for logging
            "year=2024/month=2" // partitionPath
        )

        val dfWithPartitions = df.addPartitionColumnValues(hivePartition.partitions)

        DataFrameToTable.hiveInsertOverwrite(dfWithPartitions, hivePartition.tableName, 1)

        val result: Array[Row] = spark.sql("select * from db.table sort by year desc, month desc, wiki desc").collect()
        result.length should equal(3)
        result.map(_.getString(0)) should contain allElementsOf Seq("enwiki", "dewiki", "enwiki_not_replaced")
    }

    it should "add datacenter partition" in {
        setupTable()
        spark.sql("alter table db.table add columns (datacenter string, day int, hour int);")

        val df = spark.createDataFrame(Seq(
            Tuple1("enwiki"),
            Tuple1("dewiki")
        )).toDF("wiki")

        // Here we are just using HivePartition for nice toString and logging purposes.
        val hivePartition = HivePartition(
            "table",
            "db",
            "/fake/location", // location not really needed here, but it is nice for logging
            "datacenter=eqiad/year=2024/month=2/day=15/hour=0" // partitionPath
        )

        val dfWithPartitions = df.addPartitionColumnValues(hivePartition.partitions)

        DataFrameToTable.hiveInsertOverwrite(dfWithPartitions, "db.table", 1)

        val result:Row = spark.sql("select datacenter, day, hour from db.table limit 1").collect()(0)
        (result.getString(0), result.getInt(1), result.getInt(2)) should equal(("eqiad", 15, 0))
    }

}
