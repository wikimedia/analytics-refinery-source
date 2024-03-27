package org.wikimedia.analytics.refinery.job.refine

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}
import org.scalamock.scalatest.MockFactory

class TestRefineHiveDataset extends FlatSpec
    with Matchers with DataFrameSuiteBase with BeforeAndAfterEach with MockFactory {

    val input_schema_base_uris: Seq[String] = Seq("https://schema.discovery.wmnet/repositories/secondary/jsonschema/")

    "Config" should "validate arguments" in {

        // This should not raise an error
        new RefineHiveDataset.Config(
            "json",
            Seq("equiad", "codfw").map{ dc =>
                s"/wmf/data/raw/eventlogging_legacy/${dc}.eventlogging_NavigationTiming/year=2024/month=02/day=15/hour=00"
            },
            input_schema_base_uris,
            "analytics/legacy/navigationtiming/1.6.0",
            "event.navigationtiming",
            Seq("equiad", "codfw").map{ dc => s"datacenter=$dc/year=2024/month=2/day=15/hour=0" },
        )

        // This should raise an error
        assertThrows[java.lang.IllegalArgumentException] {
            new RefineHiveDataset.Config(
                "json",
                input_schema_base_uris,
                Seq("https://schema.discovery.wmnet/repositories/secondary/jsonschema/"),
                "analytics/legacy/navigationtiming/1.6.0",
                "eventinvalidnavigationtiming",
                Seq("datacenter/eqiad/year/2024/month/2")  // Invalid partition
            )
        }

        // This should raise an error
        assertThrows[java.lang.IllegalArgumentException] {
            new RefineHiveDataset.Config(
                "json",
                Seq("/wmf/data/raw/eventlogging_legacy/eventlogging_NavigationTiming/year=2024/month=02/day=15/hour=00"),
                input_schema_base_uris,
                "analytics/legacy/navigationtiming/1.6.0",
                "eventinvalidnavigationtiming",
                // partitions and input dirs do not match
                Seq("equiad", "codfw").map{ dc => s"datacenter=$dc/year=2024/month=2/day=15/hour=0" },
            )
        }
    }

    var tableDir: Path = _
    var rawDataDir: Path = _

    private def rmDirIfExists(path: Path) = {
        if (path != null && Files.exists(path)) {
            Files.walkFileTree(path, new SimpleFileVisitor[Path]() {
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

    override def afterEach(): Unit = {
        spark.sql("DROP DATABASE IF EXISTS db CASCADE")
        rmDirIfExists(tableDir)
        rmDirIfExists(rawDataDir)
    }

    "RefineHiveDataset" should "refine a Hive dataset - Integration test" in {
        tableDir = Files.createTempDirectory("RefineHiveDatasetTest_db_table")
        rawDataDir = Files.createTempDirectory("RefineHiveDatasetTest_raw_db_table")

        spark.sql("create database db;")
        spark.sql(s"create table db.table (wiki string, year int, month int) USING PARQUET location '$tableDir' partitioned by (year, month)")

        // Prepare existing data in table
        spark.createDataFrame(Seq(
                ("enwiki_not_replaced", 2024, 1),
                ("enwiki_replaced", 2024, 2),
                ("dewiki_replaced", 2024, 2)
            )).toDF("wiki", "year", "month")
            .write
            .mode("overwrite")
            .insertInto("db.table")

        // Prepare raw data
        spark.createDataFrame(Seq(
                ("/refine_hive_dataset_test/event/1.0.0", "enwiki", 2024, 2),
                ("/refine_hive_dataset_test/event/1.0.0", "dewiki", 2024, 2)
            )).toDF("$schema", "wiki", "year", "month")
            .write
            .json(rawDataDir.toString+"/part")

        // Mocking eventutilities.core.event.EventSchemaLoader
        val schema = Some(StructType(Seq(
                StructField("wiki", StringType, nullable = true),
                StructField("year", IntegerType, nullable = true),
                StructField("month", IntegerType, nullable = true)
            )))
        val mockedLoader = mock[EventSparkSchemaLoader]
        (mockedLoader.loadSchema(_: RefineTarget)).expects(*).returning(schema)

        val config = new RefineHiveDataset.Config(
            "json",
            Seq(rawDataDir.toString+"/part"),
            input_schema_base_uris,
            "/refine_hive_dataset_test/event/1.0.0",
            "db.table",
            Seq("year=2024/month=2")
        )

        RefineHiveDataset(spark)(config, Some(mockedLoader))

        val result: Array[Row] = spark.sql("select * from db.table sort by year desc, month desc, wiki desc").collect()
        result.length should equal(3)
        result.map(_.getString(0)) should contain allElementsOf Seq("enwiki", "dewiki", "enwiki_not_replaced")
    }
}
