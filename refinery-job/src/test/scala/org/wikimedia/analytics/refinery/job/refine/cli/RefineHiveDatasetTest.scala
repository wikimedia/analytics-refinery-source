package org.wikimedia.analytics.refinery.job.refine.cli

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.core.PathUtils
import org.wikimedia.analytics.refinery.job.refine.RawRefineDataReader
import org.wikimedia.analytics.refinery.job.refine.cli.RefineHiveDataset.Config

import java.nio.file._

class RefineHiveDatasetTest extends FlatSpec with Matchers with DataFrameSuiteBase with BeforeAndAfterEach {

    var tempDir: Path = _

    override def afterEach(): Unit = {
        spark.sql("DROP DATABASE IF EXISTS db CASCADE")
        PathUtils.recursiveDelete(tempDir)
    }

    override def beforeEach(): Unit = {
        setupTable()
    }

    def setupTable(): DataFrame = {
        tempDir = Files.createTempDirectory("DataFrameToTableTest_db_table")
        spark.sql("create database db;")
        spark.sql(s"create table db.table (wiki string, year int, month int) USING PARQUET location '$tempDir' partitioned by (year, month)")
    }

    "apply" should "read input data and write into a table" in {
        val schema: StructType = StructType(Seq(
            StructField("wiki", StringType, nullable = true),
            StructField("year", IntegerType, nullable = true),
            StructField("month", IntegerType, nullable = true),
        ))
        val reader = RawRefineDataReader(
            spark = spark,
            sparkSchema = schema,
        )
        val resourcesPath = getClass.getResource("/")
        val config = new Config(
            input_paths=Seq(s"${resourcesPath}event_data/raw/event/eqiad_table_b/hourly/2021/03/22/19/"),
            schema_uri="schema_uri",
            table="db.table",
            partition_paths=Seq("year=2021/month=03"),
            schema_base_uris = Seq("http://base_uri_mockup.org")
        )
        RefineHiveDataset.apply(reader, config)
        val df = spark.sql("select count(*) from db.table")
        df.collect()(0)(0) should equal(1)
    }

}
