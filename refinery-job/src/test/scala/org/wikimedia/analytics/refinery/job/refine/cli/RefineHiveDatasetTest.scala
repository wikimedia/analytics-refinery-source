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

    // Custom transform function to filter out records with wiki="to_be_deleted"
    object filterToBeDeleted extends org.wikimedia.analytics.refinery.job.refine.RefineHelper.TransformFunction {
        def apply(partDf: org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame): org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame = {
            partDf.copy(df = partDf.df.filter("wiki != 'to_be_deleted' OR wiki IS NULL"))
        }
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

    "apply" should "add two partitions when 1/2 input path contains filtered data" in {
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
        val inputWithData = s"${resourcesPath}event_data/raw/event/simple_schema/regular/"
        val inputWithFilteredData = s"${resourcesPath}event_data/raw/event/simple_schema/with_wiki_to_delete/"
        
        val config = new Config(
            input_paths=Seq(inputWithData, inputWithFilteredData),
            schema_uri="schema_uri",
            table="db.table",
            partition_paths=Seq("year=2021/month=03", "year=2021/month=04"),
            schema_base_uris = Seq("http://base_uri_mockup.org"),
            transform_functions = Seq(filterToBeDeleted.apply)
        )
        RefineHiveDataset.apply(reader, config)
        val partitions = spark.sql("SHOW PARTITIONS db.table").collect().map(_(0)).toSet
        partitions should contain ("year=2021/month=3")
        partitions should contain ("year=2021/month=4")
        partitions.size should equal (2)
        
        // Verify that the filtered data was removed (second partition should be empty)
        val partition20Count = spark.sql("SELECT COUNT(*) FROM db.table WHERE year=2021 AND month=3").collect()(0)(0)
        partition20Count should equal (1)
        
        // Verify that the first partition still has data
        val partition19Count = spark.sql("SELECT COUNT(*) FROM db.table WHERE year=2021 AND month=4").collect()(0)(0)
        partition19Count should equal (0)
    }

}
