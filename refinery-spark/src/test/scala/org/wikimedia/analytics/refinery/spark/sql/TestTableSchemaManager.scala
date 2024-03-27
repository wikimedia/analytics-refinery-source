package org.wikimedia.analytics.refinery.spark.sql

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.spark.sql.TableSchemaManager.{HiveTableSchemaManager, IcebergTableSchemaManager}

class TestTableSchemaManager extends FlatSpec
                                  with Matchers
                                  with DataFrameSuiteBase
                                  with BeforeAndAfterEach {

    val normalizeFieldNamesAndWidenTypes: PartitionedDataFrame => PartitionedDataFrame = partDf => {
        import org.wikimedia.analytics.refinery.spark.sql.HiveExtensions._
        partDf.copy(df = partDf.df.normalizeAndWiden().emptyMetadata)
    }

    override def afterEach(): Unit = {
        spark.sql("DROP DATABASE IF EXISTS db CASCADE")
        super.afterEach()
    }

    "icebergGetTableProperties" should "get table properties" in {
        spark.sql("create database db;")
        spark.sql("""
            create table db.table (
                wiki string,
                day date
            )
            USING PARQUET -- Should be Iceberg but, the lib is provided. Does not change the test though.
            TBLPROPERTIES ( key = 'value' );
            """)
        val expected = Map("key" -> "value")
        IcebergTableSchemaManager.icebergGetTableProperties(spark, "db.table") should equal(expected)
    }


    "icebergGetTablePropertiesAlterDDLs" should "generate update ddl statements to update the table properties" in {
        spark.sql("create database db;")
        spark.sql("""
            create table db.table (
                wiki string,
                day date
            )
            USING PARQUET -- Should be Iceberg but, the lib is provided. Does not change the test though.
            TBLPROPERTIES ( k1 = 'v1', k2 = 'v2', k3 = 'v3');
            """)
        val updates = Map(
            "k1" -> "v1b", // to update
            "k2" -> "v2", // to keep
            "k4" -> "v4", // to add
            // "k3" -> "v3", // to remove
        )
        val expectedDDLs: Seq[String] = Seq(
            "ALTER TABLE db.table UNSET TBLPROPERTIES (k3);",
            "ALTER TABLE db.table SET TBLPROPERTIES (k1 = 'v1b', k4 = 'v4');",
        )
        val ddls = IcebergTableSchemaManager.icebergGetTablePropertiesAlterDDLs(spark, "db.table", updates)
        ddls should equal(expectedDDLs)


        val updatesNoUnset = Map(
            "k1" -> "v1b",  // to update
            "k2" -> "v2",  // Untouched
            "k3" -> "v3",  // Untouched
        )
        val expectedDDLsWithoutUnset: Seq[String] = Seq(
            "ALTER TABLE db.table SET TBLPROPERTIES (k1 = 'v1b');",
        )
        val notUnsetDDLs = IcebergTableSchemaManager
            .icebergGetTablePropertiesAlterDDLs(spark, "db.table", updatesNoUnset)
        notUnsetDDLs should equal(expectedDDLsWithoutUnset)

        val noUpdates = Map(
            "k1" -> "v1",  // Untouched
            "k2" -> "v2",  // Untouched
            "k3" -> "v3",  // Untouched
        )
        val expectedDDLsNoUpdates: Seq[String] = Seq.empty
        val noDDLs = IcebergTableSchemaManager
            .icebergGetTablePropertiesAlterDDLs(spark, "db.table", noUpdates)
        noDDLs should equal(expectedDDLsNoUpdates)
    }

    "icebergGetDDLStatements" should "generate statements to update a table" in {
        spark.sql("create database db;")
        spark.sql("""
            create table db.table (
                wiki string,
                day date,
                `meta` STRUCT<`dt`:STRING COMMENT 'c1'> COMMENT 'c2'
            )
            USING PARQUET -- Should be Iceberg but, the lib is provided. Does not change the test though.
            TBLPROPERTIES ( key = 'value' );
            """)
        val newSchema: StructType = StructType(Seq(
            StructField("wiki", StringType),
            StructField("day", DateType),
            StructField("meta", StructType(Seq(
                StructField("dt", TimestampType).withComment("c3")
            ))),
            StructField("dummy", StringType),
        ))
        IcebergTableSchemaManager.icebergGetDDLStatements(
            spark,
            newSchema,
            "db.table",
            Some("location"),
            Seq("meta.dt"),
            Map("key" -> "value")
        ) should equal(Seq(
            "ALTER TABLE db.table ALTER COLUMN `meta` COMMENT '' ;",
            "ALTER TABLE db.table ALTER COLUMN `meta`.`dt` COMMENT 'c3' ;",
            "ALTER TABLE db.table ALTER COLUMN `meta`.`dt` TYPE TIMESTAMP ;",
            """ALTER TABLE db.table
            |ADD COLUMNS (
            |    `dummy` STRING
            |);""".stripMargin
        ))
    }

    it should "generate statements to create a table preserving comments" in {
        spark.sql("create database db;")
        val newSchema: StructType = StructType(Seq(
            StructField("wiki", StringType, nullable = true).withComment("c1"),
            StructField("day", DateType),
            StructField("meta", StructType(Seq(
                StructField("dt", TimestampType).withComment("c3")
            ))),
            StructField("dummy", StringType),
        ))
        IcebergTableSchemaManager.icebergGetDDLStatements(
            spark,
            newSchema,
            "db.table",
            Some("location"),
            Seq("meta.dt"),
            Map("key" -> "value")
        ) should equal(Seq(
            """CREATE TABLE db.table (
              |`wiki` STRING COMMENT 'c1',
              |`day` DATE,
              |`meta` STRUCT<`dt`: TIMESTAMP COMMENT 'c3'>,
              |`dummy` STRING
              |)
              |USING iceberg
              |PARTITIONED BY (meta.dt)
              |TBLPROPERTIES(key = 'value')
              |LOCATION 'location'
              |""".stripMargin
        ))
    }

    "hiveGetDDLStatements" should "generate statements to create a table" in {
        spark.sql("create database db;")
        val newSchema: StructType = StructType(Seq(
            // Root comments are not preserved by current implementation for legacy reasons.
            StructField("wiki", StringType, nullable = true).withComment("c1"),
            StructField("day", StringType),
            StructField("meta", StructType(Seq(
                StructField("dt", StringType).withComment("c3")
            ))),
            StructField("dummy", StringType),
        ))
        HiveTableSchemaManager.hiveGetDDLStatements(spark, newSchema, "db.table", Some("location"), List("day")) should equal(Seq(
            """CREATE EXTERNAL TABLE db.table (
              |`wiki` STRING,
              |`meta` STRUCT<`dt`: STRING COMMENT 'c3'>,
              |`dummy` STRING
              |)
              |PARTITIONED BY (
              |`day` STRING
              |)
              |STORED AS PARQUET
              |LOCATION 'location'""".stripMargin
        ))
    }

    it should "generate statements to create a table partitioned by datacenter and time" in {
        spark.sql("create database db;")
        val newSchema: StructType = StructType(Seq(
            StructField("wiki", StringType),
            StructField("datacenter", StringType),
            StructField("year", LongType),
            StructField("month", LongType),
            StructField("day", StringType),
            StructField("hour", LongType),
        ))
        val partitions = List("datacenter", "year", "month", "day", "hour")
        HiveTableSchemaManager.hiveGetDDLStatements(spark, newSchema, "db.table", Some("location"), partitions) should equal(Seq(
            """CREATE EXTERNAL TABLE db.table (
              |`wiki` STRING
              |)
              |PARTITIONED BY (
              |`datacenter` STRING,
              |`year` BIGINT,
              |`month` BIGINT,
              |`day` STRING,
              |`hour` BIGINT
              |)
              |STORED AS PARQUET
              |LOCATION 'location'""".stripMargin
        ))
    }

    it should "generate statements to update a table" in {
        spark.sql("create database db;")
        // Note: Dropping columns is not supported.
        spark.sql("""
            create table db.table (
                could_by_dropped string,
                wiki string,
                day string,
                `meta` STRUCT<`dt`:STRING COMMENT 'c1'> COMMENT 'c2'
            )
            USING PARQUET
            TBLPROPERTIES ( key = 'value' );
            """)
        val newSchema: StructType = StructType(Seq(
            StructField("wiki", StringType),
            StructField("day", StringType),
            StructField("meta", StructType(Seq(
                StructField("dt", IntegerType).withComment("c3"),
                StructField("new_field_in_struct", StringType),
            ))),
            StructField("dummy", StringType),
            StructField("new_field", StringType).withComment("c5")
        ))
        HiveTableSchemaManager.hiveGetDDLStatements(spark, newSchema, "db.table", Some("location"), List("day")) should equal(Seq(
            """ALTER TABLE db.table
              |ADD COLUMNS (
              |`dummy` STRING,
              |`new_field` STRING
              |)""".stripMargin,
            """ALTER TABLE db.table
              |CHANGE COLUMN `meta` `meta` STRUCT<`dt`: STRING COMMENT 'c1', `new_field_in_struct`: STRING>""".stripMargin
        ))
    }

    private def createTestTable(columns: String): DataFrame = {
        spark.sql("create database db;")
        spark.sql(
            s"""
            CREATE TABLE db.table ($columns )
            USING PARQUET;
            """)
    }

    it should "not generate statements to update a column to string, in this version of the code" in {
        createTestTable("a BIGINT, b INT")
        val newSchema: StructType = StructType(Seq(
            StructField("a", StringType),
            StructField("b", StringType),
        ))
        HiveTableSchemaManager.hiveGetDDLStatements(spark, newSchema, "db.table", Some("location"), List.empty) should equal(Seq())
    }

// The following tests needs an Hive Server running. They are disabled for now.
//    * Option 1: We add the libs to run a Hive Server in the test.
//    * Option 2: We convert to use Spark SQL to run the tests.

//    "hiveEvolveTable" should "create a table with complex nested structures" in {
//        spark.sql("create database db;")
//        val newSchema: StructType = StructType(Seq(
//            StructField("day", StringType),
//            StructField("b", LongType),
//            StructField("h", StructType(Seq(
//                StructField("h1", StringType),
//                StructField("h2", LongType)
//            ))),
//            StructField("arr", ArrayType(StringType)),
//            StructField("arr_h", ArrayType(StructType(Seq(
//                StructField("h3", StringType)
//            )))),
//            StructField("map_h", MapType(StringType, StructType(Seq(
//                StructField("h4", StringType)
//            )))
//            )))
//        evolveTestTableWith(newSchema)
//        spark.table("db.table").schema.fields.toSeq should contain theSameElementsAs(
//            newSchema.fields ++ Seq(StructField("dummy", StringType))
//            )
//    }
//
//private def evolveTestTableWith(newSchema: StructType) = {
//    HiveTableSchemaManager.hiveEvolveTable(
//        spark,
//        newSchema,
//        "db.table",
//        "location",
//        Seq(dummy_transform),
//        Seq("day")
//    )
//}
//
//    it should "update a table by adding complex nested structures" in {
//        createTestTable("a STRING")
//        val newSchema: StructType = StructType(Seq(
//            StructField("a", StringType),
//            StructField("b", LongType),
//            StructField("h", StructType(Seq(
//                StructField("h1", StringType),
//                StructField("h2", LongType)
//            ))),
//            StructField("arr", ArrayType(StringType)),
//            StructField("arr_h", ArrayType(StructType(Seq(
//                StructField("h3", StringType)
//            )))),
//            StructField("map_h", MapType(StringType, StructType(Seq(
//                StructField("h4", StringType)
//            ))))
//        ))
//        evolveTestTableWith(newSchema)
//        spark.table("db.table").schema.fields should contain theSameElementsAs (
//            newSchema.fields ++ Seq(StructField("dummy", StringType))
//            )
//    }
//
//    it should "not modify complex structure" in {
//        createTestTable("a STRING, h STRUCT<h1: STRING, h2: LONG>")
//        val newSchema: StructType = StructType(Seq(
//            StructField("a", StringType),
//            StructField("h", StructType(Seq(
//                StructField("h1", StringType),
//                StructField("h2", StringType)  // Different type
//            )))
//        ))
//        evolveTestTableWith(newSchema)
//        spark.table("db.table").schema.fields should contain theSameElementsAs Seq(
//            StructField("a", StringType),
//            StructField("h", StructType(Seq(
//                StructField("h1", StringType),
//                StructField("h2", LongType)
//            ))),
//            StructField("dummy", StringType),
//        )
//    }
//
//    it should "not allow switch to narrower type" in {
//        createTestTable("a LONG")
//        val newSchema: StructType = StructType(Seq(StructField("a", IntegerType)))
//        evolveTestTableWith(newSchema)
//        spark.table("db.table").schema.fields should contain theSameElementsAs Seq(
//            StructField("a", LongType),
//            StructField("dummy", StringType),
//        )
//    }
//
//    it should "not allow widening of type" in {
//        createTestTable("a INT")
//        val newSchema: StructType = StructType(Seq(StructField("a", LongType)))
//        intercept[AnalysisException] {
//            evolveTestTableWith(newSchema)
//        }
//    }
}