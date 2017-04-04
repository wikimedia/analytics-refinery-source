package org.wikimedia.analytics.refinery.core

import org.apache.spark.sql.types._
import org.scalatest.{Matchers, FlatSpec}

import org.wikimedia.analytics.refinery.core.SparkSQLHiveExtensions._

class TestSparkSQLHiveExtensions extends FlatSpec with Matchers {

    val tableName = "test.table"
    val tableLocation = "/tmp/test/table"


    it should "build a Hive DDL string representing a simple column" in {
        val field = StructField("f1", LongType, nullable = false)
        val expected = s"`f1` bigint NOT NULL"

        field.hiveColumnDDL should equal(expected)
    }

    it should "build Hive DDL string representing a complex column" in {
        val field = StructField("S1", StructType(Seq(
            StructField("s1", StringType, nullable=true)
        )))
        val expected = s"`S1` struct<`s1`:string>"

        field.hiveColumnDDL should equal(expected)
    }


    it should "build a Hive DDL string representing multiple columns" in {
        val fields = Seq(
            StructField("f1", StringType, nullable = false),
            StructField("f2", IntegerType, nullable = true),
            StructField("S1", StructType(Seq(
                StructField("s1", StringType, nullable=true)
            )))
        )
        val expected =
            s"""`f1` string NOT NULL,
               |`f2` int,
               |`S1` struct<`s1`:string>""".stripMargin

        StructType(fields).hiveColumnsDDL() should equal(expected)
    }

    it should "merge and normalize 2 simple schemas" in {
        val schema1 = StructType(Seq(
            StructField("F1", IntegerType, nullable = true),
            StructField("f2", StringType, nullable = true)
        ))

        val schema2 = StructType(Seq(
            StructField("f4", LongType, nullable = true),
            StructField("f2", StringType, nullable = true),
            StructField("f3", FloatType, nullable = true)

        ))

        // Unioned schema will be first ordred by schema1 fields, with any extra fields
        // from schema2 appended in the order they are found there.
        val expected = StructType(Seq(
            StructField("f1", LongType, nullable = true),
            StructField("f2", StringType, nullable = true),
            StructField("f4", LongType, nullable = true),
            StructField("f3", DoubleType, nullable = true)
        ))

        schema1.merge(schema2) should equal(expected)
    }


    it should "merge and not normalize 2 simple schemas" in {
        val schema1 = StructType(Seq(
            StructField("F1", IntegerType, nullable = true),
            StructField("f2", StringType, nullable = true)
        ))

        val schema2 = StructType(Seq(
            StructField("f4", LongType, nullable = true),
            StructField("f2", StringType, nullable = true),
            StructField("f3", IntegerType, nullable = true)

        ))

        // Unioned schema will be first ordred by schema1 fields, with any extra fields
        // from schema2 appended in the order they are found there.
        val expected = StructType(Seq(
            StructField("F1", LongType, nullable = true),
            StructField("f2", StringType, nullable = true),
            StructField("f4", LongType, nullable = true),
            StructField("f3", LongType, nullable = true)
        ))

        schema1.merge(schema2, normalize=false) should equal(expected)
    }

    it should "merge and not normalize 2 complex schemas" in {
        val schema1 = StructType(Seq(
            StructField("F1", IntegerType, nullable = true),
            StructField("f2", StructType(Seq(
                StructField("S1", StringType, nullable = true),
                StructField("s2", StringType, nullable = true),
                StructField("A1", StructType(Seq(
                    StructField("c1", StringType, nullable=true)
                )))
            )), nullable = true),
            StructField("f3", StringType, nullable = true)
        ))

        val schema2 = StructType(Seq(
            StructField("F1", IntegerType, nullable = true),
            StructField("f2", StructType(Seq(
                StructField("S1", StringType, nullable = true),
                StructField("s3", StringType, nullable = true),
                StructField("A1", StructType(Seq(
                    StructField("c1", StringType, nullable=true),
                    StructField("c2", StringType, nullable=true)
                )))
            )), nullable = true),
            StructField("F5", StructType(Seq(
                StructField("b1", IntegerType, nullable=true))
            )),
            StructField("f4", LongType, nullable = true)
        ))

        // Merged schema will be first ordered by schema1 fields, with any extra fields
        // from schema2 appended in the order they are found there.
        val expected = StructType(Seq(
            StructField("F1", LongType, nullable = true),
            StructField("f2", StructType(Seq(
                StructField("S1", StringType, nullable = true),
                StructField("s2", StringType, nullable = true),
                StructField("A1", StructType(Seq(
                    StructField("c1", StringType, nullable=true),
                    StructField("c2", StringType, nullable=true)
                ))),
                StructField("s3", StringType, nullable = true)
            )), nullable = true),
            StructField("f3", StringType, nullable = true),
            StructField("F5", StructType(Seq(
                StructField("b1", LongType, nullable=true))
            )),
            StructField("f4", LongType, nullable = true)
        ))

        schema1.merge(schema2, normalize=false) should equal(expected)
    }


    it should "merge and normalize 2 complex schemas" in {
        val schema1 = StructType(Seq(
            StructField("F1", IntegerType, nullable = true),
            StructField("f2", StructType(Seq(
                StructField("S1", StringType, nullable = true),
                StructField("s2", StringType, nullable = true),
                StructField("A1", StructType(Seq(
                    StructField("c1", StringType, nullable=true)
                )))
            )), nullable = true),
            StructField("f3", StringType, nullable = true)
        ))

        val schema2 = StructType(Seq(
            StructField("F1", IntegerType, nullable = true),
            StructField("f2", StructType(Seq(
                StructField("S1", StringType, nullable = true),
                StructField("s3", StringType, nullable = true),
                StructField("A1", StructType(Seq(
                    StructField("c1", StringType, nullable=true),
                    StructField("c2", StringType, nullable=true)
                )))
            )), nullable = true),
            StructField("F5", StructType(Seq(
                StructField("b1", IntegerType, nullable=true))
            )),
            StructField("f4", LongType, nullable = true)
        ))

        // Merged schema will be first ordred by schema1 fields, with any extra fields
        // from schema2 appended in the order they are found there.
        val expected = StructType(Seq(
            StructField("f1", LongType, nullable = true),
            StructField("f2", StructType(Seq(
                StructField("S1", StringType, nullable = true),
                StructField("s2", StringType, nullable = true),
                StructField("A1", StructType(Seq(
                    StructField("c1", StringType, nullable=true),
                    StructField("c2", StringType, nullable=true)
                ))),
                StructField("s3", StringType, nullable = true)
            )), nullable = true),
            StructField("f3", StringType, nullable = true),
            StructField("f5", StructType(Seq(
                StructField("b1", LongType, nullable=true))
            )),
            StructField("f4", LongType, nullable = true)
        ))

        schema1.merge(schema2, normalize=true) should equal(expected)
    }


    it should "build non external no partitions create DDL with single schema" in {
        val schema = StructType(Seq(
            StructField("f2", LongType, nullable = true),
            StructField("f3", StringType, nullable = true),
            StructField("F1", IntegerType, nullable = true)
        ))

        val expected =
            s"""CREATE TABLE `$tableName` (
               |`f2` bigint,
               |`f3` string,
               |`f1` bigint
               |)
               |-- No partition provided
               |STORED AS PARQUET""".stripMargin

        val statement = schema.hiveCreateDDL(tableName)

        statement should equal(expected)
    }

    it should "build external no partitions create DDL with single schema" in {
        val schema = StructType(Seq(
            StructField("F1", IntegerType, nullable = true),
            StructField("f2", LongType, nullable = true),
            StructField("f3", StringType, nullable = true)
        ))

        val expected =
            s"""CREATE EXTERNAL TABLE `$tableName` (
               |`f1` bigint,
               |`f2` bigint,
               |`f3` string
               |)
               |-- No partition provided
               |STORED AS PARQUET
               |LOCATION '$tableLocation'""".stripMargin

        val statement = schema.hiveCreateDDL(tableName, tableLocation)

        statement should equal(expected)
    }

    it should "build external create DDL with partitions and single schema" in {
        val schema = StructType(Seq(
            StructField("F1", IntegerType, nullable = true),
            StructField("f2", LongType, nullable = true),
            StructField("f4", StringType, nullable = true),
            StructField("f3", StringType, nullable = true)
        ))
        val partitions = Seq("f3", "f4")

        val expected =
            s"""CREATE EXTERNAL TABLE `$tableName` (
               |`f1` bigint,
               |`f2` bigint
               |)
               |PARTITIONED BY (
               |`f3` string,
               |`f4` string
               |)
               |STORED AS PARQUET
               |LOCATION '$tableLocation'""".stripMargin

        val statement = schema.hiveCreateDDL(tableName, tableLocation, partitions)

        statement should equal(expected)
    }


    it should "build alter DDL with single schema" in {
        val schema = StructType(Seq(
            StructField("F1", IntegerType, nullable = true),
            StructField("f2", LongType, nullable = true),
            StructField("f3", StringType, nullable = true)
        ))

        val otherSchema = StructType(Seq(
            StructField("F1", IntegerType, nullable = true),
            StructField("f3", StringType, nullable = true),
            StructField("f4", LongType, nullable = true)
        ))

        val partitions = Seq("f1", "f2")

        val expected = Seq(
            s"""ALTER TABLE `$tableName`
               |ADD COLUMNS (
               |`f4` bigint
               |)""".stripMargin
        )

        val statements = schema.hiveAlterDDL(tableName, otherSchema)

        statements should equal(expected)
    }


    it should "build alter DDL with just modified merged structs" in {
        val schema = StructType(Seq(
            StructField("F1", IntegerType, nullable = true),
            StructField("f2", StructType(Seq(
                StructField("S1", StringType, nullable = true),
                StructField("s2", StringType, nullable = true),
                StructField("A1", StructType(Seq(
                    StructField("c1", StringType, nullable=true)
                )))
            )), nullable = true),
            StructField("f3", StringType, nullable = true)
        ))

        val otherSchema = StructType(Seq(
            StructField("F1", IntegerType, nullable = true),
            StructField("f2", StructType(Seq(
                StructField("S1", StringType, nullable = true),
                StructField("s3", StringType, nullable = true),
                StructField("A1", StructType(Seq(
                    StructField("c1", StringType, nullable=true),
                    StructField("C2", StringType, nullable=true)
                )))
            )), nullable = true)
        ))

        val expected = Seq(
            s"""ALTER TABLE `$tableName`
               |CHANGE COLUMN `f2` `f2` struct<`S1`:string,`s2`:string,`A1`:struct<`c1`:string,`C2`:string>,`s3`:string>""".stripMargin
        )

        val statements = schema.hiveAlterDDL(tableName, otherSchema)

        statements should equal(expected)
    }

    it should "build alter DDL with new columns and modified merged structs" in {
        val schema = StructType(Seq(
            StructField("F1", IntegerType, nullable = true),
            StructField("f2", StructType(Seq(
                StructField("S1", StringType, nullable = true),
                StructField("s2", StringType, nullable = true),
                StructField("A1", StructType(Seq(
                    StructField("c1", StringType, nullable=true)
                )))
            )), nullable = true),
            StructField("f3", StringType, nullable = true)
        ))

        val otherSchema = StructType(Seq(
            StructField("F1", IntegerType, nullable = true),
            StructField("f2", StructType(Seq(
                StructField("S1", StringType, nullable = true),
                StructField("s3", StringType, nullable = true),
                StructField("A1", StructType(Seq(
                    StructField("c1", StringType, nullable=true),
                    StructField("c2", StringType, nullable=true)
                )))
            )), nullable = true),
            StructField("f4", StructType(Seq(
                StructField("b1", IntegerType, nullable=true))
            )),
            StructField("f5", LongType, nullable = true)
        ))

        val expected = Seq(
            s"""ALTER TABLE `$tableName`
               |ADD COLUMNS (
               |`f4` struct<`b1`:bigint>,
               |`f5` bigint
               |)""".stripMargin,
            s"""ALTER TABLE `$tableName`
               |CHANGE COLUMN `f2` `f2` struct<`S1`:string,`s2`:string,`A1`:struct<`c1`:string,`c2`:string>,`s3`:string>""".stripMargin
        )

        val statements = schema.hiveAlterDDL(tableName, otherSchema)

        statements should equal(expected)
    }

}
