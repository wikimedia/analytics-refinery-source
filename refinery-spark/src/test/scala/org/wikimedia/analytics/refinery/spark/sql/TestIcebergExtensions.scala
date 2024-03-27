package org.wikimedia.analytics.refinery.spark.sql

import org.apache.spark.sql.types.{StructField, _}
import org.scalatest.{FlatSpec, Matchers}

class TestIcebergExtensions extends FlatSpec with Matchers {

    import IcebergExtensions._

   "fieldNameFromPartitionDefinition" should "extract column names from partition definitions" in {
        // Valid cases
        IcebergExtensions.fieldNameFromPartitionDefinition("column") should equal("column")
        IcebergExtensions.fieldNameFromPartitionDefinition("year(column)") should equal("column")
        IcebergExtensions.fieldNameFromPartitionDefinition("bucket(12, column)") should equal("column")
        // Invalid cases
        IcebergExtensions.fieldNameFromPartitionDefinition("bucket(12, column") should equal("bucket(12, column")
        IcebergExtensions.fieldNameFromPartitionDefinition("bucket(column),12") should equal("bucket(column),12")
        IcebergExtensions.fieldNameFromPartitionDefinition("(,column)") should equal("(,column)")
    }

    "icebergCreateDDL" should "generate an Iceberg CREATE TABLE statement" in {
        val schema = StructType(Seq(
            // FieldAlteration.alterCommentDDL
            StructField("a", StringType, nullable = true),
            StructField("dt", TimestampType, nullable = true),
            StructField("b", StructType(Seq(
                StructField("c", LongType, nullable = true),
                StructField("ts", TimestampType, nullable = true)
            )), nullable = true),
            // Map field with key and values as strings
            StructField("map1", MapType(StringType, StringType), nullable = true),
            // Map field with struct values
            StructField("map2", MapType(StringType, StructType(Seq(
                StructField("d", LongType, nullable = true),
                StructField("e", TimestampType, nullable = true)
            ))), nullable = true),
        ))

        val createStatement = schema.icebergCreateDDL(
            "catalog.database.table",
            "hdfs:///test/location",
            Seq("a", "year(b.ts)"),
            Map("key" -> "value")
        )

        val expectedCreateStatement =
            """CREATE TABLE catalog.database.table (
              |`a` STRING,
              |`dt` TIMESTAMP,
              |`b` STRUCT<`c`: BIGINT, `ts`: TIMESTAMP>,
              |`map1` MAP<STRING, STRING>,
              |`map2` MAP<STRING, STRUCT<`d`: BIGINT, `e`: TIMESTAMP>>
              |)
              |USING iceberg
              |PARTITIONED BY (a,
              |year(b.ts))
              |TBLPROPERTIES(key = 'value')
              |LOCATION 'hdfs:///test/location'
              |""".stripMargin

        createStatement should equal(expectedCreateStatement)
    }

    it should "fail to generate an Iceberg CREATE TABLE statement if a partition column is not found" in {
        val schema = StructType(Seq(
            StructField("a", StringType, nullable = true),
            StructField("b", StructType(Seq(
                StructField("c", LongType, nullable = true),
                StructField("ts", TimestampType, nullable = true)
            )), nullable = true),
        ))
        assertThrows[RuntimeException] {
            schema.icebergCreateDDL(
                "catalog.database.table",
                "hdfs:///test/location",
                Seq("month(missing_timestamp)"),
                Map("key" -> "value")
            )
        }
    }

    def buildCommentMetadata(content: String): Metadata = {
        val metadataBuilder = new MetadataBuilder()
        metadataBuilder.putString("comment", content)
        metadataBuilder.build()
    }

    val tableName = "database.table"

    "icebergAlterSchemaDDLs" should "build alter DDLs with simple schema" in {
        val schema = StructType(Seq(
            StructField("f1", IntegerType),
            StructField("f2", LongType),
            StructField("f3", StringType) // Removed
        ))

        val otherSchema = StructType(Seq(
            StructField("f1", LongType), // Type expanded
            StructField("f2", LongType), // Unchanged
            StructField("f4", StringType) // New column
        ))

        val expected = List(
            "ALTER TABLE database.table DROP COLUMN `f3` ;",
            "ALTER TABLE database.table ALTER COLUMN `f1` TYPE BIGINT ;",
            """ALTER TABLE database.table
               |ADD COLUMNS (
               |    `f4` STRING
               |);""".stripMargin
        )

        val statements = schema.icebergAlterSchemaDDLs(tableName, otherSchema)

        statements.sorted should equal(expected.sorted)
    }

    it should "build alter DDLs for nested schemas" in {
        val schema = StructType(Seq(
            // Struct
            StructField("h1", StructType(Seq(
                StructField("h1_1", IntegerType),
                StructField("h1_2", LongType),
                StructField("h1_3", StringType)  // Removed
            )))
        ))

        val otherSchema = StructType(Seq(
            StructField("h1", StructType(Seq(
                StructField("h1_1", LongType),  // Type expanded
                StructField("h1_2", LongType),  // Unchanged
                StructField("h1_4", StringType)  // New column
            )))
        ))


        val expected = List(
            """ALTER TABLE database.table
              |ADD COLUMNS (
              |    `h1`.`h1_4` STRING
              |);""".stripMargin,
            "ALTER TABLE database.table ALTER COLUMN `h1`.`h1_1` TYPE BIGINT ;",
            "ALTER TABLE database.table DROP COLUMN `h1`.`h1_3` ;",
        )

        val statements = schema.icebergAlterSchemaDDLs(tableName, otherSchema)

        statements.sorted should equal(expected.sorted)
    }

    it should "build alter DDLs for complex types in schema" in {
        val schema = StructType(Seq(
            // Struct with a nested struct
            StructField("h1", StructType(Seq(
                StructField("h1_1", StructType(Seq(
                    StructField("h1_1_1", StringType),
                    StructField("h1_1_2", IntegerType)
                ))),
                StructField("h1_2", LongType),
                StructField("h1_3", StringType) // Removed
            ))),
            // Array of strings
            StructField("a1", ArrayType(IntegerType)),
            // Array field with struct
            StructField("a2", ArrayType(StructType(Seq(
                StructField("a2_1", StringType),
                StructField("a2_2", StringType) // Removed
            )))),
            // Map of Strings
            StructField("map1", MapType(StringType, IntegerType)),
            // Map of struct values
            StructField("map2", MapType(StringType, StructType(Seq(
                StructField("map2_1", IntegerType),
                StructField("map2_2", StringType) // Removed
            ))))
        ))

        val otherSchema = StructType(Seq(
            // Struct with a nested struct
            StructField("h1", StructType(Seq(
                StructField("h1_1", StructType(Seq(
                    StructField("h1_1_1", StringType),
                    StructField("h1_1_2", LongType)  // Updated type
                ))),
                StructField("h1_2", LongType),
            ))),
            StructField("h1new", StructType(Seq( // New
                StructField("h1new_1", StructType(Seq(
                    StructField("h1new_1_1", StringType),
                ))),
            ))),
            // Array of strings
            StructField("a1", ArrayType(LongType)), // Updated type
            StructField("a1new", ArrayType(StringType)), // New
            // Array field with struct
            StructField("a2", ArrayType(StructType(Seq(
                StructField("a2_1", IntegerType)  // Updated type
            )))),
            StructField("a2new", ArrayType(StructType(Seq( // New
                StructField("a2new_1", StringType),
            )))),
            // Map of Strings
            StructField("map1", MapType(StringType, LongType)),  // Updated type
            StructField("map1new", MapType(StringType, StringType)), // New
            // Map of struct values
            StructField("map2", MapType(StringType, StructType(Seq(
                StructField("map2_1", LongType), // Updated type
            )))),
            StructField("map2new", MapType(StringType, StructType(Seq( // New
                StructField("map2new_1", StringType),
            ))))
        ))


        val expected = List(
            """|ALTER TABLE database.table
               |ADD COLUMNS (
               |    `h1new` STRUCT<`h1new_1`: STRUCT<`h1new_1_1`: STRING>>,
               |    `a1new` ARRAY<STRING>,
               |    `a2new` ARRAY<STRUCT<`a2new_1`: STRING>>,
               |    `map1new` MAP<STRING, STRING>,
               |    `map2new` MAP<STRING, STRUCT<`map2new_1`: STRING>>
               |);""".stripMargin,
            "ALTER TABLE database.table ALTER COLUMN `a1` TYPE ARRAY<BIGINT> ;",
            "ALTER TABLE database.table ALTER COLUMN `a2`.`element`.`a2_1` TYPE INT ;",
            "ALTER TABLE database.table ALTER COLUMN `h1`.`h1_1`.`h1_1_2` TYPE BIGINT ;",
            "ALTER TABLE database.table ALTER COLUMN `map1` TYPE MAP<STRING, BIGINT> ;",
            "ALTER TABLE database.table ALTER COLUMN `map2`.`map2_1` TYPE BIGINT ;",
            "ALTER TABLE database.table DROP COLUMN `a2`.`element`.`a2_2` ;",
            "ALTER TABLE database.table DROP COLUMN `h1`.`h1_3` ;",
            "ALTER TABLE database.table DROP COLUMN `map2`.`map2_2` ;"
        )

        val statements = schema.icebergAlterSchemaDDLs(tableName, otherSchema)

        statements.sorted should equal(expected.sorted)
    }

    it should "build alter DDLs for comment alterations" in {
        val schema = StructType(Seq(
            StructField("f1", IntegerType).withComment("Comment 1"),
            StructField("f2", IntegerType).withComment("Comment 2"),
            StructField("f3", IntegerType).withComment("Comment 3"),  // column with comment dropped
            StructField("f4", IntegerType).withComment("Comment 4"),
            StructField("h1", StructType(Seq(
                StructField("h1_1", IntegerType).withComment("Comment 5"),
                StructField("h1_2", LongType).withComment("Comment 6"),
                StructField("h1_3", StringType)  // Removed
            )))
        ))

        val otherSchema = StructType(Seq(
            StructField("f1", IntegerType).withComment("Comment 1 alt"),  // comment altered
            StructField("f2", IntegerType),  // comment removed
            StructField("f4", IntegerType).withComment("Comment 4"),  // Unchanged
            StructField("h1", StructType(Seq(
                StructField("h1_1", IntegerType),  // Comment removed
                StructField("h1_2", LongType).withComment("Comment 6 alt"),  // Comment changed
                StructField("h1_4", StringType).withComment("comment 7")  // New column with comment
            )))
        ))

        val expected = List(
            """ALTER TABLE database.table
            |ADD COLUMNS (
            |    `h1`.`h1_4` STRING COMMENT 'comment 7'
            |);""".stripMargin,
            "ALTER TABLE database.table ALTER COLUMN `f1` COMMENT 'Comment 1 alt' ;",
            "ALTER TABLE database.table ALTER COLUMN `f2` COMMENT '' ;",
            "ALTER TABLE database.table ALTER COLUMN `h1`.`h1_1` COMMENT '' ;",
            "ALTER TABLE database.table ALTER COLUMN `h1`.`h1_2` COMMENT 'Comment 6 alt' ;",
            "ALTER TABLE database.table DROP COLUMN `f3` ;",
            "ALTER TABLE database.table DROP COLUMN `h1`.`h1_3` ;")

        val statements = schema.icebergAlterSchemaDDLs(tableName, otherSchema)

        statements.sorted should equal(expected.sorted)
    }

    it should "produce altertable ddls for differences in comment within the array of struct" in {
        val schema = StructType(Seq(
            StructField("h1", ArrayType(StructType(Seq(
                StructField("h1_1", StringType, metadata = buildCommentMetadata("old comment"))
        ))))))

        val otherSchema = StructType(Seq(
            StructField("h1", ArrayType(StructType(Seq(
                StructField("h1_1", StringType, metadata = buildCommentMetadata("new comment"))
        ))))))

        val expected = List(
            "ALTER TABLE database.table ALTER COLUMN `h1`.`element`.`h1_1` COMMENT 'new comment' ;",
        )

        val statements = schema.icebergAlterSchemaDDLs(tableName, otherSchema)

        statements.sorted should equal(expected.sorted)
    }

    it should "find the column to alter regardless of case" in {
        val schema = StructType(Seq(
            StructField("f1", IntegerType),
            StructField("F2", IntegerType),
            StructField("H", StructType(Seq(
                StructField("h1", StringType)
            ))
        )))

        val otherSchema = StructType(Seq(
            StructField("F1", IntegerType),
            StructField("f2", IntegerType),
            StructField("F3", LongType),
            StructField("h", StructType(Seq(
                StructField("H1", StringType)
            ))
        )))

        val expected = List(
            """ALTER TABLE database.table
              |ADD COLUMNS (
              |    `F3` BIGINT
              |);""".stripMargin
        )

        val statements = schema.icebergAlterSchemaDDLs(tableName, otherSchema)

        statements.sorted should equal(expected.sorted)
    }

    "StructField.sameCommentAs" should "cleanup the comments spaces before comparing" in {
        val field = StructField("s1", StringType, metadata = buildCommentMetadata("comment with spaces  "))
        val otherField = StructField("s1", StringType, metadata = buildCommentMetadata("   comment \nwith spaces"))

        assert(field.sameCommentAs(otherField))  // No differences after cleanup.
    }

    "StructField.getCommentOrEmptyString" should "cleanup the comment" in {
        val field = StructField("s1", StringType, metadata = buildCommentMetadata("comment with \nspaces  "))
        field.getCommentOrEmptyString should equal("comment with spaces")
    }

    "StructField.icebergDDL" should "generate column DDL with comment in struct type" in {
        val schema = StructField("h1", StructType(Seq(
            StructField("h1_1", StringType, metadata = buildCommentMetadata("comment 1"))
        ))).withComment("comment 2")
        val expected = "`h1` STRUCT<`h1_1`: STRING COMMENT 'comment 1'> COMMENT 'comment 2'"
        schema.icebergDDL(Some("h1")) should equal(expected)
    }

    it should "generate column DDL with comment in array of struct type" in {
        val schema = StructField("h1", ArrayType(StructType(Seq(
            StructField("h1_1", StringType, metadata = buildCommentMetadata("comment 1"))
        )))).withComment("comment 2")
        val expected = "`h1` ARRAY<STRUCT<`h1_1`: STRING COMMENT 'comment 1'>> COMMENT 'comment 2'"
        schema.icebergDDL(Some("h1")) should equal(expected)
    }

    "StructField.getCommentAsDDL" should "cleanup comments" in {
        val schema = StructField("h1", StringType, metadata = buildCommentMetadata("comment"))
        schema.getCommentAsDDL should equal(" COMMENT 'comment'")

        val schema2 = StructField("h1", StringType, metadata = buildCommentMetadata("comment with 'quotes'"))
        schema2.getCommentAsDDL should equal(" COMMENT 'comment with \\'quotes\\''")

        val schema3 = StructField("h1", StringType, metadata = buildCommentMetadata("comment   with too much  \n\n spaces "))
        schema3.getCommentAsDDL should equal(" COMMENT 'comment with too much spaces'")
    }

    "fieldExists" should "find that fields exists in schemas" in {
        val schema = StructType(Seq(
            StructField("a", StringType),
            StructField("b", StructType(Seq(
                StructField("c", StringType),
                StructField("2.10", StringType)
            ))),
            StructField("d", MapType(StringType, StringType))
        ))
        schema.fieldExists("a") should be(true)
        schema.fieldExists("b") should be(true)
        schema.fieldExists("b.`c`") should be(true)
        schema.fieldExists("b.`2.10`") should be(true)
        schema.fieldExists("d.mapKey") should be(true)
        schema.fieldExists("c") should be(false)
    }

    "StructField.isTypeWithSubfields" should "find that a field is a struct" in {
        val schema = StructField("a", StructType(Seq(
            StructField("b", StringType),
            StructField("c", StringType)
        )))
        schema.isTypeWithSubfields should be(true)
    }

    "StructField.asStruct" should "return the inside struct when the field is complex" in {
        val insideStruct = StructType(Seq(
            StructField("b", StringType),
            StructField("c", StringType)
        ))
        StructField("a", insideStruct).asStruct should equal(insideStruct)
    }

    "StructField.fieldPathForChildren" should "return the path for children fields" in {
        val schema = StructField("a", StructType(Seq(
            StructField("b", StringType),
            StructField("c", StringType)
        )))
        schema.fieldPathForChildren(Some("parent")) should equal("parent.a")
    }

    it should "return the path for children fields within array of struct" in {
        val schema = StructField("a", ArrayType(StructType(Seq(
            StructField("b", StringType),
            StructField("c", StringType)
        ))))
        schema.fieldPathForChildren(Some("parent")) should equal("parent.a.element")
    }

    "FieldAlteration.dropDDL" should "generate drop column DDL" in {
        FieldAlteration(
            StructField("column", StringType),
            "parent.column",
            "drop"
        ).dropDDL("db.table") should equal("ALTER TABLE db.table DROP COLUMN `parent`.`column` ;")
    }

    "FieldAlteration.alterTypeDDL" should "generate alter column type DDL" in {
        FieldAlteration(
            StructField("column", StringType),
            "parent.column",
            "type",
            "STRING"
        ).alterTypeDDL("db.table") should equal("ALTER TABLE db.table ALTER COLUMN `parent`.`column` TYPE STRING ;")
    }

    "FieldAlteration.alterCommentDDL" should "generate alter column comment DDL" in {
        FieldAlteration(
            StructField("column", StringType),
            "parent.column",
            "comment",
            "new comment"
        ).alterCommentDDL("db.table") should equal("ALTER TABLE db.table ALTER COLUMN `parent`.`column` COMMENT 'new comment' ;")
    }

    "field.normalizeAndWiden" should "normalize the field name, makes it nullable, and widen" in {
        val field = StructField("$Field-1.other", IntegerType, nullable = false)

        val normalizedField = field.normalizeAndWiden()
        normalizedField.name should equal("_field_1_other")
        normalizedField.dataType should equal(LongType)
        normalizedField.nullable should equal(true)
    }
}
