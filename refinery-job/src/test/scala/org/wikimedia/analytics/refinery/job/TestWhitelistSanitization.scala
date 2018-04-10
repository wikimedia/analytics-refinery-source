package org.wikimedia.analytics.refinery.job

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.job.WhitelistSanitization._
import org.wikimedia.analytics.refinery.job.WhitelistSanitization.SanitizationAction._


class TestWhitelistSanitization extends FlatSpec
    with Matchers with SharedSparkContext {

    it should "return an empty DataFrame" in {
        val sqlContext = new SQLContext(sc)
        val schema = StructType(
            StructField("f1", IntegerType, nullable=true) :: Nil
        )
        val result: DataFrame = emptyDataFrame(
            sqlContext,
            schema
        )
        assert(result.schema == schema)
        assert(result.count == 0)
    }

    it should "make the whitelist lower case" in {
        val whitelist = Map(
            "lowercase" -> "keepall",
            "camelCase" -> "keepAll",
            "snake_case" -> "keep_all",
            "nested" -> Map(
                "UPPERCASE" -> "KEEPALL",
                "PascalCase" -> "KeepAll",
                "kebab-case" -> "keep-all"
            )
        )
        val result = makeWhitelistLowerCase(whitelist)
        val expected = Map(
            "lowercase" -> "keepall",
            "camelcase" -> "keepall",
            "snake_case" -> "keepall",
            "nested" -> Map(
                "uppercase" -> "keepall",
                "pascalcase" -> "keepall",
                "kebab-case" -> "keepall"
            )
        )
        assert(result == expected)
    }

    it should "create a nullify mask when a field is not in the whitelist" in {
        // Non-nested field.
        val result1 = getFieldMask(
            StructField("field", StringType, nullable=true),
            Map() // Empty whitelist, the field is not present.
        ).asInstanceOf[MaskLeafNode]
        assert(result1.action == Nullify)
        // Nested field.
        val result2 = getFieldMask(
            StructField("field", StructType(
                StructField("subfield", StringType, nullable=true) :: Nil
            ), nullable=true),
            Map() // Empty whitelist, the field is not present.
        ).asInstanceOf[MaskLeafNode]
        assert(result2.action == Nullify)
    }

    it should "create an identity mask when a field is in the whitelist" in {
        // Non-nested field.
        val result1 = getFieldMask(
            StructField("field", StringType, nullable=true),
            Map("field" -> "keep")
        ).asInstanceOf[MaskLeafNode]
        assert(result1.action == Identity)
        // Nested field.
        val result2 = getFieldMask(
            StructField("field", StructType(
                StructField("subfield", StringType, nullable=true) :: Nil
            ), nullable=true),
            Map("field" -> "keepall")
        ).asInstanceOf[MaskLeafNode]
        assert(result2.action == Identity)
    }

    it should "raise an error when a field is not in the whitelist and is non-nullable" in {
        // Non-nested field.
        an[Exception] should be thrownBy getFieldMask(
            StructField("field", StringType, nullable=false), // False means non-nullable.
            Map() // Empty whitelist, the field is not present.
        )
        // Nested field.
        an[Exception] should be thrownBy getFieldMask(
            StructField("field", StructType(
                StructField("subfield", StringType, nullable=true) :: Nil
            ), nullable=false), // False means non-nullable.
            Map() // Empty whitelist, the field is not present.
        )
    }

    it should "raise an error if the whitelist value is invalid" in {
        // Non-nested field with keepall tag.
        an[Exception] should be thrownBy getFieldMask(
            StructField("field", StringType, nullable=true),
            Map("field" -> "keepall")
        )
        // Non-nested field with childWhitelist.
        an[Exception] should be thrownBy getFieldMask(
            StructField("field", StringType, nullable=true),
            Map("field" -> Map("subfield" -> "keep"))
        )
        // Nested field with keep tag.
        an[Exception] should be thrownBy getFieldMask(
            StructField("field", StructType(
                StructField("subfield", StringType, nullable=true) :: Nil
            ), nullable=true),
            Map("field" -> "keep")
        )
    }

    it should "create a mask tree when a nested field is in the whitelist" in {
        // Define field structure.
        val field = StructField("field", StructType(
            StructField("fa1", StringType, nullable=true) ::
            StructField("fa2", StructType(
                StructField("fb1", StringType, nullable=true) ::
                StructField("fb2", StructType(
                    StructField("fc1", StringType, nullable=true) ::
                    StructField("fc2", StringType, nullable=true) :: Nil
                ), nullable=true) :: Nil
            ), nullable=true) ::
            StructField("fa3", StringType, nullable=true) :: Nil
        ), nullable=true)
        // Define Whitelist, note it must be lower case at this point.
        val whitelist = Map(
            "field" -> Map(
                "fa1" -> "keep",
                "fa2" -> Map(
                    "fb2" -> "keepall"
                )
            )
        )
        // Assert results. Scala does not apply equality operator
        // recursively to the MaskNode tree, so...
        val result = getFieldMask(field, whitelist).asInstanceOf[MaskInnerNode]
        assert(result.children.length == 3)
        assert(result.children(0).asInstanceOf[MaskLeafNode].action == Identity)
        val fa2 = result.children(1).asInstanceOf[MaskInnerNode]
        assert(fa2.children.length == 2)
        assert(fa2.children(0).asInstanceOf[MaskLeafNode].action == Nullify)
        assert(fa2.children(1).asInstanceOf[MaskLeafNode].action == Identity)
        assert(result.children(2).asInstanceOf[MaskLeafNode].action == Nullify)
    }

    it should "sanitize a row by applying a sanitization mask" in {
        val mask = MaskInnerNode(Array(
            MaskLeafNode(Identity),
            MaskLeafNode(Nullify),
            MaskInnerNode(Array(
                MaskLeafNode(Nullify),
                MaskLeafNode(Identity)
            ))
        ))
        val row = Row(1, false, Row(2.5, "blah"))
        val result = mask.apply(row).asInstanceOf[Row]
        val expected = Row(1, null, Row(null, "blah"))
        assert(result == expected)
    }

    it should "sanitize a data frame" in {
        val sqlContext = new SQLContext(sc)
        val schema = StructType(
            StructField("f1", IntegerType, nullable=true) ::
            StructField("f2", BooleanType, nullable=true) ::
            StructField("f3", StringType, nullable=true) :: Nil
        )
        val dataFrame = sqlContext.createDataFrame(
            sc.parallelize(Seq(
                Row(1, true, "muk"),
                Row(2, true, "jji"),
                Row(3, true, "ppa")
            )),
            schema
        )
        val mask = MaskInnerNode(Array(
            MaskLeafNode(Identity),
            MaskLeafNode(Nullify),
            MaskLeafNode(Identity)
        ))
        val result = sanitizeDataFrame(dataFrame, mask)
            .collect
            .sortBy(_.getInt(0))
        assert(result.length == 3)
        assert(result(0) == Row(1, null, "muk"))
        assert(result(1) == Row(2, null, "jji"))
        assert(result(2) == Row(3, null, "ppa"))
    }

    it should "return the source DataFrame as is when whitelisting a table with keepall" in {
        val sqlContext = new SQLContext(sc)
        val dataFrame = sqlContext.createDataFrame(
            sc.parallelize(Seq(Row(1))),
            StructType(StructField("f1", IntegerType, nullable=true) :: Nil)
        )
        val result = sanitizeTable(
            dataFrame,
            "table",
            Seq.empty,
            Map("table" -> "keepall")
        ).collect
        assert(result.length == 1)
        assert(result(0) == Row(1))
    }

    it should "return an empty DataFrame when the table is not in the whitelist" in {
        val sqlContext = new SQLContext(sc)
        val dataFrame = sqlContext.createDataFrame(
            sc.parallelize(Seq(Row(1))),
            StructType(StructField("f1", IntegerType, nullable=true) :: Nil)
        )
        val result = sanitizeTable(
            dataFrame,
            "table",
            Seq.empty,
            Map()
        ).collect
        assert(result.length == 0)
    }

    it should "raise an error when whitelisting a table with keep tag" in {
        val sqlContext = new SQLContext(sc)
        val dataFrame = sqlContext.createDataFrame(
            sc.parallelize(Seq(Row(1))),
            StructType(StructField("f1", IntegerType, nullable=true) :: Nil)
        )
        an[Exception] should be thrownBy sanitizeTable(
            dataFrame,
            "table",
            Seq.empty,
            Map("table" -> "keep")
        )
    }

    it should "partially sanitize table with a proper whitelist block" in {
        val sqlContext = new SQLContext(sc)
        val dataFrame = sqlContext.createDataFrame(
            sc.parallelize(Seq(Row(1, true), Row(2, false))),
            StructType(
                StructField("f1", IntegerType, nullable=true) ::
                StructField("f2", BooleanType, nullable=true) :: Nil
            )
        )
        val result = sanitizeTable(
            dataFrame,
            "table",
            Seq.empty,
            Map("table" -> Map("f1" -> "keep"))
        ).collect.sortBy(_.getInt(0))
        assert(result.length == 2)
        assert(result(0) == Row(1, null))
        assert(result(1) == Row(2, null))
    }

    it should "automatically whitelist partition fields" in {
        val sqlContext = new SQLContext(sc)
        val dataFrame = sqlContext.createDataFrame(
            sc.parallelize(Seq(Row(1, true), Row(2, false))),
            StructType(
                StructField("f1", IntegerType, nullable=true) ::
                StructField("f2", BooleanType, nullable=true) :: Nil
            )
        )
        val result = sanitizeTable(
            dataFrame,
            "table",
            Seq("f1"),
            Map("table" -> Map("f2" -> "keep"))
        ).collect.sortBy(_.getInt(0))
        assert(result.length == 2)
        assert(result(0) == Row(1, true))
        assert(result(1) == Row(2, false))
    }

    it should "lower case table and field names before checking them against the whitelist" in {
        val sqlContext = new SQLContext(sc)
        val dataFrame = sqlContext.createDataFrame(
            sc.parallelize(Seq(Row(1, true), Row(2, false))),
            StructType(
                StructField("fieldName1", IntegerType, nullable=true) ::
                StructField("FIELDNAME2", BooleanType, nullable=true) :: Nil
            )
        )
        val result = sanitizeTable(
            dataFrame,
            "table",
            Seq(),
            Map("table" -> Map("fieldname1" -> "keep", "fieldname2" -> "keep"))
        ).collect.sortBy(_.getInt(0))
        assert(result.length == 2)
        assert(result(0) == Row(1, true))
        assert(result(1) == Row(2, false))
    }
}
