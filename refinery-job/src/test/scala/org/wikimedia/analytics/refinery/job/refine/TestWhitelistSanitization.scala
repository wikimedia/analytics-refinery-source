package org.wikimedia.analytics.refinery.job.refine

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.core.HivePartition
import org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame
import scala.collection.immutable.ListMap
import WhitelistSanitization._


class TestWhitelistSanitization extends FlatSpec
    with Matchers with DataFrameSuiteBase {

    val fakeHivePartition = new HivePartition(database = "database", t = "table", location = "/fake/location")

    it should "return an empty DataFrame" in {
        val schema = StructType(Seq(
            StructField("f1", IntegerType, nullable=true)
        ))
        val result: DataFrame = emptyDataFrame(spark, schema)
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

    it should "create a value mask for simple fields" in {
        val field = StructField("field", StringType, nullable=true)

        // simple field whitelisted with keep
        val result1 = getValueMask(field, "keep")
        val expected1 = ValueMaskNode(Identity())
        assert(result1.equals(expected1))

        // string field whitelisted with hash
        val salt = "salt"
        val result2 = getValueMask(field, "hash", Some(salt))
        val expected2 = ValueMaskNode(Hash(salt))
        assert(result2.equals(expected2))

        // simple field whitelisted with invalid label
        an[Exception] should be thrownBy getValueMask(field, "keepall")
    }

    it should "create a value mask for nested fields" in {
        val field = StructField("field", StructType(Seq(
            StructField("subfield", StringType, nullable=true)
        )), nullable=true)

        // nested field whitelisted with keepall
        val result1 = getValueMask(field, "keepall")
        val expected1 = ValueMaskNode(Identity())
        assert(result1.equals(expected1))

        // nested struct field partially whitelisted
        val result2 = getValueMask(field, Map("subfield" -> "keep"))
        val expected2 = StructMaskNode(Array(ValueMaskNode(Identity())))
        assert(result2.equals(expected2))

        // nested field whitelisted with invalid label
        an[Exception] should be thrownBy getValueMask(field, "keep")
    }

    it should "raise an error when creating a value mask with an invalid hash action" in {
        // string field, but salt is not defined
        val field1 = StructField("field", StringType, nullable=true)
        an[Exception] should be thrownBy getValueMask(field1, "hash", None)

        // salt is defined, but field is not string
        val field2 = StructField("field", IntegerType, nullable=true)
        an[Exception] should be thrownBy getValueMask(field2, "hash", Some("salt"))
    }

    it should "create a map mask for a map with simple values" in {
        val mapType = MapType(StringType, StringType, false)

        // simple subfield whitelisted with keep
        val result1 = getMapMask(mapType, Map("subfield" -> "keep"))
        val expected1 = MapMaskNode(Map("subfield" -> Identity()))
        assert(result1.equals(expected1))

        // string field whitelisted with hash
        val salt = "salt"
        val result2 = getMapMask(mapType, Map("subfield" -> "hash"), Some(salt))
        val expected2 = MapMaskNode(Map("subfield" -> Hash(salt)))
        assert(result2.equals(expected2))

        // map subfield whitelisted with keep
        an[Exception] should be thrownBy getMapMask(mapType, Map("subfield" -> "keepall"))
    }

    it should "create a map mask for a map with map values" in {
        val mapOfMapsType = MapType(
            StringType,
            MapType(StringType, StringType, false),
            false
        )

        // map subfield whitelisted with keepall
        val result = getMapMask(mapOfMapsType, Map("subfield" -> "keepall"))
        val expected = MapMaskNode(Map("subfield" -> Identity()))
        assert(result.equals(expected))

        // map subfield whitelisted with keep
        an[Exception] should be thrownBy getMapMask(mapOfMapsType, Map("subfield" -> "keep"))
    }

    it should "create a struct mask with nullified value" in {
        val struct = StructType(Seq(
            StructField("field", StringType, nullable=true)
        ))
        val result = getStructMask(struct, Map())
        val expected = StructMaskNode(Array(ValueMaskNode(Nullify())))
        assert(result.equals(expected))
    }

    it should "raise and error when creating a struct mask with a non-nullable field" in {
        val struct = StructType(Seq(
            StructField("field", StringType, nullable=false)
        ))
        an[Exception] should be thrownBy getStructMask(struct, Map())
    }

    it should "create a mask tree" in {
        val field = StructField("field", StructType(Seq(
            StructField("fa1", StringType, nullable=true),
            StructField("fa2", StructType(Seq(
                StructField("fb1", StringType, nullable=true),
                StructField("fb2", MapType(StringType, StringType, false), nullable=true)
            )), nullable=true),
            StructField("fa3", StringType, nullable=true)
        )), nullable=true)
        val whitelist = Map(
            "fa1" -> "keep",
            "fa2" -> Map(
                "fb2" -> Map(
                    "fc" -> "keep"
                )
            )
        )
        val result = getValueMask(field, whitelist)
        val expected = StructMaskNode(Array(
            ValueMaskNode(Identity()),
            StructMaskNode(Array(
                ValueMaskNode(Nullify()),
                MapMaskNode(Map("fc" -> Identity()))
            )),
            ValueMaskNode(Nullify())
        ))
    }

    it should "merge a value mask with a value mask correctly" in {
        val nullifyMask = ValueMaskNode(Nullify())
        val identityMask = ValueMaskNode(Identity())
        assert(nullifyMask.merge(nullifyMask).asInstanceOf[ValueMaskNode].action == Nullify())
        assert(nullifyMask.merge(identityMask).asInstanceOf[ValueMaskNode].action == Identity())
        assert(identityMask.merge(nullifyMask).asInstanceOf[ValueMaskNode].action == Identity())
        assert(identityMask.merge(identityMask).asInstanceOf[ValueMaskNode].action == Identity())
    }

    it should "merge a value mask with a struct mask correctly" in {
        val nullifyMask = ValueMaskNode(Nullify())
        val identityMask = ValueMaskNode(Identity())
        val structMask = StructMaskNode(Array(nullifyMask, identityMask))
        assert(nullifyMask.merge(structMask).asInstanceOf[StructMaskNode].children.size == 2)
        assert(identityMask.merge(structMask).asInstanceOf[StructMaskNode].children.size == 2)
        assert(structMask.merge(nullifyMask).asInstanceOf[StructMaskNode].children.size == 2)
        assert(structMask.merge(identityMask).asInstanceOf[ValueMaskNode].action == Identity())
    }

    it should "merge a value mask with a map mask correctly" in {
        val nullifyMask = ValueMaskNode(Nullify())
        val identityMask = ValueMaskNode(Identity())
        val mapMask = MapMaskNode(Map("f1" -> Nullify(), "f2" -> Identity()))
        assert(nullifyMask.merge(mapMask).asInstanceOf[MapMaskNode].whitelist.size == 2)
        assert(identityMask.merge(mapMask).asInstanceOf[MapMaskNode].whitelist.size == 2)
        assert(mapMask.merge(nullifyMask).asInstanceOf[MapMaskNode].whitelist.size == 2)
        assert(mapMask.merge(identityMask).asInstanceOf[ValueMaskNode].action == Identity())
    }

    it should "merge a struct mask with a struct mask correctly" in {
        val nullifyMask = ValueMaskNode(Nullify())
        val identityMask = ValueMaskNode(Identity())
        val structMask1 = StructMaskNode(Array(nullifyMask, identityMask))
        val structMask2 = StructMaskNode(Array(identityMask, nullifyMask))
        val result = structMask1.merge(structMask2).asInstanceOf[StructMaskNode]
        assert(result.children.size == 2)
        assert(result.children(0).asInstanceOf[ValueMaskNode].action == Identity())
        assert(result.children(1).asInstanceOf[ValueMaskNode].action == Identity())
    }

    it should "merge a map mask with a map mask correctly" in {
        val mapMask1 = MapMaskNode(Map(
            "f1" -> Identity(),
            "f2" -> MapMaskNode(Map("sf2" -> Identity())),
            "f3" -> MapMaskNode(Map("sf3" -> Identity()))
        ))
        val mapMask2 = MapMaskNode(Map(
            "f2" -> Identity(),
            "f3" -> MapMaskNode(Map("sf3bis" -> Identity())),
            "f4" -> Identity()
        ))
        val result = mapMask1.merge(mapMask2).asInstanceOf[MapMaskNode]
        assert(result.whitelist == Map(
            "f1" -> Identity(),
            "f2" -> Identity(),
            "f3" -> MapMaskNode(Map("sf3" -> Identity(), "sf3bis" -> Identity())),
            "f4" -> Identity()
        ))
    }

    it should "correctly apply the salt and hash method" in {
        val salt = "some salt"

        // regular value
        val result1 = Hash(salt).apply("some string value")
        val expected1 = "3BF2B893B2A0F57586E7CF73AF0690F061FAA9546F2C385AD93174BB9A81468C"
        assert(result1 == expected1)

        // null value
        val result2 = Hash(salt).apply(null)
        val expected2 = null
        assert(result2 == expected2)
    }

    it should "return null when a sanitization action receives null" in {
        // StructMaskNode
        val mask1 = StructMaskNode(Array(
            ValueMaskNode(Nullify()),
            ValueMaskNode(Identity())
        ))
        assert(mask1.apply(null) == null)

        // MapMaskNode
        val mask2 = MapMaskNode(Map(
            "f1" -> Nullify(),
            "f2" -> Identity()
        ))
        assert(mask2.apply(null) == null)
    }

    it should "sanitize a row by applying a sanitization mask" in {
        val mask = StructMaskNode(Array(
            ValueMaskNode(Identity()),
            ValueMaskNode(Nullify()),
            StructMaskNode(Array(
                ValueMaskNode(Nullify()),
                ValueMaskNode(Identity())
            )),
            // Note lowercase f (it would be lowercased anyway by makeWhitelistLowerCase).
            MapMaskNode(Map(
                "f1" -> Identity(),
                "f2" -> Hash("salt")
            )),
            ValueMaskNode(Nullify())
        ))
        val row = Row(
            1,
            false,
            Row(2.5, "blah"),
            Map("F1" -> "muk", "F2" -> "jji"), // Note uppercase F.
            Map("f3" -> "ppa")
        )
        val result = mask.apply(row).asInstanceOf[Row]
        val expected = Row(
            1,
            null,
            Row(null, "blah"),
            // Uppercase is respected in output.
            Map(
                "F1" -> "muk",
                "F2" -> "69952EAF790576BC786C8B61494BD3C88EFB198B71120FD95F0B8A7FD99D1BFC"
            ),
            Map()
        )
        assert(result == expected)
    }

    it should "sanitize a data frame" in {
        val schema = StructType(Seq(
            StructField("f1", IntegerType, nullable=true),
            StructField("f2", BooleanType, nullable=true),
            StructField("f3", StringType, nullable=true)
        ))
        val partDf = new PartitionedDataFrame(
            spark.createDataFrame(
                sc.parallelize(Seq(
                    Row(1, true, "muk"),
                    Row(2, true, "jji"),
                    Row(3, true, "ppa")
                )),
                schema
            ),
            fakeHivePartition
        )
        val mask = StructMaskNode(Array(
            ValueMaskNode(Identity()),
            ValueMaskNode(Nullify()),
            ValueMaskNode(Identity())
        ))
        val result = sanitizeDataFrame(partDf, mask).df.collect.sortBy(_.getInt(0))
        assert(result.length == 3)
        assert(result(0) == Row(1, null, "muk"))
        assert(result(1) == Row(2, null, "jji"))
        assert(result(2) == Row(3, null, "ppa"))
    }

    it should "return the source DataFrame as is when whitelisting a table with keepall" in {
        val partDf = new PartitionedDataFrame(
            spark.createDataFrame(
                sc.parallelize(Seq(Row(1))),
                StructType(Seq(StructField("f1", IntegerType, nullable=true)))
            ),
            fakeHivePartition
        )
        val result = sanitizeTable(
            partDf,
            Map("table" -> "keepall")
        ).df.collect
        assert(result.length == 1)
        assert(result(0) == Row(1))
    }

    it should "return an empty DataFrame when the table is not in the whitelist" in {
        val partDf = new PartitionedDataFrame(
            spark.createDataFrame(
                sc.parallelize(Seq(Row(1))),
                StructType(Seq(StructField("f1", IntegerType, nullable=true)))
            ),
            fakeHivePartition
        )
        val result = sanitizeTable(
            partDf,
            Map()
        ).df.collect
        assert(result.length == 0)
    }

    it should "raise an error when whitelisting a table with keep tag" in {
        val partDf = new PartitionedDataFrame(
            spark.createDataFrame(
                sc.parallelize(Seq(Row(1))),
                StructType(Seq(StructField("f1", IntegerType, nullable=true)))
            ),
            fakeHivePartition
        )
        an[Exception] should be thrownBy sanitizeTable(
            partDf,
            Map("table" -> "keep")
        )
    }

    it should "partially sanitize table with a proper whitelist block" in {
        val partDf = new PartitionedDataFrame(
            spark.createDataFrame(
                sc.parallelize(Seq(Row(1, true), Row(2, false))),
                StructType(Seq(
                    StructField("f1", IntegerType, nullable=true),
                    StructField("f2", BooleanType, nullable=true)
                ))
            ),
            fakeHivePartition
        )
        val result = sanitizeTable(
            partDf,
            Map("table" -> Map("f1" -> "keep"))
        ).df.collect.sortBy(_.getInt(0))
        assert(result.length == 2)
        assert(result(0) == Row(1, null))
        assert(result(1) == Row(2, null))
    }

    it should "sanitize table with proper defaults" in {
        val partDf = new PartitionedDataFrame(
            spark.createDataFrame(
                sc.parallelize(Seq(
                    Row(1, "hi", true),
                    Row(2, "bye", false)
                )),
                StructType(Seq(
                    StructField("f1", IntegerType, nullable=true),
                    StructField("f2", StringType, nullable=true),
                    StructField("f3", BooleanType, nullable=true)
                ))
            ),
            fakeHivePartition
        )
        val result = sanitizeTable(
            partDf,
            Map(
                "table" -> Map("f1" -> "keep"),
                "__defaults__" -> Map("f2" -> "keep")
            )
        ).df.collect.sortBy(_.getInt(0))
        assert(result.length == 2)
        assert(result(0) == Row(1, "hi", null))
        assert(result(1) == Row(2, "bye", null))
    }

    it should "automatically whitelist partition fields" in {
        val partDf = new PartitionedDataFrame(
            spark.createDataFrame(
                sc.parallelize(Seq(Row(1, true), Row(2, false))),
                StructType(Seq(
                    StructField("f1", IntegerType, nullable=true),
                    StructField("f2", BooleanType, nullable=true)
                ))
            ),
            new HivePartition(
                database = "database",
                t = "table",
                location = "/fake/location",
                partitions = ListMap("f1" -> "1")
            )
        )
        val result = sanitizeTable(
            partDf,
            Map("table" -> Map("f2" -> "keep"))
        ).df.collect.sortBy(_.getInt(0))
        assert(result.length == 2)
        assert(result(0) == Row(1, true))
        assert(result(1) == Row(2, false))
    }

    it should "lower case table and field names before checking them against the whitelist" in {
        val partDf = new PartitionedDataFrame(
                spark.createDataFrame(
                    sc.parallelize(Seq(Row(1, true), Row(2, false))),
                    StructType(Seq(
                        StructField("fieldName1", IntegerType, nullable=true),
                        StructField("FIELDNAME2", BooleanType, nullable=true)
                    ))
                ),
                fakeHivePartition
            )
        val result = sanitizeTable(
            partDf,
            Map("table" -> Map("fieldname1" -> "keep", "fieldname2" -> "keep"))
        ).df.collect.sortBy(_.getInt(0))
        assert(result.length == 2)
        assert(result(0) == Row(1, true))
        assert(result(1) == Row(2, false))
    }
}
