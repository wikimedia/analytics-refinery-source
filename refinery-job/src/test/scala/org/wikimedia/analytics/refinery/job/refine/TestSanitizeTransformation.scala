package org.wikimedia.analytics.refinery.job.refine

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.joda.time.DateTime
import org.scalatest.{FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.core.HivePartition
import org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame
import scala.collection.immutable.ListMap
import SanitizeTransformation._


class TestSanitizeTransformation extends FlatSpec
    with Matchers with DataFrameSuiteBase {

    val fakeHivePartition = new HivePartition(database = "database", t = "table", location = "/fake/location")

    val keepAllTag = SanitizeTransformation.keepAllTag
    val keepTag = SanitizeTransformation.keepTag
    val hashTag = SanitizeTransformation.hashTag

    // Helper method for tests to see whether MaskNode Trees are equal
    def maskNodeTreeEquals(a: Any, b: Any): Boolean = {
        val res = a match {
            // base cases
            case aa: SanitizationAction =>
                b match {
                    case bb: SanitizationAction => aa == bb
                    case _ => false
                }
            case aa: ValueMaskNode =>
                b match {
                    case bb: ValueMaskNode => aa.action == bb.action
                    case _ => false
                }
            // recursion cases
            case aa: StructMaskNode =>
                b match {
                    case bb: StructMaskNode =>
                        aa.children.size == bb.children.size &&
                          aa.children.zip(bb.children).foldLeft(true) {
                              case (result, pair) => result && maskNodeTreeEquals(pair._1, pair._2)
                          }
                    case _ => false
                }
            case aa: MapMaskNode =>
                b match {
                    case bb: MapMaskNode =>
                        val sortedAA = aa.allowlist.toSeq.sortBy(_._1)
                        val sortedBB = bb.allowlist.toSeq.sortBy(_._1)
                        sortedAA.size == sortedBB.size &&
                          sortedAA.zip(sortedBB).foldLeft(true) {
                              case (result, pair) =>
                                  val r = result && pair._1._1 == pair._2._1
                                  val rr = r && maskNodeTreeEquals(pair._1._2, pair._2._2)
                                  rr
                          }
                    case _ => false
                }
        }
        res
    }

    it should "return an empty DataFrame" in {
        val schema = StructType(Seq(
            StructField("f1", IntegerType, nullable=true)
        ))
        val result: DataFrame = emptyDataFrame(spark, schema)
        result.schema should equal(schema)
        result.count should equal(0)
    }

    it should "get the start and end of year-month-day-hour partition" in {
        val partition = new HivePartition(
            database = "database",
            t = "table",
            location = "/fake/location",
            partitions = ListMap(
                "year" -> Some("2019"),
                "month" -> Some("2"),
                "day" -> Some("28"),
                "hour" -> Some("23")
            )
        )
        val (start, end) = getPartitionStartAndEnd(partition)
        start should equal(new DateTime(2019, 2, 28, 23, 0))
        end should equal(new DateTime(2019, 3, 1, 0, 0))
    }

    it should "get the start and end of year-month partition" in {
        val partition = new HivePartition(
            database = "database",
            t = "table",
            location = "/fake/location",
            partitions = ListMap(
                "year" -> Some("2018"),
                "month" -> Some("12")
            )
        )
        val (start, end) = getPartitionStartAndEnd(partition)
        start should equal(new DateTime(2018, 12, 1, 0, 0))
        end should equal(new DateTime(2019, 1, 1, 0, 0))
    }

    it should "choose salt correctly" in {
        val partition = new HivePartition(
            database = "database",
            t = "table",
            location = "/fake/location",
            partitions = ListMap(
                "year" -> Some("2018"),
                "month" -> Some("12")
            )
        )
        val salts = Seq(
            (new DateTime(2018, 11, 1, 0, 0), new DateTime(2018, 12, 1, 0, 0), "salt1"),
            (new DateTime(2018, 12, 1, 0, 0), new DateTime(2019, 1, 1, 0, 0), "salt2")
        )
        val result = chooseSalt(salts, partition)
        result should equal(Some("salt2"))
    }

    it should "return None if there are no matching salts" in {
        val partition = new HivePartition(
            database = "database",
            t = "table",
            location = "/fake/location",
            partitions = ListMap(
                "year" -> Some("2018"),
                "month" -> Some("12")
            )
        )
        val salts = Seq(
            (new DateTime(2018, 11, 1, 0, 0), new DateTime(2018, 12, 1, 0, 0), "salt1"),
            (new DateTime(2018, 12, 1, 0, 0), new DateTime(2018, 12, 1, 1, 0), "salt2")
        )
        val result = chooseSalt(salts, partition)
        result should equal(None)
    }

    it should "create a value mask for simple fields" in {
        val field = StructField("field", StringType, nullable=true)

        // simple field allowlisted with keep
        val result1 = getValueMask(field, keepTag)
        val expected1 = ValueMaskNode(Identity())
        assert(maskNodeTreeEquals(result1, expected1))

        // string field allowlisted with hash
        val salt = "salt"
        val result2 = getValueMask(field, hashTag, Some(salt))
        val expected2 = ValueMaskNode(Hash(salt))
        assert(maskNodeTreeEquals(result2, expected2))

        // simple field allowlisted with invalid label
        an[Exception] should be thrownBy getValueMask(field, keepAllTag)
    }

    it should "create a value mask for nested fields" in {
        val field = StructField("field", StructType(Seq(
            StructField("subfield", StringType, nullable=true)
        )), nullable=true)

        // nested field allowlisted with keepall
        val result1 = getValueMask(field, keepAllTag)
        val expected1 = ValueMaskNode(Identity())
        assert(maskNodeTreeEquals(result1, expected1))

        // nested struct field partially allowlisted
        val result2 = getValueMask(field, Map("subfield" -> keepTag))
        val expected2 = StructMaskNode(Array(ValueMaskNode(Identity())))
        assert(maskNodeTreeEquals(result2, expected2))

        // nested field allowlisted with invalid label
        an[Exception] should be thrownBy getValueMask(field, keepTag)
    }

    it should "raise an error when creating a value mask with an invalid hash action" in {
        // string field, but salt is not defined
        val field1 = StructField("field", StringType, nullable=true)
        an[Exception] should be thrownBy getValueMask(field1, hashTag, None)

        // salt is defined, but field is not string
        val field2 = StructField("field", IntegerType, nullable=true)
        an[Exception] should be thrownBy getValueMask(field2, hashTag, Some("salt"))
    }

    it should "create a map mask for a map with simple values" in {
        val mapType = MapType(StringType, StringType, false)

        // simple subfield allowlisted with keep
        val result1 = getMapMask(mapType, Map("subfield" -> keepTag))
        val expected1 = MapMaskNode(Map("subfield" -> Identity()))
        assert(maskNodeTreeEquals(result1, expected1))

        // string field allowlisted with hash
        val salt = "salt"
        val result2 = getMapMask(mapType, Map("subfield" -> hashTag), Some(salt))
        val expected2 = MapMaskNode(Map("subfield" -> Hash(salt)))
        assert(maskNodeTreeEquals(result2, expected2))

        // map subfield allowlisted with keep
        an[Exception] should be thrownBy getMapMask(mapType, Map("subfield" -> keepAllTag))
    }

    it should "create a map mask for a map with map values" in {
        val mapOfMapsType = MapType(
            StringType,
            MapType(StringType, StringType, false),
            false
        )

        // map subfield allowlisted with keepall
        val result = getMapMask(mapOfMapsType, Map("subfield" -> keepAllTag))
        val expected = MapMaskNode(Map("subfield" -> Identity()))
        assert(maskNodeTreeEquals(result, expected))

        // map subfield allowlisted with keep
        an[Exception] should be thrownBy getMapMask(mapOfMapsType, Map("subfield" -> keepTag))
    }

    it should "create a map mask for a map with struct values" in {
        // https://phabricator.wikimedia.org/T349121
        // map<string,struct<data_type:string,value:string>>
        val mapOfStructsType = MapType(
            StringType,
            StructType(Seq(
                StructField("data_type", StringType, nullable=true),
                StructField("value", StringType, nullable=true)
            )),
            false
        )

        // simple case: struct subfield allowlisted with keepAll
        val simpleResult = getMapMask(mapOfStructsType, Map("subfield" -> keepAllTag))
        val simpleExpected = MapMaskNode(Map("subfield" -> Identity()))
        assert(maskNodeTreeEquals(simpleResult, simpleExpected))

        // struct subfield allowlisted with keep
        an[Exception] should be thrownBy getMapMask(mapOfStructsType, Map("subfield" -> keepTag))

        // nested case: struct subfield with further fields allowlisted with keep
        val nestedSubField = Map(
            "subfield" -> Map(
                "data_type" -> keepTag,
                "value" -> keepTag
            )
        )
        val expectedNestedResult = MapMaskNode(Map(
            "subfield" -> StructMaskNode(Array(
                ValueMaskNode(Identity()),
                ValueMaskNode(Identity())
            ))
        ))
        val nestedResult = getMapMask(mapOfStructsType, nestedSubField)
        assert(maskNodeTreeEquals(nestedResult, expectedNestedResult))
    }

    it should "create a struct mask with nullified value" in {
        val struct = StructType(Seq(
            StructField("field", StringType, nullable=true)
        ))
        val result = getStructMask(struct, Map())
        val expected = StructMaskNode(Array(ValueMaskNode(Nullify())))
        assert(maskNodeTreeEquals(result, expected))
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
        val allowlist = Map(
            "fa1" -> keepTag,
            "fa2" -> Map(
                "fb2" -> Map(
                    "fc" -> keepTag
                )
            )
        )
        val result = getValueMask(field, allowlist)
        val expected = StructMaskNode(Array(
            ValueMaskNode(Identity()),
            StructMaskNode(Array(
                ValueMaskNode(Nullify()),
                MapMaskNode(Map("fc" -> Identity()))
            )),
            ValueMaskNode(Nullify())
        ))

        assert(maskNodeTreeEquals(result, expected))
    }

    it should "merge a value mask with a value mask correctly" in {
        val nullifyMask = ValueMaskNode(Nullify())
        val identityMask = ValueMaskNode(Identity())
        nullifyMask.merge(nullifyMask).asInstanceOf[ValueMaskNode].action should equal(Nullify())
        nullifyMask.merge(identityMask).asInstanceOf[ValueMaskNode].action should equal(Identity())
        identityMask.merge(nullifyMask).asInstanceOf[ValueMaskNode].action should equal(Identity())
        identityMask.merge(identityMask).asInstanceOf[ValueMaskNode].action should equal(Identity())
    }

    it should "merge a value mask with a struct mask correctly" in {
        val nullifyMask = ValueMaskNode(Nullify())
        val identityMask = ValueMaskNode(Identity())
        val structMask = StructMaskNode(Array(nullifyMask, identityMask))
        nullifyMask.merge(structMask).asInstanceOf[StructMaskNode].children.size should equal(2)
        identityMask.merge(structMask).asInstanceOf[StructMaskNode].children.size should equal(2)
        structMask.merge(nullifyMask).asInstanceOf[StructMaskNode].children.size should equal(2)
        structMask.merge(identityMask).asInstanceOf[ValueMaskNode].action should equal(Identity())
    }

    it should "merge a value mask with a map mask correctly" in {
        val nullifyMask = ValueMaskNode(Nullify())
        val identityMask = ValueMaskNode(Identity())
        val mapMask = MapMaskNode(Map("f1" -> Nullify(), "f2" -> Identity()))
        nullifyMask.merge(mapMask).asInstanceOf[MapMaskNode].allowlist.size should equal(2)
        identityMask.merge(mapMask).asInstanceOf[MapMaskNode].allowlist.size should equal(2)
        mapMask.merge(nullifyMask).asInstanceOf[MapMaskNode].allowlist.size should equal(2)
        mapMask.merge(identityMask).asInstanceOf[ValueMaskNode].action should equal(Identity())
    }

    it should "merge a struct mask with a struct mask correctly" in {
        val nullifyMask = ValueMaskNode(Nullify())
        val identityMask = ValueMaskNode(Identity())
        val structMask1 = StructMaskNode(Array(nullifyMask, identityMask))
        val structMask2 = StructMaskNode(Array(identityMask, nullifyMask))
        val result = structMask1.merge(structMask2).asInstanceOf[StructMaskNode]
        result.children.size should equal(2)
        result.children(0).asInstanceOf[ValueMaskNode].action should equal(Identity())
        result.children(1).asInstanceOf[ValueMaskNode].action should equal(Identity())
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
        result.allowlist should equal(Map(
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
        result1 should equal(expected1)

        // null value
        val result2 = Hash(salt).apply(null)
        assert(result2 == null)
    }

    it should "Correctly apply a MapMaskNode value with a StructMaskNode child" in {
        val expectedNestedResult = MapMaskNode(Map(
            "subfield" -> StructMaskNode(Array(
                ValueMaskNode(Identity()),
                ValueMaskNode(Identity())
            ))
        ))
        val row = Row("a", "b", "c")
        val result = expectedNestedResult.apply(Map("subfield" -> row, "otherfield" -> row))
        assert(result == Map("subfield" -> Row("a", "b")))
    }

    it should "return null when a sanitization action receives null" in {
        // StructMaskNode
        val mask1 = StructMaskNode(Array(
            ValueMaskNode(Nullify()),
            ValueMaskNode(Identity())
        ))
        val result1 = mask1.apply(null)
        assert(result1 == null)

        // MapMaskNode
        val mask2 = MapMaskNode(Map(
            "f1" -> Nullify(),
            "f2" -> Identity()
        ))
        val result2 = mask2.apply(null)
        assert(result2 == null)
    }

    it should "sanitize a row by applying a sanitization mask" in {
        val mask = StructMaskNode(Array(
            ValueMaskNode(Identity()),
            ValueMaskNode(Nullify()),
            StructMaskNode(Array(
                ValueMaskNode(Nullify()),
                ValueMaskNode(Identity())
            )),
            // Note lowercase f (it would be lowercased anyway by makeAllowlistLowerCase).
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
        result should equal(expected)
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
        result.length should equal(3)
        result(0) should equal(Row(1, null, "muk"))
        result(1) should equal(Row(2, null, "jji"))
        result(2) should equal(Row(3, null, "ppa"))
    }

    it should "return the source DataFrame as is when allowlisting a table with keepall" in {
        val partDf = new PartitionedDataFrame(
            spark.createDataFrame(
                sc.parallelize(Seq(Row(1))),
                StructType(Seq(StructField("f1", IntegerType, nullable=true)))
            ),
            fakeHivePartition
        )
        val result = sanitizeTable(
            partDf,
            Map("table" -> keepAllTag)
        ).df.collect
        result.length should equal(1)
        result(0) should equal(Row(1))
    }

    it should "return an empty DataFrame when the table is not in the allowlist" in {
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
        result.length should equal(0)
    }

    it should "raise an error when allowlisting a table with keep tag" in {
        val partDf = new PartitionedDataFrame(
            spark.createDataFrame(
                sc.parallelize(Seq(Row(1))),
                StructType(Seq(StructField("f1", IntegerType, nullable=true)))
            ),
            fakeHivePartition
        )
        an[Exception] should be thrownBy sanitizeTable(
            partDf,
            Map("table" -> keepTag)
        )
    }

    it should "partially sanitize table with a proper allowlist block" in {
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
            Map("table" -> Map("f1" -> keepTag))
        ).df.collect.sortBy(_.getInt(0))
        result.length should equal(2)
        result(0) should equal(Row(1, null))
        result(1) should equal(Row(2, null))
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
                "table" -> Map("f1" -> keepTag),
                "__defaults__" -> Map("f2" -> keepTag)
            )
        ).df.collect.sortBy(_.getInt(0))
        result.length should equal(2)
        result(0) should equal(Row(1, "hi", null))
        result(1) should equal(Row(2, "bye", null))
    }

    it should "automatically allowlist partition fields" in {
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
                partitions = ListMap("f1" -> Some("1"))
            )
        )
        val result = sanitizeTable(
            partDf,
            Map("table" -> Map("f2" -> keepTag))
        ).df.collect.sortBy(_.getInt(0))
        result.length should equal(2)
        result(0) should equal(Row(1, true))
        result(1) should equal(Row(2, false))
    }

    it should "lower case table and field names before checking them against the allowlist" in {
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
            Map("table" -> Map("fieldname1" -> keepTag, "fieldname2" -> keepTag))
        ).df.collect.sortBy(_.getInt(0))
        result.length should equal(2)
        result(0) should equal(Row(1, true))
        result(1) should equal(Row(2, false))
    }
}
