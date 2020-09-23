import scala.util.{Failure, Try}

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.spark.sql.JsonSchemaConverter

class TestJsonSchemaConverter extends FlatSpec with Matchers {

  /**********************************************************
   * Valid cases
   */

  val mapper = new ObjectMapper()

  it should "convert a properly formatted json schema (boolean)" in {
    val testSchema = """{"properties":{"test_boolean":{"type":"boolean"}}}"""
    val expectedFields = Array(StructField("test_boolean", BooleanType, nullable = true))

    val node = mapper.readTree(testSchema)
    val sparkSchema = JsonSchemaConverter.toSparkSchema(node)

    sparkSchema should not be null
    sparkSchema.fields should equal(expectedFields)
  }

  it should "convert a properly formatted json schema (integer)" in {
    val testSchema = """{"properties":{"test_integer":{"type":"integer"}}}"""
    val expectedFields = Array(StructField("test_integer", LongType, nullable = true))

    val node = mapper.readTree(testSchema)
    val sparkSchema = JsonSchemaConverter.toSparkSchema(node)

    sparkSchema should not be null
    sparkSchema.fields should equal(expectedFields)
  }

  it should "convert a properly formatted json schema (number)" in {
    val testSchema = """{"properties":{"test_number":{"type":"number"}}}"""
    val expectedFields = Array(StructField("test_number", DoubleType, nullable = true))

    val node = mapper.readTree(testSchema)
    val sparkSchema = JsonSchemaConverter.toSparkSchema(node)

    sparkSchema should not be null
    sparkSchema.fields should equal(expectedFields)
  }

  it should "convert a properly formatted json schema (string)" in {
    val testSchema = """{"properties":{"test_string":{"type":"string"}}}"""
    val expectedFields = Array(StructField("test_string", StringType, nullable = true))

    val node = mapper.readTree(testSchema)
    val sparkSchema = JsonSchemaConverter.toSparkSchema(node)

    sparkSchema should not be null
    sparkSchema.fields should equal(expectedFields)
  }

  it should "convert a properly formatted json schema (array)" in {
    val testSchema = """{"properties":{"test_array":{"type":"array","items":{"type":"string"}}}}"""
    val expectedFields = Array(
      StructField("test_array", ArrayType(StringType, containsNull = false), nullable = true))

    val node = mapper.readTree(testSchema)
    val sparkSchema = JsonSchemaConverter.toSparkSchema(node)

    sparkSchema should not be null
    sparkSchema.fields should equal(expectedFields)
  }

  it should "convert a properly formatted json schema (object-struct)" in {
    val testSchema =
      """{"properties":{"test_object":{"type":"object","properties":{
        |"subfield1":{"type":"string"},"subfield2":{"type":"integer"}}}}}""".stripMargin
    val subObjectSchema = StructType(Seq(
      StructField("subfield1", StringType, nullable = true),
      StructField("subfield2", LongType, nullable = true)))
    val expectedFields = Array(
      StructField("test_object", subObjectSchema, nullable = true))

    val node = mapper.readTree(testSchema)
    val sparkSchema = JsonSchemaConverter.toSparkSchema(node)

    sparkSchema should not be null
    sparkSchema.fields should equal(expectedFields)
  }

  it should "convert a properly formatted json schema (object-map)" in {
    val testSchema =
      """{"properties":{"test_map":{"type":"object",
        |"additionalProperties":{"type":"number"}}}}""".stripMargin
    val expectedFields = Array(
      StructField("test_map", MapType(StringType, DoubleType, valueContainsNull = false), nullable = true))

    val node = mapper.readTree(testSchema)
    val sparkSchema = JsonSchemaConverter.toSparkSchema(node)

    sparkSchema should not be null
    sparkSchema.fields should equal(expectedFields)
  }

  it should "convert a properly formatted json schema (object-map with defined properties ignored)" in {
    val testSchema =
      """
        |{
        |"properties":{
        |  "test_map":{
        |     "type":"object",
        |     "additionalProperties":{"description":"something else", "type":"number"},
        |     "properties":{
        |       "map_key": {"description": "main thing", "type":"number"}
        |     }
        |  }
        |}}""".stripMargin
    val expectedFields = Array(
      StructField("test_map", MapType(StringType, DoubleType, valueContainsNull = false), nullable = true))

    val node = mapper.readTree(testSchema)
    val sparkSchema = JsonSchemaConverter.toSparkSchema(node)

    sparkSchema should not be null
    sparkSchema.fields should equal(expectedFields)
  }

  it should "fail to convert a properly formatted json schema with an object-map with incompatible properties" in {
    val testSchema =
      """
        |{
        |"properties":{
        |  "test_map":{
        |     "type":"object",
        |     "additionalProperties":{
        |       "type":"object",
        |       "properties": {
        |         "something": {"type": "number"}
        |       }
        |     },
        |     "properties":{
        |       "something_incompatible": {
        |         "type":"object",
        |         "properties": {
        |            "something": {
        |               "description": "something as a string",
        |               "type": "string"
        |             }
        |          }
        |       },
        |       "something_compatible": {
        |         "type": "object",
        |         "description": "something compatible",
        |         "properties": {
        |           "something": {"type":"number"}
        |         }
        |       },
        |       "another_thing_incompatible": {
        |         "type": "object",
        |         "description": "another thing incompatible",
        |         "properties": {
        |           "not_this_thing":{"type": "number"}
        |         }
        |       }
        |     }
        |  }
        |}}""".stripMargin

    val node = mapper.readTree(testSchema)
    Try[StructType](JsonSchemaConverter.toSparkSchema(node)) match {
      case Failure(exception: IllegalArgumentException) => exception.getMessage should equal("Properties " +
        "something_incompatible,another_thing_incompatible are not compatible with the expected " +
        "additionalProperties type StructType(StructField(something,DoubleType,true))")
      case _ => fail("Expected a failure")
    }
  }

  it should "convert a properly formatted json schema with comment" in {
    val testSchema = """{"properties":{"test_boolean":{"type":"boolean","description":"text comment"}}}"""
    val expectedFields = Array(StructField("test_boolean", BooleanType, nullable = true).withComment("text comment"))

    val node = mapper.readTree(testSchema)
    val sparkSchema = JsonSchemaConverter.toSparkSchema(node)

    sparkSchema should not be null
    sparkSchema.fields should equal(expectedFields)
  }

  it should "convert a properly formatted json schema with required property" in {
    val testSchema =
      """{"properties":{"test_boolean":{"type":"boolean"}},
        |"required":["test_boolean"]}""".stripMargin
    val expectedFields = Array(StructField("test_boolean", BooleanType, nullable = false))

    val node = mapper.readTree(testSchema)
    val sparkSchema = JsonSchemaConverter.toSparkSchema(node)

    sparkSchema should not be null
    sparkSchema.fields should equal(expectedFields)
  }

  it should "convert a properly formatted json schema with Draft 3 required property" in {
    val testSchema =
      """{"properties":{"test_boolean":{"type":"boolean", "required": true}}}"""
    val expectedFields = Array(StructField("test_boolean", BooleanType, nullable = false))

    val node = mapper.readTree(testSchema)
    val sparkSchema = JsonSchemaConverter.toSparkSchema(node)

    sparkSchema should not be null
    sparkSchema.fields should equal(expectedFields)
  }

  it should "convert a properly formatted json schema with Draft 3 non-required property" in {
    val testSchema =
      """{"properties":{"test_boolean":{"type":"boolean", "required": false}}}"""
    val expectedFields = Array(StructField("test_boolean", BooleanType, nullable = true))

    val node = mapper.readTree(testSchema)
    val sparkSchema = JsonSchemaConverter.toSparkSchema(node)

    sparkSchema should not be null
    sparkSchema.fields should equal(expectedFields)
  }


  it should "convert a properly formatted complex json schema" in {
    val testSchema =
      """
        |{
        |  "properties": {
        |    "test_boolean": {
        |      "description": "test boolean comment",
        |      "type": "boolean"
        |    },
        |    "test_object": {
        |      "type": "object",
        |      "properties": {
        |        "subfield1": {
        |          "description": "test test_object-subobject string comment",
        |          "type": "string"
        |        },
        |        "subfield2": {
        |          "description": "test test_object-subobject integer comment",
        |          "type":"integer"
        |        }
        |      },
        |      "required": [ "subfield2" ]
        |    },
        |    "test_array": {
        |      "description": "test array comment",
        |      "type": "array",
        |      "items": {
        |        "type": "object",
        |        "properties": {
        |          "array_subfield1": {
        |            "description": "test test_array-subobject boolean comment",
        |            "type": "boolean"
        |          },
        |          "array_subfield2": {
        |            "description": "test test_array-subobject number comment",
        |            "type":"number"
        |          }
        |        },
        |        "required": [ "array_subfield1" ]
        |      }
        |    },
        |    "test_array_map": {
        |      "type": "array",
        |      "items": {
        |        "type": "object",
        |        "additionalProperties": {
        |            "type": "string"
        |        }
        |      }
        |    }
        |  },
        |  "required": [ "test_boolean", "test_array" ]
        |}""".stripMargin

    val subObjectType = StructType(Seq(
      StructField("subfield1", StringType, nullable = true)
        .withComment("test test_object-subobject string comment"),
      StructField("subfield2", LongType, nullable = false)
        .withComment("test test_object-subobject integer comment")
    ))
    val subArrayType = ArrayType(StructType(Seq(
      StructField("array_subfield1", BooleanType, nullable = false)
        .withComment("test test_array-subobject boolean comment"),
      StructField("array_subfield2", DoubleType, nullable = true)
        .withComment("test test_array-subobject number comment")
    )), containsNull = false)
    val subArrayMapType = ArrayType(
      MapType(StringType, StringType, valueContainsNull = false),
      containsNull = false)

    val expectedFields = Array(
      StructField("test_boolean", BooleanType, nullable = false).withComment("test boolean comment"),
      StructField("test_object", subObjectType, nullable = true),
      StructField("test_array", subArrayType, nullable = false).withComment("test array comment"),
      StructField("test_array_map", subArrayMapType, nullable = true)
    )

    val node = mapper.readTree(testSchema)
    val sparkSchema = JsonSchemaConverter.toSparkSchema(node)

    sparkSchema should not be null
    sparkSchema.fields should equal(expectedFields)
  }

  /**********************************************************
   * Error cases
   */

  it should "throw an IllegalArgumentException if properties is not defined" in {
    val testSchema = """"required":["test_boolean"]}"""
    val node = mapper.readTree(testSchema)
    an[IllegalArgumentException] should be thrownBy JsonSchemaConverter.toSparkSchema(node)
  }

  it should "throw an IllegalArgumentException if type is not defined" in {
    val testSchema = """{"properties":{"test_boolean":{"description":"empty"}}}"""
    val node = mapper.readTree(testSchema)
    an[IllegalArgumentException] should be thrownBy JsonSchemaConverter.toSparkSchema(node)
  }

  it should "throw an IllegalArgumentException if type is not valid" in {
    val testSchema = """{"properties":{"test_boolean":{"type":"wrong"}}}"""
    val node = mapper.readTree(testSchema)
    an[IllegalArgumentException] should be thrownBy JsonSchemaConverter.toSparkSchema(node)
  }

  it should "throw an IllegalArgumentException if array doesn't defines item" in {
    val testSchema = """{"properties":{"test_array":{"type":"array"}}}"""
    val node = mapper.readTree(testSchema)
    an[IllegalArgumentException] should be thrownBy JsonSchemaConverter.toSparkSchema(node)
  }

  it should "throw an IllegalArgumentException if array doesn't defines item type" in {
    val testSchema = """{"properties":{"test_array":{"type":"array", "items":{"description":"empty"}}}}"""
    val node = mapper.readTree(testSchema)
    an[IllegalArgumentException] should be thrownBy JsonSchemaConverter.toSparkSchema(node)
  }

  it should "throw an IllegalArgumentException if object doesn't properties nor additionalProperties" in {
    val testSchema = """{"properties":{"test_object":{"type":"object", "items":{"description":"empty"}}}}"""
    val node = mapper.readTree(testSchema)
    an[IllegalArgumentException] should be thrownBy JsonSchemaConverter.toSparkSchema(node)
  }

  it should "throw an IllegalArgumentException if object properties doesn't define type" in {
    val testSchema = """{"properties":{"test_object":{"type":"object", "properties":{"p1":{"description":"empty"}}}}}"""
    val node = mapper.readTree(testSchema)
    an[IllegalArgumentException] should be thrownBy JsonSchemaConverter.toSparkSchema(node)
  }

  it should "throw an IllegalArgumentException if object additionalProperties doesn't define type" in {
    val testSchema = """{"properties":{"test_object":{"type":"object", "additionalProperties":{"description":"empty"}}}}"""
    val node = mapper.readTree(testSchema)
    an[IllegalArgumentException] should be thrownBy JsonSchemaConverter.toSparkSchema(node)
  }

  it should "throw an IllegalArgumentException if root node is not an object" in {
    val testSchema = """1"""
    val node = mapper.readTree(testSchema)
    an[IllegalArgumentException] should be thrownBy JsonSchemaConverter.toSparkSchema(node)
  }

  it should "throw an IllegalArgumentException if a type node is not an object" in {
    val testSchema = """{"properties":{"test_boolean": false}}"""
    val node = mapper.readTree(testSchema)
    an[IllegalArgumentException] should be thrownBy JsonSchemaConverter.toSparkSchema(node)
  }

}