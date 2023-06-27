package org.wikimedia.analytics.refinery.spark.sql

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.types._
import org.wikimedia.analytics.refinery.tools.LogHelper

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Defines functions to convert a subset of JSONSchema to a spark StructType.
 *
 * Notes:
 * - Only a subset of JSONSchema is supported.
 * - object with additionalProperties as a schema are converted to a Map.
 */
object JsonSchemaConverter extends LogHelper {

    // Useful JSONSchema field names
    private val typeField                    = "type"
    private val itemsField                   = "items"
    private val propertiesField              = "properties"
    private val additionalPropertiesField    = "additionalProperties"
    private val requiredField                = "required"
    private val descriptionField             = "description"

    /*
     * Build an ArrayType from an array JSONSchema node.
     * The JsonNode is an object (checked in buildDataType)
     * and is expected to contain a valid items field with type.
     *
     * This function recursively calls the buildStructType function to get items type.
     */
    private def buildArrayType(jsonSchema: JsonNode, fieldName: String): ArrayType = {
        val itemsSchema = jsonSchema.get(itemsField)
        // Arrays must specify the type of their elements.
        if (itemsSchema == null || itemsSchema.isNull)
            throw new IllegalArgumentException(s"`$fieldName` array schema did not specify the items field")
        // Arrays must only use a single type, not tuple validation.
        if (!itemsSchema.isObject || !itemsSchema.has(typeField)) {
            throw new IllegalArgumentException(s"`$fieldName` array schema must specify the items type field")
        }
        ArrayType(buildDataType(itemsSchema, s"$fieldName-$propertiesField"), containsNull = false)
    }

    /*
     * Build a MapType from an object JSONSchema node without properties field.
     * The JsonNode is an object (checked in buildDataType) and is expected
     * to contain a valid additionalProperties field with type.
     *
     * This function recursively calls the buildStructType function to get items type.
     */
    private def buildMapType(jsonSchema: JsonNode, fieldName: String): MapType = {
        val additionalPropertiesSchema = jsonSchema.get(additionalPropertiesField)
        if (additionalPropertiesSchema == null || additionalPropertiesSchema.isNull)
            throw new IllegalArgumentException(
                s"`$fieldName` object schema must specify either properties or additionalProperties field")
        if (!additionalPropertiesSchema.isObject || !additionalPropertiesSchema.has(typeField))
            throw new IllegalArgumentException(
                s"`$fieldName` object schema additionalProperties field must specify its type")
        MapType(
            StringType,
            buildDataType(additionalPropertiesSchema, s"$fieldName-$additionalPropertiesField"),
            valueContainsNull = false)
    }

    /*
     * Build a DataType from a type-describing JsonNode and a field-name for better logging.
     * The JsonNode is expected to contain a valid JSONSchema type TextNode.
     *
     * Note: A special case of converting object with an additionalProperties schema to a Map is
     * included. In JSONSchema, additionalProperties can either be a boolean
     * or an object.  If it is an object, it expected to specify the schema
     * of the unknown properties. This is what we need for a MapType.
     * We want to still allow object schemas to indicate that they have specific
     * property keys in a MapType though, so an object with additionalProperties
     * with a schema can still include a defined properties.  In this case, we will
     * use a MapType here and the defined properties will be ignored in the Spark
     * schema.  It is up to the schema author to ensure that the types of the defined
     * properties match the additionalProperties schema; that is, all defined properties
     * must have the same type as the additionalProperties, as this is what will
     * be used for the value in the MapType.
     *
     * This function recursively calls itself to get array-items types, and the
     * buildStructType function to get object inner types.
     */
    private def buildDataType(jsonSchema: JsonNode, fieldName: String): DataType = {
        if (jsonSchema == null ||  jsonSchema.isNull)
            throw new IllegalArgumentException(s"$fieldName` node shouldn't be null when building DataType")
        if (!jsonSchema.isObject)
            throw new IllegalArgumentException(s"$fieldName` node should be an object when building DataType")

        val schemaTypeNode = jsonSchema.get(typeField)
        if (schemaTypeNode == null || !schemaTypeNode.isTextual)
            throw new IllegalArgumentException(
                s"`$fieldName` schema must contain 'type' field. schema is:\n${jsonSchema}"
            )

        val typeText = schemaTypeNode.textValue()
        typeText match {
            case "null" => NullType
            case "boolean" => BooleanType
            case "integer" => LongType
            case "number" => DoubleType
            case "string" => StringType
            case "array" => buildArrayType(jsonSchema, fieldName)
            case "object" =>
                // An object type must have a schema defined either in properties
                // or additionalProperties.
                val properties = jsonSchema.get(propertiesField)
                val additionalProperties = jsonSchema.get(additionalPropertiesField)

                if (additionalProperties != null && additionalProperties.isObject) {
                    // Special map-case: additionalProperties has a schema, assume this is a MapType.
                    val mapType = buildMapType(jsonSchema, fieldName)
                    if (properties != null) {
                        verifyMapTypeCompatibility(mapType, jsonSchema)
                    }
                    mapType
                } else if (properties != null && properties.isObject) {
                    // Regular sub-object case: properties subfield is defined with no
                    // additionalProperties schema, this should be a StructType.
                    buildStructType(jsonSchema, fieldName)
                } else {
                    throw new IllegalArgumentException(
                        s"`$fieldName` has type $typeText but does not have a schema defined in" +
                        s"either properties (StructType) or additionalProperties (MapType)"
                    )
                }
            case _ => throw new IllegalArgumentException(s"`$fieldName` has invalid type value: $typeText")
        }
    }

    private def verifyMapTypeCompatibility(mapProperty: MapType, jsonSchema: JsonNode): Unit = {
        val propertiesStruct = buildStructType(jsonSchema)
        val incompatibleProps = propertiesStruct.filterNot(_.dataType.equals(mapProperty.valueType)).toSet
        if (incompatibleProps.nonEmpty) {
            throw new IllegalArgumentException(f"Properties ${incompatibleProps.map(_.name).mkString(",")} " +
              f"are not compatible with the expected additionalProperties type ${mapProperty.valueType}")
        }
    }

    /*
     * Build a StructField from a JsonNode, its field-name, and its mandatoriness.
     * The JsonNode is expected to contain a valid JSONSchema type as subfield.
     *
     * This function recursively calls  the buildDataType function to get field type.
     */
    private def buildStructField(jsonSchema: JsonNode, fieldName: String, required: Boolean): StructField = {
        val datatype = buildDataType(jsonSchema, fieldName)
        log.debug(s"Converting JSONSchema field `$fieldName` to Spark dataType $datatype")

        // If this field is in the parent's required list
        // or it is marked as a draft 3 required field,
        // then it should not be nullable.
        val nullable = !(required || isRequiredDraft3(jsonSchema))
        val field = StructField(fieldName, datatype, nullable = nullable)

        val description = jsonSchema.get(descriptionField)
        // Return field with comment if defined, without otherwise
        if (description != null && description.isTextual) field.withComment(description.asText)
        else field
    }

    /*
     * Build a StructType from a JsonNode and a field-name for better logging (defaulting to `root`).
     * The JsonNode is an object (checked in toSparkSchema and buildDataType) and is expected
     * to contain a valid JSONSchema object-definition as subfields.
     *
     * This function recursively calls the buildStructField function to build every
     * property of the defined object associated field.
     */
    private def buildStructType(jsonSchema: JsonNode, fieldName: String = "root"): StructType = {
        val properties = jsonSchema.get(propertiesField)
        if (properties == null || !properties.isObject)
            throw new IllegalArgumentException(s"`$fieldName` struct schema's properties is not an object.")

        val requiredFieldValue = jsonSchema.get(requiredField)
        // If required is set, it must either be an array (Draft 4+) or a boolean (Draft 3)
        if (requiredFieldValue != null && !(requiredFieldValue.isArray || requiredFieldValue.isBoolean))
            throw new IllegalArgumentException(
                s"`$fieldName` struct schema's required is not an array or boolean (JSONSchema Draft3)."
            )

        val requiredFieldSet = Try(requiredFieldValue.elements().asScala.map(_.asText()).toSet).getOrElse(Set.empty)
        val fields = properties.fields.asScala.toSeq

        StructType(fields.map(mapEntry => {
            val subfieldName = mapEntry.getKey
            val subfieldNode = mapEntry.getValue
            buildStructField(subfieldNode, subfieldName, requiredFieldSet.contains(subfieldName))
        }))
    }

    /**
      * If a field has a 'required' property as a boolean, assume this is a JSONSchema Draft3
      * style schema and the required property is referring to the current field, not a list of
      * required sub properties.  This returns true if the jsonSchema has required: true
      *
      * @param jsonSchema
      * @return
      */
    private def isRequiredDraft3(jsonSchema: JsonNode): Boolean = {
        jsonSchema.has("required") &&
            jsonSchema.get("required").isBoolean &&
            jsonSchema.get("required").booleanValue
    }

    /**
     * Converts a JSON schema to a Spark StructType.
     *
     * Notes:
     * - Only a subset of JSONSchema is supported.
     * - object with additionalProperties only are converted to a Map.
     *
     * @param jsonSchema the JSONSchema root node
     * @return the spark schema StructType
     */
    def toSparkSchema(jsonSchema: JsonNode): StructType = {
        if (jsonSchema == null || jsonSchema.isNull)
            throw new IllegalArgumentException("Schema root node shouldn't be null when building Spark Schema")
        if (!jsonSchema.isObject || !jsonSchema.has(propertiesField))
            throw new IllegalArgumentException("Schema root node should be an object with properties when building Spark Schema")
        try {
            buildStructType(jsonSchema)
        } catch {
            // If we encounter an exception, it will be useful to know the full schema that
            // failed to be converted.  Log it here, and then re-raise the exception.
            case e: Exception => {
                log.error(
                    "Caught exception while attempting to convert JSONSchema to " +
                    "SparkSchema. JSONSchema was:\n" + jsonSchema,
                    e
                )
                throw e
            }
        }
    }
}
