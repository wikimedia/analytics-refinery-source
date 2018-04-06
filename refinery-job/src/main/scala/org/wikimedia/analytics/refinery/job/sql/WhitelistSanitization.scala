package org.wikimedia.analytics.refinery.job.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.wikimedia.analytics.refinery.spark.sql.HivePartition


/**
  * This module returns a transformation function that can be applied
  * to the Refine process to sanitize a given DataFrame and HivePartition.
  *
  * The sanitization is done using a whitelist to determine which tables
  * and fields should be purged and which ones should be kept. The whitelist
  * is provided as a recurive tree of Map[String, Any], following format and
  * rules described below:
  *   Map(
  *     "tableOne" -> Map(
  *         "fieldOne" -> "keep",
  *         "fieldTwo" -> "keepall",
  *         "fieldThree" -> Map(
  *             "subFieldOne" -> "keep"
  *        )
  *    ),
  *    "tableTwo" -> "keepall"
  *  )
  *
  *
  * TABLES:
  *
  * - The first level of the whitelist corresponds to table names.
  *
  * - If the table name of the given HivePartition is not present in the
  *   whitelist, the transformation function will return an empty DataFrame.
  *
  * - If the table name of the given DataFrame is present in the whitelist and
  *   is tagged as 'keepall', the transformation function will return the full
  *   DataFrame as is.
  *
  * - For tables, tags different from 'keepall' will throw an exception.
  *
  * - If the table name of the given DataFrame is present in the whitelist and
  *   has a Map as value, the transformation function will apply the
  *   sanitizations specified in that map to the DataFrame's fields
  *   and return it. See: FIELDS.
  *
  *
  * FIELDS:
  *
  * - The second and subsequent levels of the whitelist correspond to field
  *   names. It's assumed from now on, that the parent table (or struct field)
  *   is present in the whitelist, and that it has a Map as value.
  *
  * - If a field (or sub-field) name is not present in the corresponding
  *   Map, the transformation function will set all records for that
  *   field to null, regardless of field type.
  *
  * - Thus, all fields that are to be purged by this method, should be nullable.
  *   Otherwise, the transformation function will throw an exception.
  *
  * - If a field (or sub-field) name is present in the corresponding Map,
  *   it will handled differently depending on its type.
  *
  *
  * FIELDS OF TYPE STRUCT:
  *
  * - If a field name of type struct is present in the corresponding indented
  *   block and is tagged 'keepall', the transformation function will fully
  *   copy the full struct content of that field to the returned DataFrame.
  *
  * - For fields of struct type, tags different from 'keepall' will throw an exception.
  *
  * - Struct type fields, like tables, can have a Map as value as well.
  *   If a field name of struct type is present in the whitelist and has
  *   a Map value, the transformation function will apply the sanitizations
  *   specified in that Map to its nested fields. See: FIELDS.
  *
  *
  * FIELDS OF TYPES DIFFERENT FROM STRUCT:
  *
  * - If a field name of non-struct type is present in the corresponding Map
  *   and is tagged 'keep', the transformation function will copy the value of
  *   that field to the returned DataFrame.
  *
  * - For non-sruct type fields, tags different from 'keep' will throw an exception.
  *
  * - Non-struct type fields can not open indented blocks. If this happens, the
  *   whitelist will not validate.
  *
  *
  * WHY USE 2 DIFFERENT TAGS (KEEP AND KEEPALL)?
  *
  * - Different data sets might need sanitization for different reasons.
  *   For some of them, convenience might be more important than robustness.
  *   In these cases, the use of 'keepall' can save lots of lines of code.
  *   For other data sets, robustness will be the most important thing. In
  *   those cases, the use of 'keepall' might be dangerous, because it doesn't
  *   have control over new fields added to tables or new sub-fields added to
  *   struct fields. Differentiating between 'keep' and 'keepall' allows to
  *   easily avoid unwanted use of the 'keepall' semantics.
  *
  */
object WhitelistSanitization {

    type Whitelist = Map[String, Any]

    /**
      * The following tree structure stores a 'compiled' representation
      * of the whitelist. It is constructed prior to any data transformation,
      * so that the whitelist checks and lookups are performed only once per
      * table and not once per row.
      */
    sealed trait MaskNode {
        def apply(value: Any): Any
    }
    case class MaskLeafNode(action: SanitizationAction.Value) extends MaskNode {
        // For leaf nodes the apply method performs the action
        // to sanitize the given value.
        def apply(value: Any): Any = {
            action match {
                case SanitizationAction.Identity => value
                case SanitizationAction.Nullify => null
            }
        }
    }
    case class MaskInnerNode(children: Array[MaskNode]) extends MaskNode {
        // For inner nodes the apply function calls the apply function
        // on all fields of the given row.
        def apply(value: Any): Any = {
            val row = value.asInstanceOf[Row]
            Row.fromSeq(
              children.zip(row.toSeq).map { case (mask, v) => mask.apply(v) }
            )
        }
    }
    /**
      * Sanitization actions for the MaskLeafNode to apply.
      */
    object SanitizationAction extends Enumeration {
        val Identity, Nullify = Value
    }

    /**
     * Returns a transformation function to be used in the Refine process
     * to sanitize a given DataFrame and HivePartition. The sanitization is
     * based on the specified whitelist. See comment at the top of this
     * module for more details on the whitelist format.
     *
     * @param whitelist    The whitelist object (see type Whitelist).
     *
     * @return Refine.TransformFunction  See more details in Refine.scala.
     */
    def apply(
        whitelist: Whitelist
    ): (DataFrame, HivePartition) => DataFrame = {
        val lowerCaseWhitelist = makeWhitelistLowerCase(whitelist)
        (dataFrame: DataFrame, hivePartition: HivePartition) => {
            sanitizeTable(
                dataFrame,
                hivePartition.table,
                hivePartition.keys,
                lowerCaseWhitelist
            )
        }
    }

    // Recursively transforms all whitelist keys
    // and tag values to lower case.
    def makeWhitelistLowerCase(
        whitelist: Whitelist
    ): Whitelist = {
        whitelist.map { case (key, value) =>
            key.toLowerCase -> (value match {
                case tag: String => tag.replaceAll("[-_]", "").toLowerCase
                case childWhitelist: Whitelist => makeWhitelistLowerCase(childWhitelist)
            })
        }
    }

    def sanitizeTable(
        dataFrame: DataFrame,
        tableName: String,
        partitionKeys: Seq[String],
        whitelist: Whitelist
    ): DataFrame = {
        whitelist.get(tableName.toLowerCase) match {
            // Table is not in the whitelist: return empty DataFrame.
            case None => emptyDataFrame(dataFrame.sparkSession, dataFrame.schema)
            // Table is in the whitelist as keepall: return DataFrame as is.
            case Some("keepall") => dataFrame
            // Table is in the whitelist and has further specifications:
            case Some(tableWhitelist: Whitelist) =>
                // Create sanitization mask (compiled whitelist).
                val sanitizationMask = getStructMask(
                    dataFrame.schema,
                    tableWhitelist,
                    partitionKeys
                )
                // Apply sanitization to the data.
                sanitizeDataFrame(
                    dataFrame,
                    sanitizationMask
                )
            case _ => throw new Exception(
                s"Invalid whitelist value for table '$tableName'."
            )
        }
    }

    /**
      * Returns a sanitization mask (compiled whitelist) for a given StructType and whitelist.
      * The `partitions` parameter enforces whitelisting partition columns.
      *
      * NOTICE: This function actually validates that the given whitelist is correctly defined.
      *
      */
    def getStructMask(
        struct: StructType,
        whitelist: Whitelist,
        partitions: Seq[String] = Seq.empty
    ): MaskNode = {
        MaskInnerNode(
            struct.fields.map { field =>
                if (partitions.contains(field.name)) MaskLeafNode(SanitizationAction.Identity)
                else getFieldMask(field, whitelist)
            }
        )
    }

    /**
      * Returns a sanitization mask (compiled whitelist) for a given StructField and whitelist.
      */
    def getFieldMask(
        field: StructField,
        whitelist: Whitelist
    ): MaskNode = {
        val lowerCaseFieldName = field.name.toLowerCase
        if (whitelist.contains(lowerCaseFieldName)) {
            // The field is in the whitelist and should be fully or partially kept.
            field.dataType match {
                case StructType(_) =>
                    // The field contains a nested object.
                    whitelist(lowerCaseFieldName) match {
                        // The field is to be kept entirely: apply identity.
                        case "keepall" => MaskLeafNode(SanitizationAction.Identity)
                        // The field has further specifications: continue recursively.
                        case childWhitelist: Whitelist =>
                            val struct = field.dataType.asInstanceOf[StructType]
                            getStructMask(struct, childWhitelist)
                        // Invalid whitelist value.
                        case _ => throw new IllegalArgumentException(
                            s"Invalid whitelist value for nested field '${field.name}'."
                        )
                    }
                case _ =>
                    // The field contains a simple value.
                    whitelist(lowerCaseFieldName) match {
                        // The field is to be kept: apply identity.
                        case "keep" => MaskLeafNode(SanitizationAction.Identity)
                        // Invalid whitelist value.
                        case _ => throw new Exception(
                            s"Invalid whitelist value for non-nested field '${field.name}'."
                        )
                    }
            }
        } else {
            // The field is not in the whitelist and should be purged:
            // apply nullify or fail if field is not nullable.
            if (field.nullable) MaskLeafNode(SanitizationAction.Nullify) else {
                throw new Exception(
                    s"Field '${field.name}' needs to be nullified but is not nullable."
                )
            }
        }
    }

    /**
      * Applies a sanitization mask (compiled whitelist) to a DataFrame.
      */
    def sanitizeDataFrame(
        dataFrame: DataFrame,
        sanitizationMask: MaskNode
    ): DataFrame = {
        val schema = dataFrame.schema
        dataFrame.sqlContext.createDataFrame(
            dataFrame.rdd.map { row =>
                sanitizationMask.apply(row).asInstanceOf[Row]
            },
            // Note that the dataFrame object can not be referenced from
            // within this closure, because its code executed in spark
            // workers, so trying to access dataFrame object from them
            // results in ugly exceptions. That's why the schema is
            // extracted into a variable.
            schema
        )
    }

    def emptyDataFrame(
        spark: SparkSession,
        schema: StructType
    ): DataFrame = {
        val emptyRDD = spark.sparkContext.emptyRDD.asInstanceOf[RDD[Row]]
        spark.createDataFrame(emptyRDD, schema)
    }
}
