package org.wikimedia.analytics.refinery.job.refine

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{MapType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame


/**
  * This module returns a transformation function that can be applied
  * to the Refine process to sanitize a given PartitionedDataFrame.
  *
  * The sanitization is done using a whitelist to determine which tables
  * and fields should be purged and which ones should be kept. The whitelist
  * is provided as a recurive tree of Map[String, Any], following format and
  * rules described below:
  *
  *   Map(
  *       "tableOne" -> Map(
  *           "fieldOne" -> "keep",
  *           "fieldTwo" -> "keepall",
  *           "fieldThree" -> Map(
  *               "subFieldOne" -> "keep"
  *           )
  *       ),
  *       "tableTwo" -> "keepall",
  *       "__defaults__" -> Map(
  *           "fieldFour" -> "keep"
  *       )
  *   )
  *
  *
  * TABLES:
  *
  * - The first level of the whitelist corresponds to table names.
  *
  * - If the table name of the given HivePartition is not present in the
  *   whitelist, the transformation function will return an empty DataFrame.
  *
  * - If the table name of the given HivePartition is present in the whitelist and
  *   is tagged as 'keepall', the transformation function will return the full
  *   DataFrame as is.
  *
  * - For tables, tags different from 'keepall' will throw an exception.
  *
  * - If the table name of the given HivePartition is present in the whitelist
  *   and its value is a Map, the transformation function will apply the
  *   sanitizations specified in that Map to the DataFrame's fields and return it.
  *   See: FIELDS.
  *
  *
  * FIELDS:
  *
  * - The second and subsequent levels of the whitelist correspond to field names.
  *
  * - If a field (or sub-field) name is not present in the corresponding whitelist
  *   Map, the transformation function will set that field to null for all records,
  *   regardless of field type.
  *
  * - Thus, all fields that are to be purged by this method, should be nullable.
  *   Otherwise, the transformation function will throw an exception.
  *
  * - If a field (or sub-field) name is present in the corresponding Map,
  *   it will be handled differently depending on its type.
  *
  *
  * FIELDS OF TYPE STRUCT OR MAP:
  *
  * - If a field name of type Struct/Map is present in the corresponding whitelist
  *   Map and is tagged 'keepall', the transformation function will copy the full
  *   Struct/Map content of that field to the returned DataFrame.
  *
  * - For fields of type Struct/Map, tags different from 'keepall' will throw an exception.
  *
  * - Struct/Map type fields, like tables, can have a Map as whitelist value as well.
  *   If a field name of Struct/Map type is present in the whitelist and its value
  *   is a Map, the transformation function will apply the sanitizations
  *   specified in that Map to its nested fields. See: FIELDS.
  *
  *
  * FIELDS OF TYPES DIFFERENT FROM STRUCT OR MAP:
  *
  * - If a field name of non-Struct/non-Map type is present in the corresponding
  *   whitelist Map and is tagged 'keep', the transformation function will copy
  *   its value to the returned DataFrame.
  *
  * - For non-Struct/non-Map type fields, tags different from 'keep' will throw an exception.
  *
  * - Non-Struct/non-Map type fields can not have Map values in the whitelist.
  *   If this happens, an exception will be thrown.
  *
  *
  * DEFAULTS SECTION
  *
  * - If the whitelist contains a top level key named '__defaults__', its spec
  *   will be applied as a default to all whitelisted tables.
  *
  * - Fields (or sub-fields) that are present in the defaults spec and are not
  *   present in the table-specific spec will be sanitized as indicated in the
  *   defaults spec.
  *
  * - Fields (or sub-fields) that are present in the table-specific spec will be
  *   sanitized as indicated in it, regardless of the defaults spec for that field.
  *
  * - Tables that are not present in the whitelist, will not be applied defaults.
  *   Hence, the transformation function will return an empty DataFrame.
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
  *   Struct/Map fields. Differentiating between 'keep' and 'keepall' allows to
  *   easily avoid unwanted use of the 'keepall' semantics.
  *
  * - Also, in the future, other tags that apply anonymization transformations
  *   might be implemented. For instance: 'bucket', 'hash', 'noise', etc.
  *
  */
object WhitelistSanitization {

    type Whitelist = Map[String, Any]

    val WhitelistDefaultsSectionLabel = "__defaults__"


    /**
      * The following tree structure stores a 'compiled' representation
      * of the whitelist. It is constructed prior to any data transformation,
      * so that the whitelist checks and lookups are performed only once per
      * table and not once per row.
      */
    sealed trait MaskNode {
        // Applies sanitization to a given value.
        def apply(value: Any): Any
        // Merges this mask with another given one.
        def merge(other: MaskNode): MaskNode
        // Returns whether this mask equals another given one.
        def equals(other: MaskNode): Boolean
    }

    // ValueMaskNode corresponds to simple (non-nested) sanitizations.
    case class ValueMaskNode(action: SanitizationAction.Value) extends MaskNode {
        // For value nodes the apply method performs the action
        // to sanitize the given value.
        def apply(value: Any): Any = {
            action match {
                case SanitizationAction.Identity => value
                case SanitizationAction.Nullify =>
                    value match {
                        case _: Map[String, Any] => Map()
                        case _ => null
                    }
            }
        }
        // Merges another mask node (overlay) on top of this one (base).
        def merge(other: MaskNode): MaskNode = {
            other match {
                case otherValue: ValueMaskNode =>
                    otherValue.action match {
                        case SanitizationAction.Nullify => this
                        case _ => other
                    }
                case _: StructMaskNode => other
                case _: MapMaskNode => other
            }
        }
        // For testing.
        def equals(other: MaskNode): Boolean = {
            other match {
                case otherValue: ValueMaskNode => action == otherValue.action
                case _ => false
            }
        }
    }

    // StructMaskNode corresponds to nested sanitizations on top of Struct values.
    case class StructMaskNode(children: Array[MaskNode]) extends MaskNode {
        // For struct nodes the apply function calls the apply function
        // on all fields of the given row.
        def apply(value: Any): Any = {
            val row = value.asInstanceOf[Row]
            Row.fromSeq(
              children.zip(row.toSeq).map { case (mask, v) => mask.apply(v) }
            )
        }
        // Merges another mask node (overlay) on top of this one (base).
        def merge(other: MaskNode): MaskNode = {
            other match {
                case otherValue: ValueMaskNode =>
                    otherValue.action match {
                        case SanitizationAction.Nullify => this
                        case _ => other
                    }
                case otherStruct: StructMaskNode =>
                    StructMaskNode(
                        children.zip(otherStruct.children).map { case (a, b) => a.merge(b) }
                    )
            }
        }
        // For testing.
        def equals(other: MaskNode): Boolean = {
            other match {
                case otherStruct: StructMaskNode => (
                    children.size == otherStruct.children.size &&
                    children.zip(otherStruct.children).foldLeft(true) {
                        case (result, pair) => result && pair._1.equals(pair._2)
                    }
                )
                case _ => false
            }
        }
    }

    // StructMaskNode corresponds to nested sanitizations on top of Map values.
    case class MapMaskNode(whitelist: Whitelist) extends MaskNode {
        // For map nodes the apply function applies the map whitelist
        // on all key-value pairs of the given map.
        def apply(value: Any): Any = {
            val valueMap = value.asInstanceOf[Map[String, Any]]
            valueMap.flatMap { case (key, value) =>
                if (whitelist.contains(key)) {
                    Seq(
                        key -> (whitelist(key) match {
                            case SanitizationAction.Identity => value
                            case childMask: MapMaskNode => childMask.apply(value)
                        })
                    )
                } else Seq.empty
            }
        }
        // Merges another mask node (overlay) on top of this one (base).
        def merge(other: MaskNode): MaskNode = {
            other match {
                case otherValue: ValueMaskNode =>
                    otherValue.action match {
                        case SanitizationAction.Nullify => this
                        case _ => other
                    }
                case otherMap: MapMaskNode =>
                    val otherWhitelist = otherMap.whitelist
                    MapMaskNode(
                        whitelist.filterKeys(k => !otherWhitelist.contains(k)) ++
                        otherWhitelist.filterKeys(k => !whitelist.contains(k)) ++
                        whitelist.keys.filter(k => otherWhitelist.contains(k)).map { k =>
                            (whitelist(k), otherWhitelist(k)) match {
                                case (a: MapMaskNode, b: MapMaskNode) => k -> a.merge(b)
                                case _ => k -> otherWhitelist(k)
                            }
                        }.toMap
                    )
            }
        }
        // For testing.
        def equals(other: MaskNode): Boolean = {
            other match {
                case otherMap: MapMaskNode => whitelist == otherMap.whitelist
                case _ => false
            }
        }
    }

    // Sanitization actions for the ValueMaskNode to apply.
    object SanitizationAction extends Enumeration {
        val Identity, Nullify = Value
    }


    /**
     * Returns a transformation function to be used in the Refine process
     * to sanitize a given PartitionedDataFrame. The sanitization is
     * based on the specified whitelist. See comment at the top of this
     * module for more details on the whitelist format.
     *
     * @param whitelist    The whitelist object (see type Whitelist).
     *
     * @return Refine.TransformFunction  See more details in Refine.scala.
     */
    def apply(
        whitelist: Whitelist
    ): PartitionedDataFrame => PartitionedDataFrame = {
        val lowerCaseWhitelist = makeWhitelistLowerCase(whitelist)
        (partDf: PartitionedDataFrame) => {
            sanitizeTable(
                partDf,
                lowerCaseWhitelist
            )
        }
    }

    /**
     * Recursively transforms all whitelist keys and tag values to lower case.
     */
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


    /**
     * Sanitizes a given PartitionedDataFrame with the specified whitelist.
     */
    def sanitizeTable(
        partDf: PartitionedDataFrame,
        whitelist: Whitelist
    ): PartitionedDataFrame = {
        whitelist.get(partDf.partition.table.toLowerCase) match {
            // Table is not in the whitelist: return empty DataFrame.
            case None => partDf.copy(df = emptyDataFrame(partDf.df.sparkSession, partDf.df.schema))
            // Table is in the whitelist as keepall: return DataFrame as is.
            case Some("keepall") => partDf
            // Table is in the whitelist and has further specifications:
            case Some(tableWhitelist: Whitelist) =>
                // Create table-specific sanitization mask.
                val tableSpecificMask = getStructMask(
                    partDf.df.schema,
                    tableWhitelist,
                    partDf.partition.keys
                )
                // Merge the table-specific mask with the defaults mask,
                // if the defaults section is present in the whitelist.
                val defaultsWhitelist = whitelist.get(WhitelistDefaultsSectionLabel)
                val sanitizationMask = if (defaultsWhitelist.isDefined) {
                    getStructMask(
                        partDf.df.schema,
                        defaultsWhitelist.get.asInstanceOf[Whitelist],
                        partDf.partition.keys
                    ).merge(tableSpecificMask)
                } else tableSpecificMask
                // Apply sanitization to the data.
                sanitizeDataFrame(
                    partDf,
                    sanitizationMask
                )
            case _ => throw new IllegalArgumentException(
                s"Invalid whitelist value for table '${partDf.partition.table}'."
            )
        }
    }

    /**
      * Returns a sanitization mask (compiled whitelist) for a given StructType and whitelist.
      * The `partitions` parameter enforces whitelisting partition columns.
      *
      * This function also validates that the given whitelist is correctly defined.
      */
    def getStructMask(
        struct: StructType,
        whitelist: Whitelist,
        partitions: Seq[String] = Seq.empty
    ): MaskNode = {
        StructMaskNode(
            struct.fields.map { field =>
                if (partitions.contains(field.name)) {
                    // The field is a partition field and should be kept.
                    ValueMaskNode(SanitizationAction.Identity)
                } else {
                    val lowerCaseFieldName = field.name.toLowerCase
                    if (whitelist.contains(lowerCaseFieldName)) {
                        // The field is in the whitelist and should be fully or partially kept.
                        getValueMask(field, whitelist(lowerCaseFieldName))
                    } else {
                        // The field is not in the whitelist and should be purged.
                        if (field.nullable) {
                            ValueMaskNode(SanitizationAction.Nullify)
                        } else {
                            throw new RuntimeException(
                                s"Field '${field.name}' needs to be nullified but is not nullable."
                            )
                        }
                    }
                }
            }
        )
    }

    /**
      * Returns a sanitization mask (compiled whitelist) for a given MapType and whitelist.
      * As opposed to the StructMask (that uses implicit indexes), this mask uses lookups
      * to determine which fields to keep or purge. The reason being that Maps do not
      * guarantee the order their elements are iterated.
      * Thus, Maps are less performant than Structs in this case.
      *
      * This function also validates that the given whitelist is correctly defined.
      */
    def getMapMask(
        map: MapType,
        whitelist: Whitelist
    ): MaskNode = {
        MapMaskNode(
            map.valueType match {
                case MapType(_, _, _) => whitelist.map { case (key, value) =>
                    // The whitelist for this field indicates the field is nested.
                    // Build the MaskNode accordingly. If necessary, call recursively.
                    value match {
                        case "keepall" => key -> SanitizationAction.Identity
                        case childWhitelist: Whitelist =>
                            key -> getMapMask(map.valueType.asInstanceOf[MapType], childWhitelist)
                        case _ => throw new IllegalArgumentException(
                            s"Invalid whitelist value for map key '${key}'."
                        )
                    }
                }
                case _ => whitelist.map { case (key, value) =>
                    // The whitelist for this field indicates the field is simple (not nested).
                    // Build the MaskNode accordingly.
                    value match {
                        case "keep" => key -> SanitizationAction.Identity
                        case _ => throw new IllegalArgumentException(
                            s"Invalid whitelist value for map key '${key}'."
                        )
                    }
                }
            }
        )
    }

    /**
      * Returns a sanitization mask (compiled whitelist) for a given StructField and whitelist.
      *
      * This function also validates that the given whitelist is correctly defined.
      */
    def getValueMask(
        field: StructField,
        whitelistValue: Any
    ): MaskNode = {
        field.dataType match {
            case StructType(_) | MapType(_, _, _) => whitelistValue match {
                // The field is nested, either StructType or MapType.
                // Build the MaskNode accordingly. If necessary, call recursively.
                case "keepall" => ValueMaskNode(SanitizationAction.Identity)
                case childWhitelist: Whitelist => field.dataType match {
                    case StructType(_) =>
                        getStructMask(
                            field.dataType.asInstanceOf[StructType],
                            childWhitelist
                        )
                    case MapType(_, _, _) =>
                        getMapMask(
                            field.dataType.asInstanceOf[MapType],
                            childWhitelist
                        )
                }
                case _ => throw new IllegalArgumentException(
                    s"Invalid whitelist value for nested field '${field.name}'."
                )
            }
            case _ => whitelistValue match {
                // The field is not nested. Build the MaskNode accordingly.
                case "keep" => ValueMaskNode(SanitizationAction.Identity)
                case _ => throw new IllegalArgumentException(
                    s"Invalid whitelist value for non-nested field '${field.name}'."
                )
            }
        }
    }

    /**
      * Applies a sanitization mask (compiled whitelist) to a DataFrame.
      */
    def sanitizeDataFrame(
        partDf: PartitionedDataFrame,
        sanitizationMask: MaskNode
    ): PartitionedDataFrame = {
        val schema = partDf.df.schema
        partDf.copy(df = partDf.df.sparkSession.createDataFrame(
            partDf.df.rdd.map { row =>
                sanitizationMask.apply(row).asInstanceOf[Row]
            },
            // Note that the dataFrame object can not be referenced from
            // within this closure, because its code executed in spark
            // workers, so trying to access dataFrame object from them
            // results in ugly exceptions. That's why the schema is
            // extracted into a variable.
            schema
        ))
    }

    /**
     * Returns an empty DataFrame.
     */
    def emptyDataFrame(
        spark: SparkSession,
        schema: StructType
    ): DataFrame = {
        val emptyRDD = spark.sparkContext.emptyRDD.asInstanceOf[RDD[Row]]
        spark.createDataFrame(emptyRDD, schema)
    }
}
