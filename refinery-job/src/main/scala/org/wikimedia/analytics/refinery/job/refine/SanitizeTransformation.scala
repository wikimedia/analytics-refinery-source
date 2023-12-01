package org.wikimedia.analytics.refinery.job.refine

import java.io.{BufferedReader, InputStreamReader}
import com.github.nscala_time.time.Imports.DateTimeFormat

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.joda.time.DateTime
import org.wikimedia.analytics.refinery.core.HivePartition
import org.wikimedia.analytics.refinery.spark.sql.{HiveExtensions, PartitionedDataFrame}
import org.wikimedia.analytics.refinery.tools.LogHelper
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._


/**
  * This module returns a transform function that can be applied
  * to the Refine process to sanitize a given PartitionedDataFrame.
  *
  * Note that this is not a Refine.TransformFunction on its own,
  * but that SanitizeTransformation.apply creates and returns
  * a Refine.TransformFunction based on on the input allowlist and salts.
  *
  * The sanitization is done using a allowlist to determine which tables
  * and fields should be purged and which ones should be kept. The allowlist
  * is provided as a recursive tree of Map[String, Any], following format and
  * rules described below:
  *
  *   Map(
  *       "tableOne" -> Map(
  *           "fieldOne" -> "keep",
  *           "fieldTwo" -> "keep_all",
  *           "fieldThree" -> Map(
  *               "subFieldOne" -> "keep"
  *           ),
  *           "fieldFour" -> "hash",
  *       ),
  *       "tableTwo" -> "keep_all",
  *       "__defaults__" -> Map(
  *           "fieldFour" -> "keep"
  *       )
  *   )
  *
  *
  * TABLES:
  *
  * - The first level of the allowlist corresponds to table names.
  *
  * - If the table name of the given HivePartition is not present in the
  *   allowlist, the transformation function will return an empty DataFrame.
  *
  * - If the table name of the given HivePartition is present in the allowlist and
  *   is tagged as 'keep_all', the transformation function will return the full
  *   DataFrame as is.
  *
  * - For tables, string tags different from 'keep_all' will throw an exception.
  *
  * - If the table name of the given HivePartition is present in the allowlist
  *   and its value is a Map, the transformation function will apply the
  *   sanitizations specified in that Map to the DataFrame's fields and return it.
  *   See: FIELDS.
  *
  *
  * FIELDS:
  *
  * - The second and subsequent levels of the allowlist correspond to field names.
  *
  * - If a field (or sub-field) name is not present in the corresponding allowlist
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
  * - If a field of type Struct/Map is present in the corresponding allowlist and
  *   Map and is tagged 'keep_all', the transformation function will copy the full
  *   Struct/Map content of that field to the returned DataFrame.
  *
  * - For fields of type Struct/Map, string tags different from 'keep_all' will throw an exception.
  *
  * - Struct/Map type fields, like tables, can have a Map as allowlist value as well.
  *   If a field of Struct/Map type is present in the allowlist and its value
  *   is a Map, the transformation function will apply the sanitizations
  *   specified in that Map to its nested fields. See: FIELDS.
  *
  *
  * FIELDS OF TYPES DIFFERENT FROM STRUCT OR MAP:
  *
  * - If a field name of non-Struct/non-Map type is present in the corresponding
  *   allowlist Map and is tagged 'keep', the transformation function will copy
  *   its value to the returned DataFrame.
  *
  * - For non-Struct/non-Map type fields, tags different from 'keep' will throw an exception.
  *
  * - Non-Struct/non-Map type fields can not have Map values in the allowlist.
  *   If this happens, an exception will be thrown.
  *
  * - Fields of some simple types (like String) allow additional tags with handy
  *   sanitization actions, read below.
  *
  *
  * HASHING:
  *
  * - If a field name of type String is present in the corresponding allowlist Map and
  *   is tagged 'hash', the transformation function will apply an HMAC algorithm to it
  *   (salt + hash) using the salts passed to AllowlistSanitization.apply() as private key
  *   and SHA-256 as hash function, and then copy the resulting number formatted as a
  *   64-character-long hexadecimal String to the returned DataFrame.
  *
  * - If the allowlist contains 'hash' tags, but the 'salts' parameter is not passed to
  *   AllowlistSanitization.apply(), an exception will be thrown.
  *
  * - Non-String type fields can not have 'hash' tags in the allowlist.
  *   If this happens, an exception will be thrown.
  *
  *
  * DEFAULTS SECTION:
  *
  * - If the allowlist contains a top level key named '__defaults__', its spec
  *   will be applied as a default to all allowlisted tables.
  *
  * - Fields (or sub-fields) that are present in the defaults spec and are not
  *   present in the table-specific spec will be sanitized as indicated in the
  *   defaults spec.
  *
  * - Fields (or sub-fields) that are present in the table-specific spec will be
  *   sanitized as indicated in it, regardless of the defaults spec for that field.
  *
  * - Tables that are not present in the allowlist, will not be applied defaults.
  *   Hence, the transformation function will return an empty DataFrame.
  *
  *
  * WHY USE 2 DIFFERENT TAGS: keep AND keep_all?
  *
  * - Different data sets might need sanitization for different reasons.
  *   For some of them, convenience might be more important than robustness.
  *   In these cases, the use of 'keep_all' can save lots of lines of code.
  *   For other data sets, robustness will be the most important thing. In
  *   those cases, the use of 'keep_all' might be dangerous, because it doesn't
  *   have control over new fields added to tables or new sub-fields added to
  *   Struct/Map fields. Differentiating between 'keep' and 'keep_all' allows to
  *   easily avoid unwanted use of the 'keep_all' semantics.
  *
  */
object SanitizeTransformation extends LogHelper {

    type Allowlist = Map[String, Any]

    val allowlistDefaultsSectionLabel = "__defaults__"
    val hashingAlgorithm              = "HmacSHA256"

    val keepAllTag = "keep_all"
    val keepTag = "keep"
    val hashTag = "hash"

    /**
      * The following tree structure stores a 'compiled' representation
      * of the allowlist. It is constructed prior to any data transformation,
      * so that the allowlist checks and lookups are performed only once per
      * table and not once per row.
      */
    sealed trait MaskNode {
        // Applies sanitization to a given value.
        def apply(value: Any): Any
        // Merges this mask with another given one.
        def merge(other: MaskNode): MaskNode
    }

    // ValueMaskNode corresponds to simple (non-nested) sanitizations.
    case class ValueMaskNode(action: SanitizationAction) extends MaskNode {
        // For value nodes the apply method performs the action
        // to sanitize the given value.
        def apply(value: Any): Any = {
            action.apply(value)
        }
        // Merges another mask node (overlay) on top of this one (base).
        def merge(other: MaskNode): MaskNode = {
            other match {
                case otherValue: ValueMaskNode =>
                    otherValue.action match {
                        case Nullify() => this
                        case _ => other
                    }
                case _: StructMaskNode => other
                case _: MapMaskNode => other
            }
        }
    }

    // StructMaskNode corresponds to nested sanitizations on top of Struct values.
    case class StructMaskNode(children: Array[MaskNode]) extends MaskNode {
        // For struct nodes the apply function calls the apply function
        // on all fields of the given row.
        def apply(value: Any): Any = {
            if (value == null) {
                null
            } else {
                val row = value.asInstanceOf[Row]
                Row.fromSeq(
                  children.zip(row.toSeq).map { case (mask, v) => mask.apply(v) }
                )
            }
        }
        // Merges another mask node (overlay) on top of this one (base).
        def merge(other: MaskNode): MaskNode = {
            other match {
                case otherValue: ValueMaskNode =>
                    otherValue.action match {
                        case Nullify() => this
                        case _ => other
                    }
                case otherStruct: StructMaskNode =>
                    StructMaskNode(
                        children.zip(otherStruct.children).map { case (a, b) => a.merge(b) }
                    )
            }
        }
        override def toString: String = s"StructMaskNode(${children.mkString(", ")}"
    }

    // MapMaskNode corresponds to nested sanitizations on top of Map values.
    case class MapMaskNode(allowlist: Allowlist) extends MaskNode {
        // For map nodes the apply function applies the map allowlist
        // on all key-value pairs of the given map.
        def apply(value: Any): Any = {
            if (value == null) {
                null
            } else {
                val valueMap = value.asInstanceOf[Map[String, Any]]
                valueMap.flatMap { case (key, value) =>
                    val lowerCaseKey = key.toLowerCase
                    if (allowlist.contains(lowerCaseKey)) {
                        Seq(
                            key -> (allowlist(lowerCaseKey) match {
                                case childMapMask: MapMaskNode => childMapMask.apply(value)
                                case childStructMask: StructMaskNode => childStructMask.apply(value)
                                case action: SanitizationAction => action.apply(value)
                            })
                        )
                    } else Seq.empty
                }
            }
        }
        // Merges another mask node (overlay) on top of this one (base).
        def merge(other: MaskNode): MaskNode = {
            other match {
                case otherValue: ValueMaskNode =>
                    otherValue.action match {
                        case Nullify() => this
                        case _ => other
                    }
                case otherMap: MapMaskNode =>
                    val otherAllowlist = otherMap.allowlist
                    MapMaskNode(
                        allowlist.filterKeys(k => !otherAllowlist.contains(k)) ++
                        otherAllowlist.filterKeys(k => !allowlist.contains(k)) ++
                        allowlist.keys.filter(k => otherAllowlist.contains(k)).map { k =>
                            (allowlist(k), otherAllowlist(k)) match {
                                case (a: MapMaskNode, b: MapMaskNode) => k -> a.merge(b)
                                case _ => k -> otherAllowlist(k)
                            }
                        }.toMap
                    )
            }
        }
    }


    // Sanitization actions for the ValueMaskNode to apply.
    sealed trait SanitizationAction {
        def apply(value: Any): Any
    }
    case class Identity() extends SanitizationAction {
        def apply(value: Any): Any = value
    }
    case class Nullify() extends SanitizationAction {
        def apply(value: Any): Any = value match {
            case _: Map[String, Any] => Map()
            case _ => null
        }
    }
    case class Hash(salt: String) extends SanitizationAction {
        def apply(value: Any): Any = {
            if (value != null) {
                // The initialization of the mac object could have been done
                // when constructing the Hash instance, if it wasn't because
                // javax.crypto.Mac instances are not serializable...
                val mac = Mac.getInstance(hashingAlgorithm)
                val keySpec = new SecretKeySpec(salt.getBytes, hashingAlgorithm)
                mac.init(keySpec)
                val messageBytes = value.asInstanceOf[String].getBytes
                val hashBytes: Array[Byte] = mac.doFinal(messageBytes)
                hashBytes.map(b => "%02X".format(b)).mkString
            } else null
        }
    }


    /**
     * Returns a transformation function to be used in the Refine process
     * to sanitize a given PartitionedDataFrame. The sanitization is
     * based on the specified allowlist. See comment at the top of this
     * module for more details on the allowlist format.
     *
     * @param allowlist    The allowlist object (see type Allowlist).
     * @param salts        Seq of Tuples (startDateTime, endDateTime, saltString)
     *                     used to securely hash specified fields depending on time.
     *                     Required only when the allowlist contains the tag 'hash'.
     *
     * @return Refine.TransformFunction  See more details in Refine.scala.
     */
    def apply(
        allowlist: Allowlist,
        salts: Seq[(DateTime, DateTime, String)] = Seq.empty
    ): PartitionedDataFrame => PartitionedDataFrame = {
        (partDf: PartitionedDataFrame) => {
            val salt = chooseSalt(salts, partDf.partition)
            sanitizeTable(
                partDf,
                allowlist,
                salt
            )
        }
    }

    /**
      * Loads allowlist from allowListPath in fs.
      *
      * @param fs
      * @param allowlistPath
      * @param keepAllTagEnabled
      * @return
      */
    def loadAllowlist(fs: FileSystem)(
        allowlistPath: String,
        keepAllTagEnabled: Boolean = false
    ): Allowlist = {
        // Load allowlist from YAML file.
        convertAllowlist(
            new Yaml().load[Object](fs.open(new Path(allowlistPath))),
            keepAllTagEnabled
        )
    }

    /**
      * Tries to cast a java object into a [[SanitizeTransformation.Allowlist]],
      * and enforce keep_all_enabled.
      *
      * Throws a ClassCastException in case of casting failure,
      * or an IllegalArgumentException in case of keep_all tag used
      *
      * @param javaObject  The unchecked allowlist structure
      * @return a Map[String, Any] having no keep_all tag.
      */
    def convertAllowlist(
        javaObject: Object,
        keepAllTagEnabled: Boolean = false
    ): Allowlist = {
        // Transform to java map.
        val javaMap = try {
            javaObject.asInstanceOf[java.util.Map[String, Any]]
        } catch {
            case e: ClassCastException => throw new ClassCastException(
                "Allowlist object can not be cast to Map[String, Any]. " + e.getMessage
            )
        }

        // Apply recursively.
        javaMap.asScala.toMap.map { case (key, value) =>
            val newValue = value match {
                case `keepAllTag` if !keepAllTagEnabled => throw new IllegalArgumentException(
                    s"'${keepAllTag}' tag is not permitted in sanitization allowlist. ($key: $value)"
                )
                case `keepTag` | `hashTag` | `keepAllTag` => value
                case nested: Object => convertAllowlist(nested, keepAllTagEnabled)
                case _ => throw new IllegalArgumentException(
                    s"'${value}' tag is not a sanitization tag. ($key: $value)"
                )
            }

            // Allowlist keys are either Hive table or field names. Normalize them.
            HiveExtensions.normalizeName(key) -> newValue
        }
    }

    /**
      * Loads the salts stored in a salts directory in HDFS
      * and returns them in the format expected by SanitizeTransformation function.
      * A Seq of tuples of the form: (<startDateTime>, <endDateTime>, <saltString>)
      */
    def loadHashingSalts(fs: FileSystem)(
        saltsPath: String
    ): Seq[(DateTime, DateTime, String)] = {
        val dateTimeFormatter = DateTimeFormat.forPattern("yyyyMMddHH")
        val status = fs.listStatus(new Path(saltsPath))
        status.map((s) => {
            val fileNameParts = s.getPath.getName.split("_")
            val startDateTime = DateTime.parse(fileNameParts(0), dateTimeFormatter)
            // If file name does not have second component,
            // means the salt does not expire.
            val endDateTime = if (fileNameParts(1) != "") {
                DateTime.parse(fileNameParts(1), dateTimeFormatter)
            } else new DateTime(3000, 1, 1, 0, 0) // Never expires.
            val saltStream = fs.open(s.getPath)
            val saltReader = new BufferedReader(new InputStreamReader(saltStream))
            val salt = saltReader.lines.toArray.mkString
            (startDateTime, endDateTime, salt)
        })
    }

    /**
     * Searches through the list of provided salts
     * to find one that fits the given partition interval.
     */
    def chooseSalt(
        salts: Seq[(DateTime, DateTime, String)],
        partition: HivePartition
    ): Option[String] = {
        val (partitionStart, partitionEnd) = getPartitionStartAndEnd(partition)
        salts.find((salt) => {
            val (saltStart, saltEnd, saltString) = salt
            (partitionStart.getMillis() >= saltStart.getMillis() &&
             partitionEnd.getMillis() <= saltEnd.getMillis())
        }) match {
            case None => None
            case Some(chosenSalt) => Some(chosenSalt._3)
        }
    }

    /**
     * Extract start and end DateTimes from given HivePartition.
     */
    def getPartitionStartAndEnd(
        partition: HivePartition
    ): (DateTime, DateTime) = {
        val year = partition.get("year").get.get.toInt
        if (partition.get("month").isDefined) {
            val month = partition.get("month").get.get.toInt
            if (partition.get("day").isDefined) {
                val day = partition.get("day").get.get.toInt
                if (partition.get("hour").isDefined) {
                    val hour = partition.get("hour").get.get.toInt
                    val startDateTime = new DateTime(year, month, day, hour, 0)
                    (startDateTime, startDateTime.plusHours(1))
                } else {
                    val startDateTime = new DateTime(year, month, day, 0, 0)
                    (startDateTime, startDateTime.plusDays(1))
                }
            } else {
                val startDateTime = new DateTime(year, month, 1, 0, 0)
                (startDateTime, startDateTime.plusMonths(1))
            }
        } else {
            val startDateTime = new DateTime(year, 1, 1, 0, 0)
            (startDateTime, startDateTime.plusYears(1))
        }
    }

    /**
     * Sanitizes a given PartitionedDataFrame with the specified allowlist.
     */
    def sanitizeTable(
        partDf: PartitionedDataFrame,
        allowlist: Allowlist,
        salt: Option[String] = None
    ): PartitionedDataFrame = {
        val tableName = partDf.partition.table
        allowlist.get(tableName.toLowerCase) match {
            // Table is not in the allowlist: return empty DataFrame.
            case None => {
                log.debug(s"$tableName is not in the allowlist, returning an empty DataFrame")
                partDf.copy(df = emptyDataFrame(partDf.df.sparkSession, partDf.df.schema))
            }
            // Table is in the allowlist as keep_all: return DataFrame as is.
            case Some(`keepAllTag`) => {
                log.debug(s"$tableName is marked as $keepAllTag, returning the DataFrame as is.")
                partDf
            }
            // Table is in the allowlist and has further specifications:
            case Some(tableAllowlist: Allowlist) =>
                // Create table-specific sanitization mask.
                val tableSpecificMask = getStructMask(
                    partDf.df.schema,
                    tableAllowlist,
                    salt,
                    partDf.partition.keys
                )
                // Merge the table-specific mask with the defaults mask,
                // if the defaults section is present in the allowlist.
                val defaultsAllowlist = allowlist.get(allowlistDefaultsSectionLabel)
                val sanitizationMask = if (defaultsAllowlist.isDefined) {
                    getStructMask(
                        partDf.df.schema,
                        defaultsAllowlist.get.asInstanceOf[Allowlist],
                        salt,
                        partDf.partition.keys
                    ).merge(tableSpecificMask)
                } else tableSpecificMask
                // Apply sanitization to the data.
                sanitizeDataFrame(
                    partDf,
                    sanitizationMask
                )
            case _ => throw new IllegalArgumentException(
                s"Invalid allowlist value for table `$tableName`."
            )
        }
    }

    /**
      * Returns a sanitization mask (compiled allowlist) for a given StructType and allowlist.
      * The `partitions` parameter enforces allowlisting partition columns.
      *
      * This function also validates that the given allowlist is correctly defined.
      */
    def getStructMask(
        struct: StructType,
        allowlist: Allowlist,
        salt: Option[String] = None,
        partitions: Seq[String] = Seq.empty
    ): MaskNode = {
        StructMaskNode(
            struct.fields.map { field =>
                if (partitions.contains(field.name)) {
                    // The field is a partition field and should be kept.
                    ValueMaskNode(Identity())
                } else {
                    val lowerCaseFieldName = field.name.toLowerCase
                    if (allowlist.contains(lowerCaseFieldName)) {
                        // The field is in the allowlist and should be fully or partially kept.
                        getValueMask(field, allowlist(lowerCaseFieldName), salt)
                    } else {
                        // The field is not in the allowlist and should be purged.
                        if (field.nullable) {
                            ValueMaskNode(Nullify())
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
      * Returns a sanitization mask (compiled allowlist) for a given MapType and allowlist.
      * As opposed to the StructMask (that uses implicit indexes), this mask uses lookups
      * to determine which fields to keep or purge. The reason being that Maps do not
      * guarantee the order their elements are iterated.
      * Thus, Maps are less performant than Structs in this case.
      *
      * This function also validates that the given allowlist is correctly defined.
      */
    def getMapMask(
        map: MapType,
        allowlist: Allowlist,
        salt: Option[String] = None
    ): MaskNode = {
        MapMaskNode(
            map.valueType match {
                case StructType(_) | MapType(_, _, _) => allowlist.map { case (key, value) =>
                    // The allowlist for this field indicates the field is nested.
                    // Build the MaskNode accordingly. If necessary, call recursively.
                    value match {
                        case `keepAllTag` => key -> Identity()
                        case childAllowlist: Allowlist => map.valueType match {
                            case StructType(_) =>
                                key -> getStructMask(map.valueType.asInstanceOf[StructType], childAllowlist, salt)
                            case MapType(_, _, _) =>
                                key -> getMapMask(map.valueType.asInstanceOf[MapType], childAllowlist, salt)
                        }
                        case _ => throw new IllegalArgumentException(
                            s"Invalid allowlist value for map key '${key}'. Value found '${value}'."
                        )
                    }
                }
                case _ => allowlist.map { case (key, value) =>
                    // The allowlist for this field indicates the field is simple (not nested).
                    // Build the MaskNode accordingly.
                    value match {
                        case `keepTag` => key -> Identity()
                        case `hashTag` if map.valueType == StringType && salt.isDefined => key -> Hash(salt.get)
                        case _ => throw new IllegalArgumentException(
                            s"Invalid salt or allowlist value for map key '${key}'. Value found '${value}'."
                        )
                    }
                }
            }
        )
    }

    /**
      * Returns a sanitization mask (compiled allowlist) for a given StructField and allowlist.
      *
      * This function also validates that the given allowlist is correctly defined.
      */
    def getValueMask(
        field: StructField,
        allowlistValue: Any,
        salt: Option[String] = None
    ): MaskNode = {
        field.dataType match {
            case StructType(_) | MapType(_, _, _) => allowlistValue match {
                // The field is nested, either StructType or MapType.
                // Build the MaskNode accordingly. If necessary, call recursively.
                case `keepAllTag` => ValueMaskNode(Identity())
                case childAllowlist: Allowlist => field.dataType match {
                    case StructType(_) =>
                        getStructMask(
                            field.dataType.asInstanceOf[StructType],
                            childAllowlist,
                            salt
                        )
                    case MapType(_, _, _) =>
                        getMapMask(
                            field.dataType.asInstanceOf[MapType],
                            childAllowlist,
                            salt
                        )
                }
                case _ => throw new IllegalArgumentException(
                    s"Invalid allowlist value ${allowlistValue} for nested field ${field.toDDL}"
                )
            }
            case _ => allowlistValue match {
                // The field is not nested. Build the MaskNode accordingly.
                case `keepTag` => ValueMaskNode(Identity())
                case `hashTag` if field.dataType == StringType => {
                    if (salt.isEmpty) {
                        throw new IllegalArgumentException(
                            s"Salt must be defined in order to use allowlist value " +
                            s"$allowlistValue for field ${field.toDDL}"
                        )
                    }
                    ValueMaskNode(Hash(salt.get))
                }
                case _ => throw new IllegalArgumentException(
                    s"Invalid allowlist value ${allowlistValue} for field ${field.toDDL}"
                )
            }
        }
    }

    /**
      * Applies a sanitization mask (compiled allowlist) to a DataFrame.
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
