package org.wikimedia.analytics.refinery.spark.sql

import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, FloatType, IntegerType, LongType, MapType, Metadata, StructField, StructType}
import org.wikimedia.analytics.refinery.core.HivePartition

import java.util.UUID
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util.{escapeSingleQuotedString, quoteIdentifier}
import org.wikimedia.analytics.refinery.tools.LogHelper

import scala.annotation.tailrec
import scala.util.Try
import scala.util.matching.Regex

object SparkSqlExtensions extends LogHelper {

    /**
     * Normalizes a Hive table or field name using HivePartition.normalize.
     *
     * @param name name to normalize
     * @param lowerCase whether to convert the name to lower case.
     * @return
     */
    def normalizeName(name: String, lowerCase: Boolean = true): String = {
        HivePartition.normalize(name, lowerCase)
    }

    /**
     * Remove multiple spaces from a string and trim it.
     * Used to cleanup comments.
     * @param comment a string to cleanup
     * @return the cleaned up string
     */
    private def removeSpaces(comment: String): String = comment.replaceAll("\\s+", " ").trim

    private val fieldMatcher: Regex = "(`.+`|[a-zA-z0-9_]+)".r

    /**
     * Function splitting a fieldPath into its sub-fields. This function expects the field
     * to be correctly formatted and follows spark SQL identifiers specification defined
     * here: https://spark.apache.org/docs/latest/sql-ref-identifier.html
     *
     * @param fieldName the field to split
     * @return the parts of the field in order
     */
    def splitFieldName(fieldName: String): Seq[String] = {
        fieldMatcher
            .findAllMatchIn(fieldName)
            .map(m => m.group(0).stripPrefix("`").stripSuffix("`"))
            .toList
    }

    def quoteFieldPath(fieldPath: String): String = splitFieldName(fieldPath).map(quoteIdentifier).mkString(".")

    implicit class StructFieldExtensions(field: StructField) {

        /**
         * If possible, widens the field's type.  This currently only
         * widens integers to longs and floats to doubles.
         * @return
         */
        def widen(): StructField = field.dataType match {
                case IntegerType    => field.copy(dataType=LongType)
                case FloatType      => field.copy(dataType=DoubleType)
                case _              => field
            }

        /**
         * Returns a nullable or non nullable copy of this StructField.
         * @param nullable whether the field should be nullable
         * @return
         */
        def makeNullable(nullable: Boolean = true): StructField = field.copy(nullable=nullable)

        /**
         * Normalizes a copy of this StructField.
         * Here, normalizing means:
         * - conditionally convert field name to lower case
         * - Convert bad characters in field names to underscores.
         * - makeNullable true
         *
         * Ints are converted to Longs, Floats are converted to Doubles.
         * Longs and Doubles will handle more cases where field values
         * look like an int or float during one iteration, and a long or double later.
         *
         * @param lowerCase if true, the field name will be lower cases.  Default: true
         * @return
         */
        def normalize(lowerCase: Boolean = true): StructField = {
            field
                // convert bad chars to underscores
                .copy(name=SparkSqlExtensions.normalizeName(field.name, lowerCase))
                // make nullable
                .makeNullable()
        }

        /**
         * Normalizes the fields and widen some types (ints -> longs, floats -> doubles).
         * @return
         */
        def normalizeAndWiden(): StructField = {
            field.normalize().widen()
        }

        /**
         * Returns copy of this StructField with empty metadata
         * @return
         */
        def emptyMetadata: StructField = field.copy(metadata=Metadata.empty)

        /**
         * Returns the field path in the schema.
         * e.g. for a field named "foo" in a struct named "bar", the field path is "bar.foo".
         * @param parentFieldPath The parent field path if any.
         * @return The field path in the schema.
         */
        def fieldPath(parentFieldPath: Option[String]): String = if (parentFieldPath.isEmpty)
            field.name else s"${parentFieldPath.get}.${field.name}"

        /**
         * Returns the field path used to compute the children path of this field.
         * If the field is an array of struct, the field path for the children is "field.element".
         * @param parentFieldPath The parent field path if any.
         * @return The field path for the children of this field.
         */
        def fieldPathForChildren(parentFieldPath: Option[String]): String = field.dataType match {
            case ArrayType(StructType(_), _) => s"${field.fieldPath(parentFieldPath)}.element"
            case _ => field.fieldPath(parentFieldPath)
        }

        /**
         * Returns true if this field.dataType is a StructType or an ArrayType of StructType,
         * or a map with a value of StructType, else False.
         * @return
         */
        def isTypeWithSubfields: Boolean = {
            field.dataType match {
                case ArrayType(StructType(_), _) => true
                case StructType(_) => true
                case MapType(_, StructType(_), _) => true
                case _ => false
            }
        }

        /**
         * Converts the field's data type to a StructType if possible.
         *
         * This method examines the data type of the field. If the field's data type is an ArrayType containing
         * StructType, it extracts and returns the StructType. If the field's data type is a MapType with StructType
         * as its value type, it returns the StructType of the value. If the field itself is a StructType, it is
         * directly returned. For any other data type, an IllegalArgumentException is thrown, indicating
         * that the conversion to StructType is not possible.
         *
         * @throws IllegalArgumentException if the field's data type cannot be converted to a StructType.
         * @return The StructType representation of the field's data type.
         */
        def asStruct: StructType = {
            field.dataType match {
                case ArrayType(StructType(_), _) =>
                    field.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
                case MapType(_, StructType(_), _) =>
                    field.dataType.asInstanceOf[MapType].valueType.asInstanceOf[StructType]
                case StructType(_) => field.dataType.asInstanceOf[StructType]
                case _ => throw new IllegalArgumentException(s"Cannot convert field ${field.name} to a StructType")
            }
        }


        def sameCommentAs(otherField: StructField): Boolean =
            field.getCommentOrEmptyString == otherField.getCommentOrEmptyString


        def getCommentOrEmptyString: String = if (field.metadata.contains("comment"))
            removeSpaces(field.metadata.getString("comment")) else ""

        /**
         * Returns the SQL comment of the field, or None if no comment is set.
         */
        def getCommentAsDDL: String = field.getComment
            .map(removeSpaces)
            .map(escapeSingleQuotedString)
            .map(" COMMENT '" + _ + "'")
            .getOrElse("")

    }

    implicit class StructTypeExtensions(struct: StructType) {

        /**
         * Returns a copy of this struct with all fields 'normalized'.
         * If lowerCaseTopLevel is true, then top level field names will be lower cased.
         * This function recurses on sub structs, and normalizes them
         * with lowerCase = false, keeping the cases on sub struct field names.
         *
         * @param lowerCaseTopLevel Default: true
         * @return
         */
        def normalize(lowerCaseTopLevel: Boolean = true): StructType = {
            struct.convert((field, depth) => {
                if (depth == 0) field.normalize(lowerCase=lowerCaseTopLevel)
                else field.normalize(lowerCase=false)  // Don't lower case sub struct field names.
            })
        }

        /**
         * Recursively applies fn to each StructField in this schema and
         * replaces the field with the result of fn.
         *
         * @param fn The function to apply to the struct.
         *           The function takes a StructField and a depth of recursion.
         *           The depth is 0 for the top level fields, and increases by 1
         *           for each level of nested struct.
         * @param depth current depth of recursion
         *
         * @return The converted StructType schema
         */
        def convert(fn: (StructField, Int) => StructField, depth: Int = 0): StructType = {
            StructType(struct.foldLeft(Seq.empty[StructField])(
                (convertedFields: Seq[StructField], field: StructField) => {
                    val convertedField = fn(field, depth)
                    convertedField.dataType match {
                        case StructType(_) => convertedFields :+ convertedField.copy(
                            dataType=convertedField.dataType.asInstanceOf[StructType].convert(fn, depth + 1)
                        )
                        case _ => convertedFields :+ convertedField
                    }
                }
            ))
        }

        /**
         * All ints will be converted to longs, and all floats will be
         * * converted to doubles.  A field value that may
         * at one time look like an int, may during a later iteration
         * look like a long.  We choose to always use the wider data type.
         *
         * @return
         */
        def widen(): StructType = {
            struct.convert((field, depth) => field.widen())
        }

        def normalizeAndWiden(): StructType = struct.normalize().widen()

        /**
         * Recursively sets nullablity on every field in this schema and returns the new schema.
         * @param nullable
         * @return
         */
        def makeNullable(nullable: Boolean = true): StructType = {
            struct.convert((field, _) => field.makeNullable(nullable))
        }

        def findFieldIgnoreCase(newFieldName: String): Option[StructField] = struct
            .find(_.name.toLowerCase == newFieldName.toLowerCase)

        def fieldExistsIgnoreCase(newFieldName: String): Boolean = struct
            .exists(_.name.toLowerCase == newFieldName.toLowerCase)

        /**
         * Recursively empty metadata on every field in this schema and returns the new schema.
         * @return
         */
        def emptyMetadata: StructType = {
            struct.convert((field, _) => field.emptyMetadata)
        }

        /**
         * Function recursively checking if a field exists in a schema. This is useful for struct subfields.
         * Note: Map-keys can be accessed using doted notation in Spark. In this case,
         *       spark doesn't validate the key itself as it would require accessing the
         *       underlying data, but it validates that the subfield-name can be coerced
         *       to the map KeyType. In this function we don't do the type checking, we
         *       just assume that accessing the map-key will work.
         *
         * @param field the field to look for (using '.' as subfield separator)
         * @return true if the field is found
         */
        def fieldExists(field: String): Boolean = findSubfieldDataType(field).isDefined

        /**
         * Function recursively checking if a (sub)field exists in a schema, and if so returns
         * the DataType of the deepest subfield. This is useful for struct subfields.
         * Note: Map-keys can be accessed using doted notation in Spark. In this case,
         *       spark doesn't validate the key itself as it would require accessing the
         *       underlying data, but it validates that the subfield-name can be coerced
         *       to the map KeyType. In this function we don't do the type checking, we
         *       just assume that accessing the map-key will work.
         *
         * @param fieldName the fieldPath to look for (using '.' as subfield separator)
         * @return the DataType of the deepest subfield if found, None otherwise.
         */
        private def findSubfieldDataType(fieldName: String): Option[DataType] = {
            @tailrec
            def findSplitSubfieldDataType(struct: StructType, fields: Seq[String]): Option[DataType] = {
                fields match {
                    case Nil => None
                    case head +: tail =>
                        val headDataType = struct.fields.find(f => head.equals(f.name)).map(_.dataType)
                        headDataType match {
                            case Some(obj: StructType) if tail.nonEmpty => findSplitSubfieldDataType(obj, tail)
                            // Special case: a map key can be accessed as a field
                            // Type checking on key is not done, return the map value DataType
                            case Some(m: MapType) if tail.length == 1 => Some(m.valueType)
                            case Some(_) if tail.isEmpty => headDataType
                            case _ => None
                        }
                }
            }

            findSplitSubfieldDataType(struct, splitFieldName(fieldName))
        }
    }

    implicit class DataFrameExtensions(df: DataFrame) {
        def normalize(): DataFrame = {
            df.sparkSession.createDataFrame(df.rdd, df.schema.normalize())
        }

        def widen(): DataFrame = {
            df.convertToSchema(df.schema.widen())
        }

        def normalizeAndWiden(): DataFrame = {
            df.normalize().widen()
        }

        /**
         * Converts a DataFrame to schema.
         * The schema is expected to be an unordered superset of df's schema, i.e.
         * all fields from df.schema must exist in schema with compatible types
         * and similar nullableness.  Fields that exist in schema but not
         * in df.schema will be set to NULL (and as such must be nullable).  If this
         * condition does not match, this will throw an AssertionError.
         *
         * @param schema   schema to convert this df to
         *
         * @return         a DataFrame abiding to this struct (reordered fields and NULL new fields)
         */
        def convertToSchema(schema: StructType): DataFrame = {

            def buildSQLFieldsRec(srcSchema: StructType, dstSchema: StructType, depth: Int = 0, prefix: String = ""): String = {
                dstSchema.fields.map(dstField => {
                    val prefixedFieldName = if (depth == 0) dstField.name else s"$prefix.${dstField.name}"

                    def namedValue(value: String): String = {
                        if (depth == 0) s"$value AS ${dstField.name}" // Not in struct, aliases ok
                        else s"'${dstField.name}', $value"            // In named_struct, quoted-name then value
                    }

                    val idx = srcSchema.fieldNames.indexOf(dstField.name)
                    // No field in source, setting to NULL, casted for schema coherence
                    if (idx == -1) namedValue(s"CAST(NULL AS ${dstField.dataType.sql})")
                    else dstField.dataType match {
                        case dstChildFieldType: StructType =>
                            val srcChildFieldType = srcSchema(idx).dataType.asInstanceOf[StructType]
                            val childSQL = buildSQLFieldsRec(srcChildFieldType, dstChildFieldType, depth + 1, prefixedFieldName)
                            namedValue(s"NAMED_STRUCT($childSQL)")
                        case _ =>
                            // Same types, no cast
                            if (srcSchema(idx).dataType == dstField.dataType) {
                                namedValue(s"$prefixedFieldName")
                            } else { // Different types, cast needed
                                namedValue(s"CAST($prefixedFieldName AS ${dstField.dataType.sql})")
                            }
                    }
                }).mkString(", ")
            }

            // Enforce single-usage temporary table name, starting with a letter
            val tableName = "t_" + UUID.randomUUID.toString.replaceAll("[^a-zA-Z0-9]", "")
            // Keep partition number to reset it after SQL transformation
            val partitionNumber = df.rdd.getNumPartitions

            // Convert using generated SQL over registered temporary table
            // Warning: SQL Generated schema needs to be made nullable
            df.createOrReplaceTempView(tableName)
            val sqlQuery = s"SELECT ${buildSQLFieldsRec(df.schema, schema)} FROM $tableName"
            log.debug(s"Converting DataFrame using SQL query:\n$sqlQuery")
            df.sqlContext.sql(sqlQuery).makeNullable().repartitionAs(df)
        }

        def makeNullable(): DataFrame = {
            df.sparkSession.createDataFrame(df.rdd, df.schema.makeNullable())
        }

        def repartitionAs(originalDf: DataFrame): DataFrame = {
            val partitionNumber = originalDf.rdd.getNumPartitions
            if (partitionNumber > 0) {
                df.repartition(partitionNumber)
            } else {
                df
            }
        }

        /**
         * Checks if a DataFrame contains a column with the specified name.
         *
         * This method attempts to access a column in the DataFrame by its name. If the operation
         * is successful, it indicates that the column exists, and the method returns true. Otherwise,
         * it returns false. This is achieved by wrapping the column access attempt in a `Try` block
         * and checking the success of the operation.
         *
         * @param columnName The name of the column to check for existence in the DataFrame.
         * @return Boolean indicating whether the column exists (true) or not (false).
         */
        def hasColumn(columnName: String): Boolean = {
            Try(df(columnName)).isSuccess
        }

        /**
         * Creates a new DataFrame with the same data as the current DataFrame but with empty metadata for all columns.
         *
         * This method utilizes the SparkSession associated with the DataFrame to create a new DataFrame instance.
         * It preserves the data and structure of the original DataFrame but clears all metadata from the schema,
         * which can be useful for operations that do not require metadata or when metadata needs to be reset.
         *
         * @return A new DataFrame with the same data as the original but with all column metadata removed.
         */
        def emptyMetadata: DataFrame = df.sparkSession.createDataFrame(df.rdd, df.schema.emptyMetadata)

        /**
         * Filters and returns a sequence of column names that exist in the DataFrame.
         *
         * This method iterates over a given sequence of column names and checks if each column exists
         * in the DataFrame. It utilizes a `Try` block to attempt accessing each column by name. If the
         * access is successful (indicating the column exists), the column name is included in the output
         * sequence. Otherwise, it is omitted. This can be useful for validating a list of column names
         * against the current DataFrame schema.
         *
         * @param columnNames A sequence of strings representing the column names to check.
         * @return A sequence of strings representing the column names that exist in the DataFrame.
         */
        def findColumnNames(columnNames: Seq[String]): Seq[String] = {
            columnNames.flatMap(c => {
                Try({
                    df(c)
                    c
                }).toOption
            })
        }

        /**
         * Transform DataFrame fields.
         * This applies SQL transformation given per field-name to the source dataframe.
         *
         * @param fieldTransformers The transformers to apply: a map with field-name key and the
         *                          transformation to be applied for the given field as value (in SQL)
         *
         * @return the transformed DataFrame
         */
        def transformFields(fieldTransformers: Map[String, String]): DataFrame = {
            def buildSQLFieldsRec(schema: StructType, depth: Int = 0, prefix: String = ""): String = {
                schema.fields.map(field => {
                    val prefixedFieldName = if (depth == 0) s"${field.name}" else s"$prefix.${field.name}"

                    def namedValue(value: String): String = {
                        if (depth == 0) s"$value AS ${field.name}" // Not in struct, aliases ok
                        else s"'${field.name}', $value"            // In named_struct, quoted-name then value
                    }

                    field.dataType match {
                        case childFieldType: StructType =>
                            val childSQL = buildSQLFieldsRec(childFieldType, depth + 1, prefixedFieldName)
                            namedValue(s"NAMED_STRUCT($childSQL)")
                        case _ =>
                            namedValue(fieldTransformers.getOrElse(prefixedFieldName, prefixedFieldName))
                    }
                }).mkString(", ")
            }

            // Enforce single-usage temporary table name, starting with a letter
            val tableName = "t_" + UUID.randomUUID.toString.replaceAll("[^a-zA-Z0-9]", "")
            // Keep partition number to reset it after SQL transformation
            val partitionNumber = df.rdd.getNumPartitions

            // Convert using generated SQL over registered temporary table
            df.createOrReplaceTempView(tableName)
            val sqlQuery = s"SELECT ${buildSQLFieldsRec(df.schema)} FROM $tableName"
            log.debug(s"Converting DataFrame using SQL query:\n$sqlQuery")
            df.sqlContext.sql(sqlQuery).repartitionAs(df)
        }
    }
}
