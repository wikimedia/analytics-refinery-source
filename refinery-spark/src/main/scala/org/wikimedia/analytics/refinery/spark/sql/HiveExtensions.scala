package org.wikimedia.analytics.refinery.spark.sql

import java.util.UUID

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.types._
import org.wikimedia.analytics.refinery.core.LogHelper


/**
  * Implicit method extensions to Spark's StructType and StructField.
  * This is useful for converting and evolving Hive tables to match
  * Spark DataTypes.
  *
  * Usage:
  *
  *     import org.wikimedia.analytics.refinery.spark.sql.HiveExtensions._
  *
  *     val df: DataFrame = // read original source data
  *     val hiveCreateStatement = df.schema.hiveCreateDDL("mydb.mytable")
  *     hiveContext.sql(hiveCreateStatement)
  *     //...
  *
  *     val table = hiveContext.table("mydb.table")
  *     val df: DataFrame =  // read new data, maybe has new fields
  *     val hiveAlterStatement = table.schema.hiveAlterDDL("mydb.mytable", df.schema)
  *     hiveContext.sql(hiveAlterStatement)
  */
object HiveExtensions extends LogHelper {

    /**
      * Implicit methods extensions for Spark StructField.
      *
      * @param field
      */
    implicit class StructFieldExtensions(field: StructField) {

        /**
          * Returns a copy of this StructField with a name toLowerCase.
          * @return
          */
        def toLowerCase: StructField = {
            Option(field.name).map(n => field.copy(name=n.toLowerCase)).getOrElse(field)
        }

        /**
          * Returns a nullable or non nullable copy of this StructField.
          * @param nullable
          * @return
          */
        def makeNullable(nullable: Boolean = true): StructField =
            field.copy(nullable=nullable)

        /**
          * If possible, widens the field's type.  This currently only
          * widens integers to longs and floats to doubles.
          * @return
          */
        def widen(): StructField = {
            field.dataType match {
                case IntegerType    => field.copy(dataType=LongType)
                case FloatType      => field.copy(dataType=DoubleType)
                case _              => field
            }
        }

        /**
          * Normalizes a copy of this StructField.
          * Here, normalizing means:
          * - conditionally convert field name to lower case
          * - convert hyphens to underscores
          * - widen some types (ints -> longs, floats -> doubles)
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
            val f = field
                // convert hyphens to underscores
                .copy(name=field.name.replace('-', '_'))
                // widen types
                .widen()
                // make nullable
                .makeNullable()
            // conditionally to lower case
            if (lowerCase) f.toLowerCase else f
        }

        /**
          * Builds a Hive DDL string representing the Spark field, useful in
          * CREATE and ALTER DDL statements.
          *
          * @return
          */
        def hiveColumnDDL: String = {
            s"`${field.name}` ${field.typeString}" +
                s"${if (field.nullable) "" else " NOT NULL"}"
        }

        /**
          * Spark's DataType.simpleString mostly works for Hive field types.  But, since
          * we need struct field names to be backtick quoted, we need to call
          * a special method for in the case where this field is a StructType.
          *
          * @return
          */
        def typeString: String = {
            if (field.isStructType) {
                field.dataType.asInstanceOf[StructType].quotedSimpleString
            }
            else {
                field.dataType.simpleString
            }
        }

        /**
          * Returns true if this field.dataType is a StructType, else False.
          * @return
          */
        def isStructType: Boolean = {
            field.dataType match {
                case(StructType(_)) => true
                case _ => false
            }
        }

        /**
          * Find the tightest common DataType of a Seq of StructFields by continuously applying
          * `HiveTypeCoercion.findTightestCommonTypeOfTwo` on these types, or choosing the
          * left most type (original) if we have a value caster defined from candidate -> original type.
          * If not tightest is found, return original type.
          * See: https://github.com/apache/spark/blob/v1.6.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/HiveTypeCoercion.scala#L65-L87
          *
          *
          * @param fields
          * @return
          */
        def chooseCompatibleType(fields: Seq[StructField]): DataType = {
            fields.map(_.dataType).foldLeft[DataType](field.dataType)((original, candidate) => {
                // If the types of the two current fields are the same, just use original.
                if (original == candidate) {
                    return original
                }

                // If Spark can handle type coercion from candidate -> original, then
                // return the type it will use.
                val tightest = TypeCoercion.findTightestCommonType(original, candidate)
                if (tightest.isDefined) {
                    log.debug(s"HiveTypeCoercion is possible, choosing type ${tightest.get} for ($original, $candidate)")
                    tightest.get
                } else {
                    original
                }
            })
        }
    }

    /**
      * Implicit method extensions for Spark StructType.
      *
      * @param struct
      */
    implicit class StructTypeExtensions(struct: StructType) {

        /**
          * Returns a copy of this struct with all fields 'normalized'.
          * If lowerCaseTopLevel is true, then top level field names will be lower cased.
          * This function recurses on sub structs, and normalizes them
          * with lowerCase = false, keeping the cases on sub struct field names.
          *
          * All ints will be converted to longs, and all floats will be
          * converted to doubles.  A field value that may
          * at one time look like an int, may during a later iteration
          * look like a long.  We choose to always use the wider data type.
          *
          * @param lowerCaseTopLevel Default: true
          * @return
          */
        def normalize(lowerCaseTopLevel: Boolean = true): StructType = {
            struct.convert((field, depth) => {
                if (depth == 0) field.normalize(lowerCase=lowerCaseTopLevel)
                else field.normalize(lowerCase=false)
            })
        }

        /**
          * Recursively sets nullablity on every field in this schema and returns the new schema.
          * @param nullable
          * @return
          */
        def makeNullable(nullable: Boolean = true): StructType = {
            struct.convert((field, _) => field.makeNullable(nullable))
        }

        /**
          * Recursively applies fn to each StructField in this schema and
          * replaces the field with the result of fn.
          *
          * @param fn convert a given field to a new field.
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


        // NOTE: Fully recursive normalize and denormalize was implemented at
        // https://gist.github.com/jobar/91c552321efbedba03c8215284726f88#gistcomment-2077149,
        // but we have decided not to include this functionality.  Sub StructType field names
        // are not easily denormalizeable.  It is legal to have field names at different
        // struct levels that all normalize to the same name.  Reverting them back to
        // their original names would require building and keeping a recursive map that matched
        // the original struct hierarchy exactly.
        // These methods were created in order to build Spark schemas that can be used
        // to maintain and update a Hive table, and Hive seems to be indifferent to
        // cases used in its struct<> column type.


        /**
          * Returns a new StructType with otherStruct merged into this.  Any identical duplicate
          * fields shared by both will be reduced to one field.  Non StructType Fields with the
          * same name but different incompatible types will result in an IllegalStateException.
          * StructType fields with the same name will be recursively merged.  All fields will
          * be made nullable.  Comparison of top level field names is done case insensitively,
          * i.e. myField is equivalent to myfield.
          *
          * @param otherStruct  Spark StructType schema
          *
          * @param lowerCaseTopLevel    If false, the returned schema will contain the original
          *                             (non lowercased) top level field names. If true (default)
          *                             top level field names will be lower cased.  All fields
          *                             in the merged schema will be 'normalized', in that they
          *                             will be made nullable and have certain types widened.
          *                             Comparison of fields between schemas will always be done
          *                             case insensitive.
          *
          * @return
          */
        def merge(otherStruct: StructType, lowerCaseTopLevel: Boolean = true): StructType = {
            val combined = StructType(struct ++ otherStruct)
            val combinedNormalized = combined.normalize()

            // Distinct using case insensitive and types.
            // Result will be sorted by n1 fields first, with n2 fields at the end.
            val distinctNames: Seq[String] = combinedNormalized.fieldNames.distinct

            // Store a map of fields by name.  We will iterate through the fields and
            // resolve the cases where there are more than one field (type) for a given name
            // as best we can.
            val fieldsByName: Map[String, Seq[StructField]] = combinedNormalized.distinct.groupBy(_.name)

            val mergedStruct = StructType(
                distinctNames.map { name =>
                    fieldsByName(name) match {
                        // If all field types for this name are structs, then we attempt
                        // to recursively merge them.
                        case fields if fields.forall(_.isStructType) =>
                            val mergedStruct = fields
                                // Map each StructField to its StructType DataType
                                .map(_.dataType.asInstanceOf[StructType])
                                // Recursively merge each StructType together
                                .foldLeft(StructType(Seq.empty))((merged, current) =>
                                    // Don't normalize sub struct schemas.  Spark doesn't
                                    // lowercase Hive struct<> field names, and those
                                    // seem to be nullable by default anyway.
                                    // If we did normalize, then we'd have to recursively
                                    // un-normalize if the original caller passed normalize=false.
                                    merged.merge(current, lowerCaseTopLevel = false)
                                )

                            // Convert the StructType back into a StructField with this field name.
                            StructField(name, mergedStruct, nullable = true)

                        case fields =>
                            // Find the tightest common type for Hive. If there is there is only
                            // one field for this name, this will return that field's type.
                            // If there are multiple fields, this will try to find a common
                            // type that can be cast.  E.g. if we are given an LongType
                            // and a DoubleType, this will return DoubleType.
                            val commonDataType = fields.head.chooseCompatibleType(fields.tail)

                            // Else: let's get weird.  Since we don't support type changes,
                            // We choose to keep the first field type we have.  This should
                            // be the field belonging to this struct schema (which is probably
                            // a Hive table schema).  Because we know that there is a compatible
                            // DataType, then a DataFrame that uses the non original
                            // field should still be convertable to to the original one later,
                            // either by Spark-Hive, or by one of our custom caster functions.
                            if (commonDataType != fields.head.dataType) {
                                log.warn(
                                    s"$name has repeat types which are resolvable.  " +
                                    s"Choosing ${fields.head.dataType} for schema merge.  " +
                                    s"Other fields were:\n  ${fields.tail.mkString("  \n")}\n" +
                                    "NOTE: Data loaded into the merged schema might " +
                                    "lose precision or nullify fields for a " +
                                    "conflicting record, depending on the value of the " +
                                    "DataFrameReader mode option used and available casters. See also: " +
                                    "https://spark.apache.org/docs/2.0.2/api/java/org/apache/spark/sql/DataFrameReader.html#json(java.lang.String...)"
                                )
                            }
                            fields.head
                    }
                }
            )

            // If we want the normalized (lower cased) field names, return mergedStruct now.
            if (lowerCaseTopLevel) {
                mergedStruct
            }
            // Else we want the mergedStruct returned with the original
            // top level non normalized field names.
            else {
                // Build a map from top level lowercased names to non normal names.
                val lookup: Map[String, String] = combined.fieldNames
                    .distinct.map(n => n.toLowerCase -> n).toMap
                // Map the mergedStruct, renaming any fields in lookup.
                StructType(mergedStruct.map(f => f.copy(name=lookup.getOrElse(f.name, f.name))))
            }
        }


        /**
          * Like simpleString, but backtick quotes field names inside of the struct<>.
          * e.g. struct<`fieldName`:string,`database`:string>
          * This works better in Hive, as reserved keywords need to be quoted.
          * If this struct contains sub structs, those will be recursively quoted too.
          *
          * @return
          */
        def quotedSimpleString: String = {
            val fieldTypes = struct.fields.map { field =>
                if (field.isStructType) {
                    s"`${field.name}`:${field.dataType.asInstanceOf[StructType].quotedSimpleString}"
                }
                else {
                    s"`${field.name}`:${field.dataType.simpleString}"
                }
            }
            s"struct<${fieldTypes.mkString(",")}>"
        }

        /**
          * Returns String representing Hive column DDL, for use in Hive DDL statements.
          * This only returns the portion of the DDL statement representing each column.
          * E.g.
          * `fieldname1` string,
          * `fieldname2` bigint
          * ..
          *
          * @param sep column DDL separator, default ",\n"
          *
          * @return
          */
        def hiveColumnsDDL(sep: String = ",\n"): String =
            struct.map(_.hiveColumnDDL).mkString(sep)


        /**
          * Builds a Hive CREATE statement DDL string from this StructType schema.
          * Since Hive is case insensitive, the top level field names will lowercased.
          * To ease integration with missing fields in data, all fields are made nullable.
          *
          * @param tableName        Fully qualified Hive database.table name.
          * @param locationPath     HDFS path to external Hive table.
          * @param partitionNames   List of partition column names.
          * @param storageFormat    Hive storage format string to use in `STORED AS ` clause.
          *                         See: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-StorageFormatsStorageFormatsRowFormat,StorageFormat,andSerDe
          *
          * @return CREATE statement DDL string
          */
        def hiveCreateDDL(
            tableName: String,
            locationPath: String = "",
            partitionNames: Seq[String] = Seq.empty,
            storageFormat: String = "PARQUET"
        ): String = {
            val schemaNormalized = struct.normalize()
            val partitionNamesNormalized = partitionNames.map(_.toLowerCase)

            // Validate that all partitions are in the schema.
            if (partitionNamesNormalized.diff(schemaNormalized.fieldNames).nonEmpty) {
                throw new IllegalStateException(
                    s"""At least one partition field is not the Spark StructType schema.
                       |partitions: [${partitionNamesNormalized.mkString(",")}]""".stripMargin
                )
            }

            val externalClause = if (locationPath.nonEmpty) " EXTERNAL" else ""

            val columnsClause = StructType(schemaNormalized
                .filterNot(f => partitionNamesNormalized.contains(f.name))
            ).hiveColumnsDDL()


            val partitionClause = {
                if (partitionNamesNormalized.isEmpty) "-- No partition provided"
                else {
                    s"""PARTITIONED BY (
                       |${StructType(partitionNamesNormalized.map(
                            p => schemaNormalized(schemaNormalized.fieldIndex(p))
                        )).hiveColumnsDDL()}
                       |)""".stripMargin
                }
            }

            val locationClause = if (locationPath.nonEmpty) s"\nLOCATION '$locationPath'" else ""

            s"""CREATE$externalClause TABLE $tableName (
               |$columnsClause
               |)
               |$partitionClause
               |STORED AS $storageFormat$locationClause""".stripMargin
        }


        /**
          * Merges otherSchema into this struct StructType, and builds Hive
          * ALTER DDL statements to add any new fields to or change struct definitions
          * of an existing Hive table.  Each DDL statement returned should be executed in order
          * to alter the target Hive table to match the merged schemas.
          *
          * Type changes for non-struct fields are not supported and will result in an
          * IllegalStateException.
          *
          * Field names will be lower cased, and all fields are made nullable.
          *
          * @param tableName    Hive table name
          * @param otherSchema  Spark schema
          *
          * @return             Iterable of ALTER statement DDL strings
          */
        def hiveAlterDDL(
            tableName: String,
            otherSchema: StructType
        ): Iterable[String] = {
            val schemaNormalized = struct.normalize()
            val otherSchemaNormalized = otherSchema.normalize()

            // Merge the base schema with otherSchema to ensure there are no non struct type changes.
            // (merge() will throw an exception if it encounters any)
            val mergedSchemaNormalized = schemaNormalized.merge(otherSchemaNormalized)

            // diffSchema contains fields that differ in name or type from the original schema.
            val diffSchema = mergedSchemaNormalized.diff(schemaNormalized)


            // If there are no new fields at all, then return empty Seq now.
            if (diffSchema.isEmpty) {
                Seq.empty[String]
            }
            else {
                // Group the schema changes into ones for which we can
                // just ADD COLUMNS in a single statement, and those
                // for which we need individual CHANGE COLUMN statements.
                // We must CHANGE COLUMN any StructType field that has changed.
                val tableModifications = diffSchema.groupBy(f =>
                    // If this field is in diffSchema and in the original schema,
                    // we know that it must be a StructType.  merge() wouldn't let
                    // us have have fields with the same name and different types unless
                    // it is a StructType.
                    if (schemaNormalized.fieldNames.contains(f.name)) "change"
                    else "add"
                )
                // To be 100% sure we keep ordering, sort the grouped fields by name.
                .map { case (group, fields) => (group, fields.sortBy(f => f.name)) }

                // Generate the ADD COLUMNS statement to add all new COLUMNS
                val addStatements: Option[String] = if (tableModifications.contains("add")) {
                    Option(s"""ALTER TABLE $tableName
                       |ADD COLUMNS (
                       |${StructType(tableModifications("add")).hiveColumnsDDL()}
                       |)""".stripMargin
                    )
                }
                else
                    None

                // Generate each CHANGE COLUMNS statement needed to
                // update the struct<> definition of a struct COLUMN.
                val changeStatements: Seq[String] = tableModifications
                    .getOrElse("change", Seq.empty[StructField])
                    .map { f =>
                        s"""ALTER TABLE $tableName
                            |CHANGE COLUMN `${f.name}` ${f.hiveColumnDDL}""".stripMargin
                    }

                // Return a Seq of all statements to run to update the Hive table
                // to match mergedSchemaNormalized.
                addStatements ++ changeStatements
            }
        }


        /**
          * Given a subsetSchema, recursively find any fields in it that are not in this
          * struct superSchema, or that have incompatible DataTypes.
          *
          * @param subsetSchema     Subset schema to check.
          *
          * @param fieldNamePrefix  Used when recursing into sub structs to report any offending
          *                         field names as fully qualified, e.g. event.newField.
          *                         Should be blank when first called.
          *
          * @return Seq[(StructField, String)] bad field from subsetSchema and reason why the field was incompatible.
          */
        def findIncompatibleFields(
            subsetSchema: StructType,
            fieldNamePrefix: String = ""
        ): Seq[(StructField, String)] = {

            val superSchema = struct
            // Recursively iterate through all the fields in smallerSchema and
            // compare them against superSchema.
            subsetSchema.fields.foldLeft(Seq.empty[(StructField, String)])((incompatibleFields, field) => {
                val name = field.name
                val fullName = fieldNamePrefix + name

                // If field from smallerSchema is present in superSchema, we need to check it.
                if (superSchema.fieldNames.contains(name)) {
                    val superSchemaField = superSchema(name)

                    val fieldType = field.dataType
                    val superSchemaFieldType = superSchemaField.dataType
                    // If the nullable-ness of the fields are different
                    if (field.nullable != superSchemaField.nullable) {
                        incompatibleFields :+ (
                            field,
                            s"""`$fullName` nullable=${superSchemaField.nullable} in super schema,
                                |but is nullable=${field.nullable} in subset schema.""".stripMargin
                        )
                    }

                    // If both are a StructType, then we recurse into the struct to check sub fields.
                    else if (fieldType.isInstanceOf[StructType]) {
                        incompatibleFields ++ superSchemaFieldType.asInstanceOf[StructType].findIncompatibleFields(
                            // search the smallerSchema's struct field
                            fieldType.asInstanceOf[StructType],
                            // Prefix any found bad fields with the current field name.
                            s"$fullName."
                        )
                    }

                    // Else we didn't find anything wrong with this field, so don't
                    // add anything new to incompatibleFields.
                    else {
                        incompatibleFields
                    }
                }
                // Else this is a field in the smallerSchema, but not in the bigger one.
                else {
                    incompatibleFields :+ (
                        field,
                        s"`$fullName` is missing in bigger schema, but is present in subset schema."
                    )
                }
            })
        }

    }

    implicit class DataFrameExtensions(df: DataFrame) {
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
                    if (idx == -1) namedValue(s"CAST(NULL AS ${dstField.dataType.simpleString})")
                    else dstField.dataType match {
                        case dstChildFieldType: StructType =>
                            val srcChildFieldType = srcSchema(idx).dataType.asInstanceOf[StructType]
                            val childSQL = buildSQLFieldsRec(srcChildFieldType, dstChildFieldType, depth + 1, prefixedFieldName)
                            namedValue(s"NAMED_STRUCT($childSQL)")
                        case _ => {
                            // Same types, no cast
                            if (srcSchema(idx).dataType == dstField.dataType) {
                                namedValue(prefixedFieldName)
                            } else { // Different types, cast needed
                                namedValue(s"CAST($prefixedFieldName AS ${dstField.dataType.simpleString})")
                            }
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
            log.debug(s"Converting DataFrame using SQL query:\n${sqlQuery}")
            df.sqlContext.sql(sqlQuery).makeNullable().repartition(partitionNumber)
        }

        def makeNullable(): DataFrame = {
            df.sparkSession.createDataFrame(df.rdd, df.schema.makeNullable())
        }

        def normalize(): DataFrame = {
            df.sparkSession.createDataFrame(df.rdd, df.schema.normalize())
        }
    }
}
