package org.wikimedia.analytics.refinery.spark.sql

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.types._
import org.wikimedia.analytics.refinery.tools.LogHelper

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
    implicit class StructFieldExtensions(field: StructField) extends SparkSqlExtensions.StructFieldExtensions(field) {

        /**
          * Returns a copy of this StructField with a name toLowerCase.
          * @return
          */
        def toLowerCase: StructField = {
            Option(field.name).map(n => field.copy(name=n.toLowerCase)).getOrElse(field)
        }

        /**
          * Builds a Hive DDL string representing the Spark field, useful in
          * CREATE and ALTER DDL statements.
          *
          * @return
          */
        def hiveColumnDDL: String = {
            s"`${field.name}` ${field.dataType.sql}" +
                s"${if (field.nullable) "" else " NOT NULL"}"
        }

        /**
          * Returns true if this field.dataType is a StructType, else False.
          * @return
          */
        def isStructType: Boolean = {
            field.dataType match {
                case StructType(_) => true
                case _ => false
            }
        }

        /**
          * Returns true if this field.dataType is an ArrayType, else False.
          * @return
          */
        def isArrayType: Boolean = {
            field.dataType match {
                case ArrayType(_, _) => true
                case _ => false
            }
        }

        /**
          * Returns true if this field.dataType is a MapType, else False.
          * @return
          */
        def isMapType: Boolean = {
            field.dataType match {
                case MapType(_, _, _) => true
                case _ => false
            }
        }

        /**
          * Find the tightest common DataType of a Seq of StructFields by continuously applying
          * `HiveTypeCoercion.findTightestCommonType` on these types, or choosing the
          * left most type (original) if we have a value caster defined from candidate -> original type.
          * If not tightest is found, return original type.
          * See: https://github.com/apache/spark/blob/v1.6.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/HiveTypeCoercion.scala#L65-L87
          *
          * @param fields
          * @return
          */
        def chooseCompatiblePrimitiveType(fields: Seq[StructField]): DataType = {
            fields.foldLeft[StructField](field)((original, candidate) => {
                val originalType = original.dataType
                val candidateType = candidate.dataType

                // If the types of the two current fields are the same, choose original.
                if (originalType == candidateType) {
                    original
                }
                else {
                    // Check if Spark has common type for later type coercion.
                    // If it does, use it, else choose the originalType.
                    val tightestType = TypeCoercion.findTightestCommonType(
                        originalType, candidateType
                    )

                    val chosenType = if (tightestType.isDefined) {
                        log.debug(
                            s"Type coercion is possible, choosing type ${tightestType.get} " +
                                s"for ($original, $candidate)"
                            )
                        tightestType.get
                    }
                    else {
                        log.warn(
                            s"Type coercion is not possible, choosing original type " +
                                s"$originalType for ($original, $candidate)"
                            )
                        originalType
                    }

                    // If any field won't later be cast-able to our chosen type, log a warning now.
                    // If you later do something like attempt to read a JSON dataset
                    // with this incompatible field schema, make sure you read it with
                    // .option("mode", "FAILFAST") to make spark fail if its implicit cast fails.
                    // Otherwise you might end up in a weird state, like where all fields of the
                    // offending record are null.
                    if (!Cast.canCast(candidateType, chosenType)) {
                        log.warn(
                            s"Spark cannot cast from candidate field ${candidate} to chosen type " +
                                s"${chosenType}. If you later attempt to read candidate data using " +
                                "the chosen type, you will likely encounter errors."
                            )
                    }

                    // Collect a dummy StructField with the chosen type (but original's name)
                    StructField(original.name, chosenType)
                }
            }).dataType
        }
    }

    /**
      * Implicit method extensions for Spark StructType.
      *
      * @param struct
      */
    implicit class StructTypeExtensions(struct: StructType) extends SparkSqlExtensions.StructTypeExtensions(struct) {

        /**
          * Like StructType.find, but compares by name instead of StructField
          * @param fieldNames Find fields in this struct that have these field names.
          * @param caseInsensitive If true, field name comparison will be done case insensitive
          * @return
          */
        def find(fieldNames: Seq[String], caseInsensitive: Boolean = false): Seq[StructField] = {
            val targetNames = caseInsensitive match {
                case true => fieldNames.map(_.toLowerCase)
                case false => fieldNames
            }

            struct.filter(field => {
                val fieldName = caseInsensitive match {
                    case true => field.name.toLowerCase
                    case false => field.name
                }
                targetNames.contains(fieldName)
            })
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
          * Returns a new StructType with otherStruct merged into this. Any identical duplicate
          * fields shared by both will be reduced to one field. Non StructType Fields with the
          * same name but different incompatible types will result in an IllegalStateException.
          * StructType fields with the same name will be recursively merged.  All fields will
          * be made nullable. Comparison of top level field names is done case insensitively,
          * i.e. myField is equivalent to myfield.
          *
          * `merge` is case sensitive. So if the two schemas have columns with same name but
          * different casing, the merged schema will include both columns.
          *
          * @param otherStruct Spark StructType schema
          * @return Merged schema
          */
        def merge(otherStruct: StructType): StructType = {
            val combined = StructType(struct ++ otherStruct)
            mergeByFieldNameLowerCase(lowerCaseTopLevel = false, combined)
        }

        /**
         * Returns a new StructType with otherStruct merged into this. Any identical duplicate
         * fields shared by both will be reduced to one field. Non StructType Fields with the
         * same name but different incompatible types will result in an IllegalStateException.
         * StructType fields with the same name will be recursively merged.  All fields will
         * be made nullable. Comparison of top level field names is done case insensitively,
         * i.e. myField is equivalent to myfield.
         * @param lowerCaseTopLevel If false, the returned schema will contain the original
         *                          (non lowercased) top level field names. If true (default)
         *                          top level field names will be lower cased.  All fields
         *                          in the merged schema will be 'normalized', in that they
         *                          will be made nullable and have certain types widened.
         *                          Comparison of fields between schemas will always be done
         *                          case insensitive.
         * @param combined StructType schema with otherStruct merged into this.
         * @return
         */
        private def mergeByFieldNameLowerCase(lowerCaseTopLevel: Boolean, combined: StructType) = {
            // Distinct using case insensitive and types.
            // Result will be sorted by n1 fields first, with n2 fields at the end.
            val distinctNames: Seq[String] = if (lowerCaseTopLevel) combined.fieldNames.map(_.toLowerCase).distinct
            else combined.fieldNames.distinct

            // Store a map of fields by name.  We will iterate through the fields and
            // resolve the cases where there are more than one field (type) for a given name
            // as best we can.
            val fieldsByName: Map[String, Seq[StructField]] = if (lowerCaseTopLevel) {
                combined.distinct.groupBy(_.name.toLowerCase)
            } else combined.distinct.groupBy(_.name)


            val mergedStruct = StructType(
                distinctNames.map { name =>
                    fieldsByName(name) match {

                        // If all field types for this name are structs, then recursively merge them.
                        case fields if fields.forall(_.isStructType) => mergeStructTypeFields(name, fields)

                        // If all field types for this name are arrays, then recursively merge the array elementType.
                        case fields if fields.forall(_.isArrayType) => mergeArrayTypeFields(name, fields)

                        // If all field types for this name are maps, then we attempt
                        // to recursively merge the map keyType and recursively merge the valueType.
                        case fields if fields.forall(_.isMapType) => mergeMapTypeFields(name, fields)

                        case fields =>
                            // Find the tightest common type for Hive. If there is there is only
                            // one field for this name, this will return that field's type.
                            // If there are multiple fields, this will try to find a common
                            // type that can be cast.  E.g. if we are given an LongType
                            // and a DoubleType, this will return DoubleType.
                            val commonDataType = fields.head.chooseCompatiblePrimitiveType(fields.tail)

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
            mergedStruct
        }

        /**
         * Returns a new StructType with otherStruct merged into this. Any identical duplicate
         * fields shared by both will be reduced to one field.  Non StructType Fields with the
         * same name but different incompatible types will result in an IllegalStateException.
         * StructType fields with the same name will be recursively merged. All fields will
         * be made nullable. Comparison of top level field names is done case insensitively,
         * i.e. myField is equivalent to myfield.
         *
         * This is a legacy method that do normalize and widen the schema.
         *
         * @param otherStruct       Spark StructType schema
         * @param lowerCaseTopLevel If false, the returned schema will contain the original
         *                          (non lowercased) top level field names. If true (default)
         *                          top level field names will be lower cased.  All fields
         *                          in the merged schema will be 'normalized', in that they
         *                          will be made nullable and have certain types widened.
         *                          Comparison of fields between schemas will always be done
         *                          case insensitive.
         * @return
         */
        def normalizeMerge(otherStruct: StructType, lowerCaseTopLevel: Boolean = true): StructType = {
            val combined = StructType(struct ++ otherStruct)
            val combinedNormalized = combined.normalizeAndWiden()

            val mergedStruct: StructType = mergeByFieldNameLowerCase(lowerCaseTopLevel, combinedNormalized)

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
          * Given a Seq of StructFields all with StructType dataTypes,
          * recursively merge them together.
          * @param name
          * @param fields
          * @return
          */
        private def mergeStructTypeFields(name: String, fields: Seq[StructField]): StructField = {
            val mergedStruct = fields
                // Map each StructField to its StructType DataType
                .map(_.dataType.asInstanceOf[StructType])
                // Recursively merge each StructType together
                // Don't normalize sub struct schemas.  Spark doesn't
                // lowercase Hive struct<> field names, and those
                // seem to be nullable by default anyway.
                // If we did normalize, then we'd have to recursively
                // un-normalize if the original caller passed normalize=false.
                .foldLeft(StructType(Seq.empty))((merged, current) => {
                    merged.merge(current)
                })
            // Convert the StructType back into a StructField with this field name.
            StructField(name, mergedStruct, nullable = true)
        }


        /**
          * Given a Seq of StructFields all with ArrayType dataTypes,
          * recursively merge their elementTypes together.
          * @param name
          * @param fields
          * @return
          */
        private def mergeArrayTypeFields(name: String, fields: Seq[StructField]): StructField = {
            val mergedFakeElementStruct = fields
                // Map each StructField to a wrapper StructType of the Array's elementType.
                // This allows us to pass the elementType recursively to merge()
                .map(field => encloseTypeInStruct(name, field.dataType.asInstanceOf[ArrayType].elementType))
                // Recursively merge the fake structs each containing one field of elementType
                .foldLeft(StructType(Seq.empty))((merged, current) => {
                    merged.merge(current)
                })

            // Convert the merged elementType back into a StructField ArrayType of elementTypes with this name.
            // We know we are working with a single element StructType, who's first element's
            // type is our merged array elementType.
            StructField(
                name,
                ArrayType(mergedFakeElementStruct.head.dataType, containsNull = true),
                nullable = true
            )
        }

        /**
          * Given a Seq of StructFields all with MapType dataTypes,
          * recursively merge their keyTypes and valueTypes together.
          * @param name
          * @param fields
          * @return
          */
        private def mergeMapTypeFields(name: String, fields: Seq[StructField]): StructField = {
            val (mergedFakeKeyStruct, mergedFakeValueStruct) = fields
                // Map each StructField to its ArrayType elementType
                .map(_.dataType.asInstanceOf[MapType])
                .map(mapType => (encloseTypeInStruct(name, mapType.keyType), encloseTypeInStruct(name, mapType.valueType)))
                // Recursively merge the fake key and value structs each containing one field
                // of keyType or valueType.
                // foldLeft over tuples of merged (keyStruct, valueStruct).
                .foldLeft((StructType(Seq.empty), StructType(Seq.empty)))((merged, current) => {
                    // Extract the collected key and value structs from the foldLeft params.
                    val (mergedKeyStruct, mergedValueStruct) = merged
                    val (currentKeyStruct, currentValueStruct) = current
                    (
                        // NOTE: complex map key types are not supported, so the merge() call
                        // here on the key types should cause the key types to use the primitive case.
                        mergedKeyStruct.merge(currentKeyStruct),
                        mergedValueStruct.merge(currentValueStruct)
                    )
                })

            // Convert the fake key and value structs back into a single StructField of MapType with this name.
            // We know we are working with a single element StructType in each fake key and value struct,
            // who's first element's type is our merged key or value type.
            StructField(
                name,
                MapType(mergedFakeKeyStruct.head.dataType, mergedFakeValueStruct.head.dataType, valueContainsNull = true),
                nullable = true
            )
        }

        /**
          * Creates a new StructType with a single StructField with name and of type dataType.
          * @param name
          * @param dataType
          * @return
          */
        private def encloseTypeInStruct(name: String, dataType: DataType): StructType = {
            StructType(Seq(StructField(name, dataType, nullable = true)))
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
          *
          * This method makes sure that all partition columns are in the schema and are lowercased.
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
            val partitionNamesNormalized = partitionNames.map(_.toLowerCase)

            // Validate that all partitions are in the schema.
            if (partitionNamesNormalized.diff(struct.fieldNames).nonEmpty) {
                throw new IllegalStateException(
                    s"""At least one partition field is not in the Spark schema.
                       |partitions: [${partitionNamesNormalized.mkString(",")}]""".stripMargin
                )
            }

            val externalClause = if (locationPath.nonEmpty) " EXTERNAL" else ""

            val columnsClause = StructType(struct
                .filterNot(f => partitionNamesNormalized.contains(f.name))
            ).hiveColumnsDDL()


            val partitionClause = {
                if (partitionNamesNormalized.isEmpty) "-- No partition provided"
                else {
                    s"""PARTITIONED BY (
                       |${StructType(partitionNamesNormalized.map(
                        p => struct(struct.fieldIndex(p))
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
          * of an existing Hive table. Each DDL statement returned should be executed in order
          * to alter the target Hive table to match the merged schemas.
          *
          * Some type changes are not supported by Hive and will result in an IllegalStateException.
          * See more details here:
          * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-ChangeColumnName/Type/Position/Comment
          *
          * Notice: The updates for the schema don't include schema comments.
          *
          * @param tableName    Hive table name
          * @param otherSchema  Spark schema
          * @return             Iterable of ALTER statement DDL strings
          */
        def hiveAlterDDL(
            tableName: String,
            otherSchema: StructType
        ): Iterable[String] = {
            // Merge the base schema with otherSchema to ensure there are no non struct type changes.
            // (merge() will throw an exception if it encounters any)
            val mergedSchema = struct.merge(otherSchema)
            // diffSchema contains fields that differ in name or type from the original schema.
            val diffSchema = mergedSchema.diff(struct)


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
                    if (struct.fieldNames.contains(f.name)) "change"
                    else "add"
                )
                    // To be 100% sure we keep ordering, sort the grouped fields by name.
                    .map { case (group, fields) => (group, fields.sortBy(f => f.name)) }

                // Generate the ADD COLUMNS statement to add all new COLUMNS
                val addStatements: Option[String] = if (tableModifications.contains("add")) {
                    Option(
                        s"""|ALTER TABLE $tableName
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
                        s"""|ALTER TABLE $tableName
                            |CHANGE COLUMN `${f.name}` ${f.hiveColumnDDL}""".stripMargin
                    }

                // Return a Seq of all statements to run to update the Hive table
                // to match mergedSchema.
                addStatements ++ changeStatements
            }
        }

        private val TimePartitions: Set[String] = Set("year", "month", "day", "hour")


        /**
         * Adds partition columns to the schema.
         *
         * This method extends the current schema by adding new columns specified in `timePartitionColumnNames`.
         *
         * @param timePartitionColumnNames A sequence of strings representing the names of the partition columns
         *                                 to be added. Defaults to the predefined set of time partitions.
         * @return A new `StructType` representing the schema with the added partition columns.
         */
        def addPartitionColumns(
            timePartitionColumnNames: Seq[String] = TimePartitions.toSeq
        ): StructType = {
            val partitionColumnFields = timePartitionColumnNames.map { partitionColumnName =>
                val fieldType = if (TimePartitions.contains(partitionColumnName)) LongType else StringType
                StructField(partitionColumnName, fieldType)
            }
            StructType(struct ++ partitionColumnFields)
        }
    }

    implicit class DataFrameExtensions(df: DataFrame) extends SparkSqlExtensions.DataFrameExtensions(df)

    /**
      * Extensions to DataFrameReader
      * @param dfReader
      */
    implicit class DataFrameReaderExtensions(dfReader: DataFrameReader) {
        /**
          * Reads in Hadoop SequenceFiles where the value of each
          * record is a JSON string.
          * @param path
          * @param spark
          * @return
          */
        def sequenceFileJson(path: String, spark: SparkSession): DataFrame = {
            dfReader.json(spark.createDataset[String](
                spark.sparkContext.sequenceFile[Long, String](path).map(t => t._2)
            )(Encoders.STRING))
        }
    }
}
