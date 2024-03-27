package org.wikimedia.analytics.refinery.spark.sql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}
import org.wikimedia.analytics.refinery.spark.sql.SparkSqlExtensions.quoteFieldPath
import org.wikimedia.analytics.refinery.tools.LogHelper

import scala.language.higherKinds

object IcebergExtensions extends LogHelper {

    /**
     * Returns the column name from an Iceberg partition definition.
     * It is expected that parenthesis are only used after partition function keyword,
     * and that only one partition functions is used per partition-field.
     *
     * Note: The function doesn't check for partition validity - it returns the partition as
     *       originally provided if it doesn't manage to extract a column from a function.
     *
     * @param partitionDefinition The partition definition to extract the column from
     * @return The column used in the partition
     */
    def fieldNameFromPartitionDefinition(partitionDefinition: String): String = {
        val parStart = partitionDefinition.indexOf('(')
        if (parStart > 0) {
            val parEnd = partitionDefinition.indexOf(')', parStart)
            val commaIdx = partitionDefinition.indexOf(',', parStart)
            (commaIdx, parEnd) match {
                case (-1, e) => partitionDefinition.slice(parStart + 1, e).trim
                case (c, e) if c < e => partitionDefinition.slice(c + 1, e).trim
                case _ => partitionDefinition
            }
        } else { // No parenthesis - the partitionDefinition is the columnName
            partitionDefinition
        }
    }

    implicit class IcebergStructFieldExtensions(field: StructField) extends SparkSqlExtensions.StructFieldExtensions(field) {

        /**
         * Generates the DDL statement for an Iceberg table column.
         * The statement includes the column name, data type, and, if applicable, a NOT NULL constraint and a comment.
         *
         * @param fieldPath Optional custom path for the field. If not provided, the field's name is used.
         * @return A string representing the DDL statement for the column.
         */
        def icebergDDL(fieldPath: Option[String] = None): String = {
            s"${quoteFieldPath(fieldPath.getOrElse(field.name))} ${field.dataType.sql}" +
                s"${if (field.nullable) "" else " NOT NULL"}" +
                field.getCommentAsDDL
        }
    }

    case class FieldAlteration(
        field: StructField,
        fieldPath: String,
        alterationType: String,
        alterationValue: String = ""
    ) {

        private lazy val quotedFieldPath: String = quoteFieldPath(fieldPath)

        lazy val icebergDDL: String = field.icebergDDL(fieldPath = Some(fieldPath))

        def dropDDL(tableName: String): String = s"ALTER TABLE $tableName DROP COLUMN $quotedFieldPath ;"

        def alterTypeDDL(tableName: String): String =
            s"ALTER TABLE $tableName ALTER COLUMN $quotedFieldPath TYPE $alterationValue ;"

        def alterCommentDDL(tableName: String): String = {
            s"ALTER TABLE $tableName ALTER COLUMN $quotedFieldPath COMMENT '$alterationValue' ;"
        }
    }

    implicit class IcebergStructTypeExtensions(struct: StructType) extends SparkSqlExtensions.StructTypeExtensions(struct) {

        /**
         *
         * Builds an Iceberg CREATE statement DDL string from this StructType schema.
         * Following Hive case insensitive manners, the top level field names will be lowercase.
         * To ease integration with missing fields in data, all fields are made nullable.
         *
         * @param schema          StructType schema to be used for the table creation.
         * @param tableName       Fully qualified catalog.database.table name.
         * @param locationPath    HDFS path to external Iceberg table.
         * @param partitionBy     Partitioning statement to be used in the CREATE TABLE statement.
         *                        You can provide a valid sql statement part to partition the table.
         *                        If no field is found, the table won't be partitioned.
         *                        Iceberg allows partitioning by columns or functions.
         * @param tableProperties Values to be passed as table properties.
         * @return                CREATE statement DDL string
         */
        def icebergCreateDDL(
            tableName: String,
            locationPath: Option[String],
            partitionBy: Seq[String] = Seq.empty,
            tableProperties: Map[String, String] = Map.empty
        ): String = {
            // Validate that all partitions names are in the schema.
            if (partitionBy.nonEmpty
                && !partitionBy.forall(p =>
                    struct.fieldExists(fieldNameFromPartitionDefinition(p)))) {
                throw new RuntimeException(
                    s"""Error generating Iceberg CREATE statement for table $tableName.
                       |At least one partition fieldPath from partition definitions:
                       |[${partitionBy.mkString(",")}]
                       |is not the Spark schema ${struct.treeString}.""".stripMargin
                )
            }

            // We are using hiveColumnsDDL in place of the Spark provided `toDDL` method.
            // As of 2024-04, they produce the same output, except that with hiveColumnsDDL:
            // * we can customize the separators for readability,
            // * the "NOT NULL" keyword is added (when nullable is false),
            // * and comment are added.
            val columnsClause = struct.icebergColumnsDDL()

            val partitionClause = if (partitionBy.isEmpty) "-- No partition provided" else {
                    s"PARTITIONED BY (${partitionBy.mkString(",\n")})"
                }

            val tablePropertiesClause = if (tableProperties.isEmpty) "-- No table properties provided" else {
                s"TBLPROPERTIES(${tableProperties.map(t => s"${t._1} = '${t._2}'").mkString(", ")})"
            }

            val locationClause = if (locationPath.getOrElse("").nonEmpty) s"LOCATION '${locationPath.get}'" else ""

            s"""CREATE TABLE $tableName (
               |$columnsClause
               |)
               |USING iceberg
               |$partitionClause
               |$tablePropertiesClause
               |$locationClause
               |""".stripMargin
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
        def icebergColumnsDDL(sep: String = ",\n"): String =
            struct.map(_.icebergDDL()).mkString(sep)

        /**
         * Builds Iceberg ALTER statements DDL strings to match the new schema.
         * The ALTER statement will contain all the necessary commands to update the table schema
         * to match the provided schema.
         *
         * The supported alterations are here:
         * https://iceberg.apache.org/docs/latest/spark-ddl/
         *
         * @param tableName Fully qualified catalog.database.table name.
         * @param newSchema The new schema to be used for the table alteration.
         * @return          List of ALTER statements DDL strings
         */
        def icebergAlterSchemaDDLs(tableName: String, newSchema: StructType): Seq[String] = {
            struct.icebergCollectFieldAlterations(newSchema, None)
                .groupBy(_.alterationType)
                .map({ case (alterationType, alterations) =>
                    log.debug(s"$alterationType: ${alterations.map(_.fieldPath)}")
                    alterationType match {
                        case "drop" => alterations.map(_.dropDDL(tableName))
                        case "add" =>
                            Seq(s"""ALTER TABLE $tableName
                                   |ADD COLUMNS (
                                   |    ${alterations.map(_.icebergDDL).mkString(",\n    ")}
                                   |);""".stripMargin)
                        case "alter_type" => alterations.map(_.alterTypeDDL(tableName))
                        case "alter_comment" => alterations.map(_.alterCommentDDL(tableName))
                    }
                }).toSeq.flatten
        }

        /**
         * Collects necessary field alterations between the current and new schema for an Iceberg table.
         * The method supports nested fields by recursively collecting alterations for subfields.
         *
         * @param newSchema The new schema to compare against the current schema.
         * @param parentFieldPath An optional string representing the path to the parent field for nested structures.
         *                        This is used to correctly identify the full path of nested fields in the alteration list.
         * @return A sequence of `FieldAlteration` instances representing the necessary alterations to the schema.
         */
        def icebergCollectFieldAlterations(
            newSchema: StructType,
            parentFieldPath: Option[String]
        ): Seq[FieldAlteration] = {

            val dropAlterations: Seq[FieldAlteration] = struct.fields
                .filterNot(field => newSchema.fieldExistsIgnoreCase(field.name))
                .map(field => FieldAlteration(
                    field = field,
                    fieldPath = field.fieldPath(parentFieldPath),
                    alterationType = "drop"
                ))

            newSchema.flatMap { newField: StructField =>
                val field = struct.findFieldIgnoreCase(newField.name)
                val fieldPath = newField.fieldPath(parentFieldPath)
                if (field.isEmpty) { // Field to add
                    Seq(FieldAlteration(
                        field = newField,
                        fieldPath = fieldPath,
                        alterationType = "add"
                    ))
                } else { // check for alterations
                    val typeAlteration: Seq[FieldAlteration] = {

                        if (!field.get.isTypeWithSubfields && field.get.dataType != newField.dataType) {
                            Seq(FieldAlteration(
                                field = field.get,
                                fieldPath = fieldPath,
                                alterationType = "alter_type",
                                alterationValue = newField.dataType.sql
                            ))
                        } else Seq.empty[FieldAlteration]
                    }
                    val commentAlteration: Seq[FieldAlteration] = if (!field.get.sameCommentAs(newField)) {
                        Seq(FieldAlteration(
                            field = field.get,
                            fieldPath = fieldPath,
                            alterationType = "alter_comment",
                            alterationValue = newField.getCommentOrEmptyString
                        ))
                    } else Seq.empty[FieldAlteration]
                    val subFieldsAlterations: Seq[FieldAlteration] = if (newField.isTypeWithSubfields) {
                        field.get.asStruct.icebergCollectFieldAlterations(
                            newField.asStruct,
                            Some(newField.fieldPathForChildren(parentFieldPath))
                        )
                    } else Seq.empty[FieldAlteration]
                    typeAlteration ++ commentAlteration ++ subFieldsAlterations
                }
            } ++ dropAlterations
        }
    }

    implicit class IcebergDataFrameExtensions(df: DataFrame) extends SparkSqlExtensions.DataFrameExtensions(df)
}
