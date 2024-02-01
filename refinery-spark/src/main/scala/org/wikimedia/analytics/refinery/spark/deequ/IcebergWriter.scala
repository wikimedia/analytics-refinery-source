package org.wikimedia.analytics.refinery.spark.deequ

import org.apache.spark.sql.DataFrame
/**
 * Writes a DataFrame out as an Iceberg table.
 * If the table doesn't exist, it will first be created using the DataFrame's schema.
 *
 * This is a very rudimentary connector.  It will not handle any table evolution.
 * If the table already exists, be sure that the dataframe can be written to it.
 */
protected class IcebergWriter(table: String,
                              dataFrame: DataFrame,
                              partitionColumnNames: Seq[String]) extends Writer {
    private val TableFormatVersion: String = "2"
    private var _outputTable = table
    private var _createIfAbsent = false
    private def createTableDDL: String = {
        val partitionClause = partitionColumnNames match {
            case Seq() => "-- No partition provided"
            case _ => s"PARTITIONED BY (${partitionColumnNames.mkString(",")})"
        }
        s"""
             |CREATE TABLE ${output}
             |    (${dataFrame.schema.toDDL})
             |USING iceberg TBLPROPERTIES ('format-version'='$TableFormatVersion')
             |${partitionClause}
             |""".stripMargin
    }

    /**
     * Set the output table
     *
     * @param table
     * @return
     */
    def output(table: String): IcebergWriter = {
      this._outputTable = table
      this
    }
    /**
     * Get the output table
     */
    def output: String = this._outputTable


    /**
     * Create the table and database if they do not exist.
     * False by default.
     *
     * @return
     */
    def createIfAbsent: IcebergWriter = {
        this._createIfAbsent = true
        this
    }
    /**
     * Append `dataFrame` to the target partition into
     * `table`.
     *
     */
    def save(): Unit = {
        val spark = dataFrame.sparkSession
        if (!spark.catalog.tableExists(output) && _createIfAbsent) {
            spark.sql(createTableDDL)
        }
        dataFrame.writeTo(output).append()
    }
}

/**
    * Write a Spark DataFrame to Iceberg table.
    * The table will be created if it doesn't yet exist.
    *
    * @param table
    *     Fully qualified database.table name to write to
    *
    * @param dataFrame
    *     DataFrame to write
    *
    * @param partitionColumnNames
    *     Names of columns on which to partition.  These columns
    *     must exist in the dataFrame.
    */

object IcebergWriter {
    def apply(
               table: String,
               dataFrame: DataFrame,
               partitionColumnNames: Seq[String] = Seq.empty
             ): IcebergWriter = new IcebergWriter(table, dataFrame, partitionColumnNames)
}