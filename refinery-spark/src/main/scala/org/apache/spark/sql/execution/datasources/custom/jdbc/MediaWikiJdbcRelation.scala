/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/** NOTE: this file is copied from Spark and slightly modified.  The original source is on the 3.1.2 branch
 *  as that is the version we are using at the time of this copy:
 *
 *  https://github.com/apache/spark/blob/v3.1.2/sql/core/src/main/scala/
 *            org/apache/spark/sql/execution/datasources/jdbc/JDBCRelation.scala
 *
 *  Modifications are marked with BEGIN and END comment blocks
 */
package org.apache.spark.sql.execution.datasources.custom.jdbc

import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{getZoneId, stringToDate, stringToTimestamp, stringToTimestampAnsi}
import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions.{JDBC_LOWER_BOUND, JDBC_NUM_PARTITIONS, JDBC_UPPER_BOUND}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCPartition, JDBCPartitioningInfo, JDBCRDD, JDBCRelation, JdbcUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ArrayBuffer

/**
 * This MediaWiki-specific version implements one additional DataType allowed for
 *   partitionColumn: BinaryType
 * When passing in a string value to upper and lower bound, it will be interpreted
 *   as a MediaWiki binary-format timestamp of size 14, in configuration YYYYMMDDhhmmss
 * NOTE: the companion class, JDBCRelation, does not need to change to implement this
 */
private[sql] object MediaWikiJdbcRelation extends Logging {
  /**
   * Given a partitioning schematic (a column of integral type, a number of
   * partitions, and upper and lower bounds on the column's value), generate
   * WHERE clauses for each partition so that each row in the table appears
   * exactly once.  The parameters minValue and maxValue are advisory in that
   * incorrect values may cause the partitioning to be poor, but no data
   * will fail to be represented.
   *
   * Null value predicate is added to the first partition where clause to include
   * the rows with null value for the partitions column.
   *
   * @param schema resolved schema of a JDBC table
   * @param resolver function used to determine if two identifiers are equal
   * @param timeZoneId timezone ID to be used if a partition column type is date or timestamp
   *                   or the custom MediaWiki-specific binary type string timestamp in format YYYYMMDDhhmmss
   * @param jdbcOptions JDBC options that contains url
   * @return an array of partitions with where clause for each partition
   */
  def columnPartition(
                       schema: StructType,
                       resolver: Resolver,
                       timeZoneId: String,
                       jdbcOptions: JDBCOptions): Array[Partition] = {
    val partitioning = {

      val partitionColumn = jdbcOptions.partitionColumn
      val lowerBound = jdbcOptions.lowerBound
      val upperBound = jdbcOptions.upperBound
      val numPartitions = jdbcOptions.numPartitions

      if (partitionColumn.isEmpty) {
        assert(lowerBound.isEmpty && upperBound.isEmpty, "When 'partitionColumn' is not " +
          s"specified, '$JDBC_LOWER_BOUND' and '$JDBC_UPPER_BOUND' are expected to be empty")
        null
      } else {
        assert(lowerBound.nonEmpty && upperBound.nonEmpty && numPartitions.nonEmpty,
          s"When 'partitionColumn' is specified, '$JDBC_LOWER_BOUND', '$JDBC_UPPER_BOUND', and " +
            s"'$JDBC_NUM_PARTITIONS' are also required")

        val (column, columnType) = verifyAndGetNormalizedPartitionColumn(
          schema, partitionColumn.get, resolver, jdbcOptions)

        val lowerBoundValue = toInternalBoundValue(lowerBound.get, columnType, timeZoneId)
        val upperBoundValue = toInternalBoundValue(upperBound.get, columnType, timeZoneId)
        JDBCPartitioningInfo(
          column, columnType, lowerBoundValue, upperBoundValue, numPartitions.get)
      }
    }

    if (partitioning == null || partitioning.numPartitions <= 1 ||
      partitioning.lowerBound == partitioning.upperBound) {
      return Array[Partition](JDBCPartition(null, 0))
    }

    val lowerBound = partitioning.lowerBound
    val upperBound = partitioning.upperBound
    require (lowerBound <= upperBound,
      "Operation not allowed: the lower bound of partitioning column is larger than the upper " +
        s"bound. Lower bound: $lowerBound; Upper bound: $upperBound")

    val boundValueToString: Long => String =
      toBoundValueInWhereClause(_, partitioning.columnType, timeZoneId)
    val numPartitions =
      if ((upperBound - lowerBound) >= partitioning.numPartitions || /* check for overflow */
        (upperBound - lowerBound) < 0) {
        partitioning.numPartitions
      } else {
        logWarning("The number of partitions is reduced because the specified number of " +
          "partitions is less than the difference between upper bound and lower bound. " +
          s"Updated number of partitions: ${upperBound - lowerBound}; Input number of " +
          s"partitions: ${partitioning.numPartitions}; " +
          s"Lower bound: ${boundValueToString(lowerBound)}; " +
          s"Upper bound: ${boundValueToString(upperBound)}.")
        upperBound - lowerBound
      }
    // Overflow and silliness can happen if you subtract then divide.
    // Here we get a little roundoff, but that's (hopefully) OK.
    val stride: Long = upperBound / numPartitions - lowerBound / numPartitions

    var i: Int = 0
    val column = partitioning.column
    var currentValue = lowerBound
    val ans = new ArrayBuffer[Partition]()
    while (i < numPartitions) {
      val lBoundValue = boundValueToString(currentValue)
      val lBound = if (i != 0) s"$column >= $lBoundValue" else null
      currentValue += stride
      val uBoundValue = boundValueToString(currentValue)
      val uBound = if (i != numPartitions - 1) s"$column < $uBoundValue" else null
      val whereClause =
        if (uBound == null) {
          lBound
        } else if (lBound == null) {
          s"$uBound or $column is null"
        } else {
          s"$lBound AND $uBound"
        }
      ans += JDBCPartition(whereClause, i)
      i = i + 1
    }
    val partitions = ans.toArray
    logInfo(s"Number of partitions: $numPartitions, WHERE clauses of these partitions: " +
      partitions.map(_.asInstanceOf[JDBCPartition].whereClause).mkString(", "))
    partitions
  }

  // Verify column name and type based on the JDBC resolved schema
  private def verifyAndGetNormalizedPartitionColumn(
                                                     schema: StructType,
                                                     columnName: String,
                                                     resolver: Resolver,
                                                     jdbcOptions: JDBCOptions): (String, DataType) = {
    val dialect = JdbcDialects.get(jdbcOptions.url)
    val column = schema.find { f =>
      resolver(f.name, columnName) || resolver(dialect.quoteIdentifier(f.name), columnName)
    }.getOrElse {
      val maxNumToStringFields = SQLConf.get.maxToStringFields
      throw new AnalysisException(s"User-defined partition column $columnName not " +
        s"found in the JDBC relation: ${schema.simpleString(maxNumToStringFields)}")
    }
    column.dataType match {
      case _: NumericType | DateType | BinaryType | TimestampType =>
      case _ =>
        throw new AnalysisException(
          s"Partition column type should be ${NumericType.simpleString}, " +
            // -- BEGIN MediaWiki MODIFICATION --
            s"${BinaryType.catalogString} (representing YYYYMMDDhhmmss timestamps), " +
            // -- END MediaWiki MODIFICATION --
            s"${DateType.catalogString}, or ${TimestampType.catalogString}, but " +
            s"${column.dataType.catalogString} found.")
    }
    (dialect.quoteIdentifier(column.name), column.dataType)
  }

  // this converts upper and lower bounds to Long for partitioning, customized to deal with mw timestamps
  private def toInternalBoundValue(
                                    value: String,
                                    columnType: DataType,
                                    timeZoneId: String): Long = {
    def parse[T](f: UTF8String => Option[T]): T = {
      f(UTF8String.fromString(value)).getOrElse {
        throw new IllegalArgumentException(
          s"Cannot parse the bound value $value as ${columnType.catalogString}")
      }
    }
    // -- BEGIN MediaWiki MODIFICATION --
    def formatMediaWikiToISO (mw: UTF8String): UTF8String = UTF8String.fromString(
      s"${mw.substring(0, 4)}-${mw.substring(4, 6)}-${mw.substring(6, 8)} " +
        s"${mw.substring(8, 10)}:${mw.substring(10, 12)}:${mw.substring(12, 14)}"
    )
    // -- END MediaWiki MODIFICATION --

    columnType match {
      case _: NumericType => value.toLong
      case DateType => parse(stringToDate(_, getZoneId(timeZoneId))).toLong
      case TimestampType => parse(stringToTimestamp(_, getZoneId(timeZoneId)))
      // -- BEGIN MediaWiki MODIFICATION --
      case BinaryType => parse((u: UTF8String) => stringToTimestamp(formatMediaWikiToISO(u), getZoneId(timeZoneId)))
      // -- END MediaWiki MODIFICATION --
    }
  }

  // this converts Longs back to a string that can be passed to a MariaDB query, so it's the reverse of the above
  private def toBoundValueInWhereClause(
                                         value: Long,
                                         columnType: DataType,
                                         timeZoneId: String): String = {
    def dateTimeToString(): String = {
      val dateTimeStr = columnType match {
        case DateType =>
          val dateFormatter = DateFormatter(DateTimeUtils.getZoneId(timeZoneId))
          dateFormatter.format(value.toInt)
        case TimestampType | BinaryType =>
          val timestampFormatter = TimestampFormatter.getFractionFormatter(
            DateTimeUtils.getZoneId(timeZoneId))
          timestampFormatter.format(value)
      }
      s"'$dateTimeStr'"
    }
    columnType match {
      case _: NumericType => value.toString
      // -- BEGIN MediaWiki MODIFICATION --
      // dateTimeToString above just happens to do exactly what we need, but may not in future versions
      // the conversion from iso to mw format is simple, just remove everything that's not a digit
      case BinaryType => dateTimeToString().replaceAll("[^0-9']", "")
      // -- END MediaWiki MODIFICATION --
      case DateType | TimestampType => dateTimeToString()
    }
  }

  /**
   * Takes a (schema, table) specification and returns the table's Catalyst schema.

   * If `customSchema` defined in the JDBC options, replaces the schema's dataType with the
   * custom schema's type.
   *
   * @param resolver function used to determine if two identifiers are equal
   * @param jdbcOptions JDBC options that contains url, table and other information.
   * @return resolved Catalyst schema of a JDBC table
   */
  def getSchema(resolver: Resolver, jdbcOptions: JDBCOptions): StructType = {
    val tableSchema = JDBCRDD.resolveTable(jdbcOptions)
    jdbcOptions.customSchema match {
      case Some(customSchema) => JdbcUtils.getCustomSchema(
        tableSchema, customSchema, resolver)
      case None => tableSchema
    }
  }

  /**
   * Resolves a Catalyst schema of a JDBC table and returns [[JDBCRelation]] with the schema.
   */
  def apply(
             parts: Array[Partition],
             jdbcOptions: JDBCOptions)(
             sparkSession: SparkSession): JDBCRelation = {
    val schema = JDBCRelation.getSchema(sparkSession.sessionState.conf.resolver, jdbcOptions)
    JDBCRelation(schema, parts, jdbcOptions)(sparkSession)
  }
}

