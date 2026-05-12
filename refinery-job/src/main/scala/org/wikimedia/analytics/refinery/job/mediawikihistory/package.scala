package org.wikimedia.analytics.refinery.job

import org.apache.spark.sql.Row

import java.sql.Timestamp

package object mediawikihistory {

    def getOptString(row: Row, idx: Int): Option[String] = {
        if (row.isNullAt(idx)) None else Some(row.getString(idx))
    }

    def getOptInt(row: Row, idx: Int): Option[Int] = {
        if (row.isNullAt(idx)) None else Some(row.getInt(idx))
    }

    def getOptLong(row: Row, idx: Int): Option[Long] = {
        if (row.isNullAt(idx)) None else Some(row.getLong(idx))
    }

    def getOptBoolean(row: Row, idx: Int): Option[Boolean] = {
        if (row.isNullAt(idx)) None else Some(row.getBoolean(idx))
    }

    def getOptTimestamp(row: Row, idx: Int): Option[Timestamp] = {
        if (row.isNullAt(idx)) None else Some(Timestamp.valueOf(row.getString(idx)))
    }

    def getOptSeq[T](row: Row, idx: Int): Option[Seq[T]] = {
        if (row.isNullAt(idx)) None else Some(row.getSeq[T](idx))
    }

    def getOptMap[T1, T2](row: Row, idx: Int): Option[Map[T1, T2]] = {
        if (row.isNullAt(idx)) None else Some(row.getMap[T1, T2](idx).toMap)
    }

    def getOptString(row: Row, col: String): Option[String] = {
        getOptString(row, row.fieldIndex(col))
    }

    def getOptInt(row: Row, col: String): Option[Int] = {
        getOptInt(row, row.fieldIndex(col))
    }

    def getOptLong(row: Row, col: String): Option[Long] = {
        getOptLong(row, row.fieldIndex(col))
    }

    def getOptBoolean(row: Row, col: String): Option[Boolean] = {
        getOptBoolean(row, row.fieldIndex(col))
    }

    def getOptTimestamp(row: Row, col: String): Option[Timestamp] = {
        getOptTimestamp(row, row.fieldIndex(col))
    }

    def getOptSeq[T](row: Row, col: String): Option[Seq[T]] = {
        getOptSeq[T](row, row.fieldIndex(col))
    }

    def getOptMap[T1, T2](row: Row, col: String): Option[Map[T1, T2]] = {
        getOptMap[T1, T2](row, row.fieldIndex(col))
    }

}
