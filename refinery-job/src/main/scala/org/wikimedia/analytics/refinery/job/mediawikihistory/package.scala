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

}
