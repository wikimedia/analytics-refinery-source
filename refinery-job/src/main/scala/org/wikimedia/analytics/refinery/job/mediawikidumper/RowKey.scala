package org.wikimedia.analytics.refinery.job.mediawikidumper

import java.sql.Timestamp

import org.apache.spark.sql.Row

/** DTO used for sorting rows.
  *
  * @param pageId
  *   page ID
  * @param timestamp
  *   revision timestamp
  */
case class RowKey(pageId: Long, timestamp: Timestamp, revisionId: Long)
    extends Ordered[RowKey] {
    override def compare(that: RowKey): Int = {
        val pageResult = pageId.compareTo(that.pageId)
        if (pageResult == 0) {
            val timestampResult = timestamp.compareTo(that.timestamp)
            if (timestampResult == 0) {
                return revisionId.compareTo(that.revisionId)
            }
            return timestampResult
        }
        pageResult
    }
}

object RowKey {
    def apply(row: Row): RowKey = RowKey(
      row.getAs("pageId"),
      row.getAs("timestamp"),
      row.getAs("revisionId")
    )
}
