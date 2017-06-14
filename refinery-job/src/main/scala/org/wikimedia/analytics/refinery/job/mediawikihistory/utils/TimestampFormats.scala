package org.wikimedia.analytics.refinery.job.mediawikihistory.utils

import java.sql.Timestamp

import com.github.nscala_time.time.Imports._


object TimestampFormats {

  implicit def orderedTimestamp: Ordering[Timestamp] = new Ordering[Timestamp] {
    def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
  }

  @transient
  lazy val timestampPattern = "^\\d{14}$".r
  lazy val timestampParser = DateTimeFormat.forPattern("YYYYMMddHHmmss")

  def makeMediawikiTimestamp(s: String): Option[Timestamp] = {
    Option(s) // Handle null string case
      .filter(timestampPattern.findFirstIn(_).isDefined) // Handle wrongly formatted timestamp
      .map(s =>new Timestamp(timestampParser.parseDateTime(s).getMillis)) // Handle regular use case
  }

}
