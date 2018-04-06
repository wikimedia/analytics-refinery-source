package org.wikimedia.analytics.refinery.core

import java.sql.Timestamp

import org.joda.time.format.DateTimeFormat


object TimestampHelpers {

  @transient
  lazy val timestampPattern = "^\\d{14}$".r
  lazy val timestampParser = DateTimeFormat.forPattern("YYYYMMddHHmmss")

  def makeMediawikiTimestamp(s: String): Option[Timestamp] = {
    Option(s) // Handle null string case
      .filter(timestampPattern.findFirstIn(_).isDefined) // Handle wrongly formatted timestamp
      .map(s =>new Timestamp(timestampParser.parseDateTime(s).getMillis)) // Handle regular use case
  }


  implicit def orderedTimestamp: Ordering[Timestamp] = new Ordering[Timestamp] {
    def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
  }

  /**
    * Returns the number of seconds elapsed between the first timestamp and the second one.
    **/
  def getTimestampDifference(ot1: Option[Timestamp], ot2: Option[Timestamp]): Option[Long] = (ot1, ot2) match {
    case (Some(t1), Some(t2)) => Option((t1.getTime - t2.getTime) / 1000)
    case _ => None
  }

}
