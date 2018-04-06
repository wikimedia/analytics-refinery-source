package org.wikimedia.analytics.refinery.job.mediawikihistory

import java.sql.Timestamp

import org.wikimedia.analytics.refinery.core.TimestampHelpers

object TestHelpers {

  def string(v: String): Option[String] = if (v == "None") None else Some(v)
  def long(v: String): Option[Long] = if (v == "None") None else Some(v.toLong)
  def int(v: String): Option[Int] = if (v == "None") None else Some(v.toInt)
  def boolean(v: String): Option[Boolean] = if (v == "None") None else Some(v.toBoolean)
  def timestamp(v: String): Option[Timestamp] = {
    if (v == "None") None
    else if (v.length == 14) TimestampHelpers.makeMediawikiTimestamp(v) // If the string length is 14, mediawiki-formatted timestamp
    else Some(new Timestamp(v.toLong)) // Else just a simple long-build timestamp
  }
  def list(v: String): Option[Seq[String]] = v match {
    case "None" => None
    case "Empty" => Some(Seq.empty)
    case csv => Some(csv.filter(!"()".contains(_)).split(",").map(_.trim).filter(_.nonEmpty))
  }

}
