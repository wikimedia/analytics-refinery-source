package org.wikimedia.analytics.refinery.job.mediawikihistory

/**
 * Created by jo on 10/3/16.
 */
object TestHelpers {

  def string(v: String): Option[String] = if (v == "None") None else Some(v)
  def long(v: String): Option[Long] = if (v == "None") None else Some(v.toLong)
  def int(v: String): Option[Int] = if (v == "None") None else Some(v.toInt)
  def boolean(v: String): Option[Boolean] = if (v == "None") None else Some(v.toBoolean)
  def list(v: String): Option[Seq[String]] = v match {
    case "None" => None
    case "Empty" => Some(Seq.empty)
    case csv => Some(csv.filter(!"()".contains(_)).split(",").map(_.trim).filter(_.nonEmpty))
  }

}
