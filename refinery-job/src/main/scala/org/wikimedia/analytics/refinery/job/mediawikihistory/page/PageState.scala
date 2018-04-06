package org.wikimedia.analytics.refinery.job.mediawikihistory.page

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized.TimeBoundaries
import org.wikimedia.analytics.refinery.spark.utils.Vertex


/**
  * This case class represents a page state object, by opposition
  * to a page event object. It extends [[Vertex]] (for graph partitioning)
  * with [[key]] defined as (WikiDb, title, namespace), and [[TimeBoundaries]]
  * since it has [[startTimestamp]] and [[endTimestamp]] fields.
  * It provides utility functions to read/write spark Rows.
  */
case class PageState(
                      // Generic Fields
                      wikiDb: String,
                      startTimestamp: Option[Timestamp] = None,
                      endTimestamp: Option[Timestamp] = None,
                      causedByEventType: String,
                      causedByUserId: Option[Long] = None,
                      // Specific fields
                      pageId: Option[Long] = None,
                      pageIdArtificial: Option[String] = None,
                      titleHistorical: String,
                      title: String,
                      namespaceHistorical: Int,
                      namespaceIsContentHistorical: Boolean,
                      namespace: Int,
                      namespaceIsContent: Boolean,
                      isRedirect: Option[Boolean] = None,
                      pageCreationTimestamp: Option[Timestamp] = None,
                      pageFirstEditTimestamp: Option[Timestamp] = None,
                      inferredFrom: Option[String] = None
) extends Vertex[(String, String, Int)] with TimeBoundaries {

  def toRow: Row = Row(
    wikiDb,
    pageId.orNull,
    pageIdArtificial.orNull,
    pageCreationTimestamp.map(_.toString).orNull,
    //pageCreationTimestamp.orNull,
    titleHistorical,
    title,
    namespaceHistorical,
    namespaceIsContentHistorical,
    namespace,
    namespaceIsContent,
    isRedirect.orNull,
    startTimestamp.map(_.toString).orNull,
    //startTimestamp.orNull,
    endTimestamp.map(_.toString).orNull,
    //endTimestamp.orNull,
    causedByEventType,
    causedByUserId.orNull,
    inferredFrom.orNull
  )

  def key: (String, String, Int) = (wikiDb, titleHistorical, namespaceHistorical)
}

object PageState {

  /**
    * Rebuilds a [[PageState]] from a row saved following [[PageState.schema]] (first 15 fields).
    *
    * Used in [[org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized.DenormalizedRunner.run]]
    * to load objects from parquet files.
    *
    * @param row The PageState row to load data from
    * @return the [[PageState]] built
    */
  def fromRow(row: Row): PageState = PageState(
    wikiDb = row.getString(0),
    pageId = if (row.isNullAt(1)) None else Some(row.getLong(1)),
    pageIdArtificial = Option(row.getString(2)),
    pageCreationTimestamp = if (row.isNullAt(3)) None else Some(Timestamp.valueOf(row.getString(3))),
    //pageCreationTimestamp = if (row.isNullAt(3)) None else Some(row.getTimestamp(3)),
    titleHistorical = row.getString(4),
    title = row.getString(5),
    namespaceHistorical = row.getInt(6),
    namespaceIsContentHistorical = row.getBoolean(7),
    namespace = row.getInt(8),
    namespaceIsContent = row.getBoolean(9),
    isRedirect = if (row.isNullAt(10)) None else Some(row.getBoolean(10)),
    startTimestamp = if (row.isNullAt(11)) None else Some(Timestamp.valueOf(row.getString(11))),
    //startTimestamp = if (row.isNullAt(11)) None else Some(row.getTimestamp(11)),
    endTimestamp = if (row.isNullAt(12)) None else Some(Timestamp.valueOf(row.getString(12))),
    //endTimestamp = if (row.isNullAt(12)) None else Some(row.getTimestamp(12)),
    causedByEventType = row.getString(13),
    causedByUserId = if (row.isNullAt(14)) None else Some(row.getLong(14)),
    inferredFrom = Option(row.getString(15))
  )

  val schema = StructType(
    Seq(
      StructField("wiki_db", StringType, nullable = false),
      StructField("page_id", LongType, nullable = true),
      StructField("page_id_artificial", StringType, nullable = true),
      StructField("page_creation_timestamp", StringType, nullable = true),
      //StructField("page_creation_timestamp", TimestampType, nullable = true),
      StructField("page_title_historical", StringType, nullable = false),
      StructField("page_title", StringType, nullable = false),
      StructField("page_namespace_historical", IntegerType, nullable = false),
      StructField("page_namespace_is_content_historical", BooleanType, nullable = false),
      StructField("page_namespace", IntegerType, nullable = false),
      StructField("page_namespace_is_content", BooleanType, nullable = false),
      StructField("page_is_redirect", BooleanType, nullable = true),
      StructField("start_timestamp", StringType, nullable = true),
      //StructField("start_timestamp", TimestampType, nullable = true),
      StructField("end_timestamp", StringType, nullable = true),
      //StructField("end_timestamp", TimestampType, nullable = true),
      StructField("caused_by_event_type", StringType, nullable = false),
      StructField("caused_by_user_id", LongType, nullable = true),
      StructField("inferred_from", StringType, nullable = true)
    )
  )
}
