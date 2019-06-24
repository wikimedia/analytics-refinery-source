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
                      causedByAnonymousUser: Option[Boolean] = None,
                      causedByUserText: Option[String] = None,
                      // Specific fields
                      pageId: Option[Long] = None,
                      pageArtificialId: Option[String] = None,
                      titleHistorical: String,
                      title: String,
                      namespaceHistorical: Int,
                      namespaceIsContentHistorical: Boolean,
                      namespace: Int,
                      namespaceIsContent: Boolean,
                      isRedirect: Option[Boolean] = None,
                      isDeleted: Boolean,
                      pageCreationTimestamp: Option[Timestamp] = None,
                      pageFirstEditTimestamp: Option[Timestamp] = None,
                      inferredFrom: Option[String] = None,
                      sourceLogId: Option[Long] = None,
                      sourceLogComment: Option[String] = None,
                      sourceLogParams: Option[Map[String, String]] = None
) extends Vertex[(String, String, Int)] with TimeBoundaries {

  def toRow: Row = Row(
    wikiDb,
    pageId.orNull,
    pageArtificialId.orNull,
    pageCreationTimestamp.map(_.toString).orNull,
    //pageCreationTimestamp.orNull,
    titleHistorical,
    title,
    namespaceHistorical,
    namespaceIsContentHistorical,
    namespace,
    namespaceIsContent,
    isRedirect.orNull,
    isDeleted,
    startTimestamp.map(_.toString).orNull,
    //startTimestamp.orNull,
    endTimestamp.map(_.toString).orNull,
    //endTimestamp.orNull,
    causedByEventType,
    causedByUserId.orNull,
    causedByAnonymousUser.orNull,
    causedByUserText.orNull,
    inferredFrom.orNull,
    sourceLogId.orNull,
    sourceLogComment.orNull,
    sourceLogParams.orNull
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
    pageArtificialId = Option(row.getString(2)),
    pageCreationTimestamp = if (row.isNullAt(3)) None else Some(Timestamp.valueOf(row.getString(3))),
    //pageCreationTimestamp = if (row.isNullAt(3)) None else Some(row.getTimestamp(3)),
    titleHistorical = row.getString(4),
    title = row.getString(5),
    namespaceHistorical = row.getInt(6),
    namespaceIsContentHistorical = row.getBoolean(7),
    namespace = row.getInt(8),
    namespaceIsContent = row.getBoolean(9),
    isRedirect = if (row.isNullAt(10)) None else Some(row.getBoolean(10)),
    isDeleted = row.getBoolean(11),
    startTimestamp = if (row.isNullAt(12)) None else Some(Timestamp.valueOf(row.getString(12))),
    //startTimestamp = if (row.isNullAt(12)) None else Some(row.getTimestamp(12)),
    endTimestamp = if (row.isNullAt(13)) None else Some(Timestamp.valueOf(row.getString(13))),
    //endTimestamp = if (row.isNullAt(13)) None else Some(row.getTimestamp(13)),
    causedByEventType = row.getString(14),
    causedByUserId = if (row.isNullAt(15)) None else Some(row.getLong(15)),
    causedByAnonymousUser = if (row.isNullAt(16)) None else Some(row.getBoolean(16)),
    causedByUserText = Option(row.getString(17)),
    inferredFrom = Option(row.getString(18)),
    sourceLogId = if (row.isNullAt(19)) None else Some(row.getLong(19)),
    sourceLogComment = Option(row.getString(20)),
    sourceLogParams = Option(row.getMap[String, String](21)).map(_.toMap)
  )

  val schema = StructType(
    Seq(
      StructField("wiki_db", StringType, nullable = false),
      StructField("page_id", LongType, nullable = true),
      StructField("page_artificial_id", StringType, nullable = true),
      StructField("page_creation_timestamp", StringType, nullable = true),
      //StructField("page_creation_timestamp", TimestampType, nullable = true),
      StructField("page_title_historical", StringType, nullable = false),
      StructField("page_title", StringType, nullable = false),
      StructField("page_namespace_historical", IntegerType, nullable = false),
      StructField("page_namespace_is_content_historical", BooleanType, nullable = false),
      StructField("page_namespace", IntegerType, nullable = false),
      StructField("page_namespace_is_content", BooleanType, nullable = false),
      StructField("page_is_redirect", BooleanType, nullable = true),
      StructField("page_is_deleted", BooleanType, nullable = false),
      StructField("start_timestamp", StringType, nullable = true),
      //StructField("start_timestamp", TimestampType, nullable = true),
      StructField("end_timestamp", StringType, nullable = true),
      //StructField("end_timestamp", TimestampType, nullable = true),
      StructField("caused_by_event_type", StringType, nullable = false),
      StructField("caused_by_user_id", LongType, nullable = true),
      StructField("caused_by_anonymous_user", BooleanType, nullable = true),
      StructField("caused_by_user_text", StringType, nullable = true),
      StructField("inferred_from", StringType, nullable = true),
      StructField("source_log_id", LongType, nullable = true),
      StructField("source_log_comment", StringType, nullable = true),
      StructField("source_log_params", MapType(StringType, StringType, valueContainsNull = true), nullable = true)
    )
  )
}
