package org.wikimedia.analytics.refinery.job.mediawikihistory.user

import java.sql.Timestamp
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.wikimedia.analytics.refinery.job.mediawikihistory._
import org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized.TimeBoundaries
import org.wikimedia.analytics.refinery.spark.utils.Vertex

/**
  * This case class represents a user state object, by opposition
  * to a user event object. It extends [[Vertex]] (for graph partitioning)
  * with [[key]] defined as (WikiDb, userText), and [[TimeBoundaries]]
  * since it has [[startTimestamp]] and [[endTimestamp]] fields.
  * It provides utility functions to read/write spark Rows.
  */
case class UserState(
                      // Generic Fields
                      wikiDb: String,
                      startTimestamp: Option[Timestamp] = None,
                      endTimestamp: Option[Timestamp] = None,
                      causedByEventType: String,
                      causedByUserId: Option[Long] = None,
                      causedByAnonymousUser: Option[Boolean] = None,
                      causedByUserText: Option[String] = None,
                      // Specific fields
                      userId: Long,
                      userTextHistorical: String,
                      userText: String,
                      userGroupsHistorical: Seq[String] = Seq.empty[String],
                      userGroups: Seq[String] = Seq.empty[String],
                      userBlocksHistorical: Seq[String] = Seq.empty[String],
                      userBlocks: Seq[String] = Seq.empty[String],
                      isBotByHistorical: Seq[String] = Seq.empty[String],
                      isBotBy: Seq[String] = Seq.empty[String],
                      userRegistrationTimestamp: Option[Timestamp] = None,
                      userCreationTimestamp: Option[Timestamp] = None,
                      userFirstEditTimestamp: Option[Timestamp] = None,
                      createdBySelf: Boolean = false,
                      createdBySystem: Boolean = false,
                      createdByPeer: Boolean = false,
                      anonymous: Boolean = false,
                      causedByBlockExpiration: Option[String] = None,
                      inferredFrom: Option[String] = None,
                      sourceLogId: Option[Long] = None,
                      sourceLogComment: Option[String] = None,
                      sourceLogParams: Option[Map[String, String]] = None
) extends Vertex[(String, String)] with TimeBoundaries {

  def toRow: Row = Row(
      wikiDb,
      userId,
      userTextHistorical,
      userText,
      userGroupsHistorical,
      userGroups,
      userBlocksHistorical,
      userBlocks,
      isBotByHistorical,
      isBotBy,
      userRegistrationTimestamp.map(_.toString).orNull,
      //userRegistrationTimestamp.orNull,
      userCreationTimestamp.map(_.toString).orNull,
      //userCreationTimestamp.orNull,
      userFirstEditTimestamp.map(_.toString).orNull,
      //userFirstEditTimestamp.orNull,
      createdBySelf,
      createdBySystem,
      createdByPeer,
      anonymous,
      startTimestamp.map(_.toString).orNull,
      //startTimestamp.orNull,
      endTimestamp.map(_.toString).orNull,
      //endTimestamp.orNull,
      causedByEventType,
      causedByUserId.orNull,
      causedByAnonymousUser.orNull,
      causedByUserText.orNull,
      causedByBlockExpiration.orNull,
      inferredFrom.orNull,
      sourceLogId.orNull,
      sourceLogComment.orNull,
      sourceLogParams.orNull
  )

  override def key: (String, String) = (wikiDb, userTextHistorical)
}

object UserState {

  /**
    * Rebuilds a [[UserState]] from a row saved following [[UserState.schema]] (first 19 fields).
    *
    * Used in [[org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized.DenormalizedRunner.run]]
    * to load objects from parquet files.
    *
    * @param row The UserState row to load data from
    * @return the [[UserState]] built
    */
  def fromRow(row: Row): UserState = UserState(
      wikiDb = row.getString(0),
      userId = row.getLong(1),
      userTextHistorical = row.getString(2),
      userText = row.getString(3),
      userGroupsHistorical = row.getSeq(4),
      userGroups = row.getSeq(5),
      userBlocksHistorical = row.getSeq(6),
      userBlocks = row.getSeq(7),
      isBotByHistorical = row.getSeq(8),
      isBotBy = row.getSeq(9),
      userRegistrationTimestamp = getOptTimestamp(row, 10),
      //userRegistrationTimestamp = if (row.isNullAt(10)) None else Some(row.getTimestamp(10)),
      userCreationTimestamp = getOptTimestamp(row, 11),
      //userCreationTimestamp = if (row.isNullAt(11)) None else Some(row.getTimestamp(11)),
      userFirstEditTimestamp = getOptTimestamp(row, 12),
      //userFirstEditTimestamp = if (row.isNullAt(12)) None else Some(row.getTimestamp(12)),
      createdBySelf = row.getBoolean(13),
      createdBySystem = row.getBoolean(14),
      createdByPeer = row.getBoolean(15),
      anonymous = row.getBoolean(16),
      startTimestamp = getOptTimestamp(row, 17),
      //startTimestamp = if (row.isNullAt(17)) None else Some(row.getTimestamp(17)),
      endTimestamp = getOptTimestamp(row, 18),
      //endTimestamp = if (row.isNullAt(18)) None else Some(row.getTimestamp(18)),
      causedByEventType = row.getString(19),
      causedByUserId = getOptLong(row, 20),
      causedByAnonymousUser = getOptBoolean(row, 21),
      causedByUserText = getOptString(row, 22),
      causedByBlockExpiration = getOptString(row, 23),
      inferredFrom = getOptString(row, 24),
      sourceLogId = getOptLong(row, 25),
      sourceLogComment = getOptString(row, 26),
      sourceLogParams = getOptMap[String, String](row, 27)
  )

  val schema: StructType = StructType(
    Seq(
      StructField("wiki_db", StringType, nullable = false),
      StructField("user_id", LongType, nullable = false),
      StructField("user_text_historical", StringType, nullable = false),
      StructField("user_text", StringType, nullable = false),
      StructField("user_groups_historical", ArrayType(StringType, containsNull = true), nullable = false),
      StructField("user_groups", ArrayType(StringType, containsNull = true), nullable = false),
      StructField("user_blocks_historical", ArrayType(StringType, containsNull = true), nullable = false),
      StructField("user_blocks", ArrayType(StringType, containsNull = true), nullable = false),
      StructField("is_bot_by_historical", ArrayType(StringType, containsNull = true), nullable = false),
      StructField("is_bot_by", ArrayType(StringType, containsNull = true), nullable = false),
      StructField("user_registration_timestamp", StringType, nullable = true),
      //StructField("user_registration_timestamp", TimestampType, nullable = true),
      StructField("user_creation_timestamp", StringType, nullable = true),
      //StructField("user_creation_timestamp", TimestampType, nullable = true),
      StructField("user_first_edit_timestamp", StringType, nullable = true),
      //StructField("user_first_edit_timestamp", TimestampType, nullable = true),
      StructField("created_by_self", BooleanType, nullable = false),
      StructField("created_by_system", BooleanType, nullable = false),
      StructField("created_by_peer", BooleanType, nullable = false),
      StructField("anonymous", BooleanType, nullable = false),
      StructField("start_timestamp", StringType, nullable = true),
      //StructField("start_timestamp", TimestampType, nullable = true),
      StructField("end_timestamp", StringType, nullable = true),
      //StructField("end_timestamp", TimestampType, nullable = true),
      StructField("caused_by_event_type", StringType, nullable = false),
      StructField("caused_by_user_id", LongType, nullable = true),
      StructField("caused_by_anonymous_user", BooleanType, nullable = true),
      StructField("caused_by_user_text", StringType, nullable = true),
      StructField("caused_by_block_expiration", StringType, nullable = true),
      StructField("inferred_from", StringType, nullable = true),
      StructField("source_log_id", LongType, nullable = true),
      StructField("source_log_comment", StringType, nullable = true),
      StructField("source_log_params", MapType(StringType, StringType, valueContainsNull = true), nullable = true)
    )
  )
}
