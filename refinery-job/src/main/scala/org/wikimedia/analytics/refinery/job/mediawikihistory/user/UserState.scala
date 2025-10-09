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
                      // Generic fields
                      wikiDb: String,
                      startTimestamp: Option[Timestamp] = None,
                      endTimestamp: Option[Timestamp] = None,
                      causedByEventType: String,
                      causedByUserId: Option[Long] = None,
                      causedByUserCentralId: Option[Long] = None,
                      causedByAnonymousUser: Option[Boolean] = None,
                      causedByTemporaryUser: Option[Boolean] = None,
                      causedByPermanentUser: Option[Boolean] = None,
                      causedByUserText: Option[String] = None,
                      // Specific fields
                      userId: Long,
                      userCentralId: Option[Long],
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
                      isAnonymous: Boolean = false,
                      isTemporary: Boolean,
                      isPermanent: Boolean,
                      causedByBlockExpiration: Option[String] = None,
                      inferredFrom: Option[String] = None,
                      sourceLogId: Option[Long] = None,
                      sourceLogComment: Option[String] = None,
                      sourceLogParams: Option[Map[String, String]] = None
) extends Vertex[(String, String)] with TimeBoundaries {

  def toRow: Row = Row(
      wikiDb,
      userId,
      userCentralId.orNull,
      userTextHistorical,
      userText,
      userGroupsHistorical,
      userGroups,
      userBlocksHistorical,
      userBlocks,
      isBotByHistorical,
      isBotBy,
      userRegistrationTimestamp.map(_.toString).orNull,
      userCreationTimestamp.map(_.toString).orNull,
      userFirstEditTimestamp.map(_.toString).orNull,
      createdBySelf,
      createdBySystem,
      createdByPeer,
      isAnonymous,
      isTemporary,
      isPermanent,
      startTimestamp.map(_.toString).orNull,
      endTimestamp.map(_.toString).orNull,
      causedByEventType,
      causedByUserId.orNull,
      causedByUserCentralId.orNull,
      causedByAnonymousUser.orNull,
      causedByTemporaryUser.orNull,
      causedByPermanentUser.orNull,
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
      userCentralId = getOptLong(row, 2),
      userTextHistorical = row.getString(3),
      userText = row.getString(4),
      userGroupsHistorical = row.getSeq(5),
      userGroups = row.getSeq(6),
      userBlocksHistorical = row.getSeq(7),
      userBlocks = row.getSeq(8),
      isBotByHistorical = row.getSeq(9),
      isBotBy = row.getSeq(10),
      userRegistrationTimestamp = getOptTimestamp(row, 11),
      //userRegistrationTimestamp = if (row.isNullAt(10)) None else Some(row.getTimestamp(10)),
      userCreationTimestamp = getOptTimestamp(row, 12),
      //userCreationTimestamp = if (row.isNullAt(11)) None else Some(row.getTimestamp(11)),
      userFirstEditTimestamp = getOptTimestamp(row, 13),
      //userFirstEditTimestamp = if (row.isNullAt(12)) None else Some(row.getTimestamp(12)),
      createdBySelf = row.getBoolean(14),
      createdBySystem = row.getBoolean(15),
      createdByPeer = row.getBoolean(16),
      isAnonymous = row.getBoolean(17),
      isTemporary = row.getBoolean(18),
      isPermanent = row.getBoolean(19),
      startTimestamp = getOptTimestamp(row, 20),
      //startTimestamp = if (row.isNullAt(20)) None else Some(row.getTimestamp(19)),
      endTimestamp = getOptTimestamp(row, 21),
      //endTimestamp = if (row.isNullAt(21)) None else Some(row.getTimestamp(20)),
      causedByEventType = row.getString(22),
      causedByUserId = getOptLong(row, 23),
      causedByUserCentralId = getOptLong(row, 24),
      causedByAnonymousUser = getOptBoolean(row, 25),
      causedByTemporaryUser = getOptBoolean(row, 26),
      causedByPermanentUser = getOptBoolean(row, 27),
      causedByUserText = getOptString(row, 28),
      causedByBlockExpiration = getOptString(row, 29),
      inferredFrom = getOptString(row, 30),
      sourceLogId = getOptLong(row, 31),
      sourceLogComment = getOptString(row, 32),
      sourceLogParams = getOptMap[String, String](row, 33)
  )

  val schema: StructType = StructType(
    Seq(
      StructField("wiki_db", StringType, nullable = false),
      StructField("user_id", LongType, nullable = false),
      StructField("user_central_id", LongType, nullable = true),
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
      StructField("is_anonymous", BooleanType, nullable = false),
      StructField("is_temporary", BooleanType, nullable = false),
      StructField("is_permanent", BooleanType, nullable = false),
      StructField("start_timestamp", StringType, nullable = true),
      //StructField("start_timestamp", TimestampType, nullable = true),
      StructField("end_timestamp", StringType, nullable = true),
      //StructField("end_timestamp", TimestampType, nullable = true),
      StructField("caused_by_event_type", StringType, nullable = false),
      StructField("caused_by_user_id", LongType, nullable = true),
      StructField("caused_by_user_central_id", LongType, nullable = true),
      StructField("caused_by_anonymous_user", BooleanType, nullable = true),
      StructField("caused_by_temporary_user", BooleanType, nullable = true),
      StructField("caused_by_permanent_user", BooleanType, nullable = true),
      StructField("caused_by_user_text", StringType, nullable = true),
      StructField("caused_by_block_expiration", StringType, nullable = true),
      StructField("inferred_from", StringType, nullable = true),
      StructField("source_log_id", LongType, nullable = true),
      StructField("source_log_comment", StringType, nullable = true),
      StructField("source_log_params", MapType(StringType, StringType, valueContainsNull = true), nullable = true)
    )
  )
}
