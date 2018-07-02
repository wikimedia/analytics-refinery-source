package org.wikimedia.analytics.refinery.job.mediawikihistory.user

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
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
                      // Specific fields
                      userId: Long,
                      userTextHistorical: String,
                      userText: String,
                      userGroupsHistorical: Seq[String] = Seq.empty[String],
                      userGroups: Seq[String] = Seq.empty[String],
                      userBlocksHistorical: Seq[String] = Seq.empty[String],
                      userBlocks: Seq[String] = Seq.empty[String],
                      userRegistrationTimestamp: Option[Timestamp] = None,
                      createdBySelf: Boolean = false,
                      createdBySystem: Boolean = false,
                      createdByPeer: Boolean = false,
                      anonymous: Boolean = false,
                      botByName: Boolean = false,
                      causedByBlockExpiration: Option[String] = None,
                      inferredFrom: Option[String] = None
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
      userRegistrationTimestamp.map(_.toString).orNull,
      //userRegistrationTimestamp.orNull,
      createdBySelf,
      createdBySystem,
      createdByPeer,
      anonymous,
      botByName,
      startTimestamp.map(_.toString).orNull,
      //startTimestamp.orNull,
      endTimestamp.map(_.toString).orNull,
      //endTimestamp.orNull,
      causedByEventType,
      causedByUserId.orNull,
      causedByBlockExpiration.orNull,
      inferredFrom.orNull
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
      userRegistrationTimestamp = if (row.isNullAt(8)) None else Some(Timestamp.valueOf(row.getString(8))),
      //userRegistrationTimestamp = if (row.isNullAt(8)) None else Some(row.getTimestamp(8)),
      createdBySelf = row.getBoolean(9),
      createdBySystem = row.getBoolean(10),
      createdByPeer = row.getBoolean(11),
      anonymous = row.getBoolean(12),
      botByName = row.getBoolean(13),
      startTimestamp = if (row.isNullAt(14)) None else Some(Timestamp.valueOf(row.getString(14))),
      //startTimestamp = if (row.isNullAt(14)) None else Some(row.getTimestamp(14)),
      endTimestamp = if (row.isNullAt(15)) None else Some(Timestamp.valueOf(row.getString(15))),
      //endTimestamp = if (row.isNullAt(15)) None else Some(row.getTimestamp(15)),
      causedByEventType = row.getString(16),
      causedByUserId = if (row.isNullAt(17)) None else Some(row.getLong(17)),
      causedByBlockExpiration = Option(row.getString(18)),
      inferredFrom = Option(row.getString(19))
  )

  val schema = StructType(
    Seq(
      StructField("wiki_db", StringType, nullable = false),
      StructField("user_id", LongType, nullable = false),
      StructField("user_text_historical", StringType, nullable = false),
      StructField("user_text", StringType, nullable = false),
      StructField("user_groups_historical", ArrayType(StringType, containsNull = true), nullable = false),
      StructField("user_groups", ArrayType(StringType, containsNull = true), nullable = false),
      StructField("user_blocks_historical", ArrayType(StringType, containsNull = true), nullable = false),
      StructField("user_blocks", ArrayType(StringType, containsNull = true), nullable = false),
      StructField("user_registration_timestamp", StringType, nullable = true),
      //StructField("user_registration_timestamp", TimestampType, nullable = true),
      StructField("created_by_self", BooleanType, nullable = false),
      StructField("created_by_system", BooleanType, nullable = false),
      StructField("created_by_peer", BooleanType, nullable = false),
      StructField("anonymous", BooleanType, nullable = false),
      StructField("is_bot_by_name", BooleanType, nullable = false),
      StructField("start_timestamp", StringType, nullable = true),
      //StructField("start_timestamp", TimestampType, nullable = true),
      StructField("end_timestamp", StringType, nullable = true),
      //StructField("end_timestamp", TimestampType, nullable = true),
      StructField("caused_by_event_type", StringType, nullable = false),
      StructField("caused_by_user_id", LongType, nullable = true),
      StructField("caused_by_block_expiration", StringType, nullable = true),
      StructField("inferred_from", StringType, nullable = true)
    )
  )
}
