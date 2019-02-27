package org.wikimedia.analytics.refinery.job.mediawikihistory.user

import java.sql.Timestamp

import org.wikimedia.analytics.refinery.core.TimestampHelpers
import org.wikimedia.analytics.refinery.job.mediawikihistory.TestHelpers
import org.wikimedia.analytics.refinery.job.mediawikihistory.TestHelpers._

object TestUserHistoryHelpers {

  def userEventSet(
    wikiDb: Option[String] = Some("testwiki"),
    timestamp: Option[Timestamp] = None,
    eventType: Option[String] = None,
    causedByUserId: Option[Long] = Some(1L),
    causedByUserText: Option[String] = Some("User"),
    causedByAnonymousUser: Option[Boolean] = Some(false),
    oldUserText: Option[String] = Some("User"),
    newUserText: Option[String] = Some("User"),
    oldUserGroups: Option[Seq[String]] = Some(Seq.empty),
    newUserGroups: Option[Seq[String]] = Some(Seq.empty),
    newUserBlocks: Option[Seq[String]] = Some(Seq.empty),
    blockExpiration: Option[String] = None,
    autoCreate: Option[Boolean] = Some(false),
    sourceLogId: Option[Long] = Some(0L),
    sourceLogComment: Option[String] = Some("comment"),
    sourceLogParams: Option[Map[String, String]] = Some(Map.empty),
    parsingErrors: Seq[String] = Seq.empty
  )(
    headerLine: String,
    eventLines: String*
  ): Seq[UserEvent] = {
    val headers = headerLine.split(" ").filter(_.nonEmpty)
    eventLines.map { line =>
      val values = line.split(" ").filter(_.nonEmpty)
      val valueMap = headers.zip(values).map { case (h, v) =>
        h match {
          case "wiki" | "db" | "wikiDb" => ("wikiDb" -> string(v))
          case "time" | "timestamp" => ("timestamp" -> TestHelpers.timestamp(v))
          case "type" | "eventType" => ("eventType" -> string(v))
          case "adminId" | "causedByUserId" => ("causedByUserId" -> long(v))
          case "adminText" | "causedByUserText" => ("causedByUserText" -> string(v))
          case "adminAnon" | "causedByAnonymousUser" => ("causedByAnonymousUser" -> boolean(v))
          case "oldText" | "oldUserText" => ("oldUserText" -> string(v))
          case "newText" | "newUserText" => ("newUserText" -> string(v))
          case "oldGroups" | "oldUserGroups" => ("oldUserGroups" -> list(v))
          case "newGroups" | "newUserGroups" => ("newUserGroups" -> list(v))
          case "newBlocks" | "newUserBlocks" => ("newUserBlocks" -> list(v))
          case "expiration" | "blockExpiration" => ("blockExpiration" -> string(v))
          case "auto" | "autoCreate" => ("autoCreate" -> boolean(v))
          case "log" | "logId" | "sourceLogId" => ("sourceLogId" -> long(v))
          case "commentText" | "sourceLogComment" => ("sourceLogComment" -> string(v))
          case "params" | "logParams" | "sourceLogParams" => ("sourceLogParams" -> map(v))
        }
      }.toMap
      new UserEvent(
        wikiDb = valueMap.getOrElse("wikiDb", wikiDb).get.asInstanceOf[String],
        timestamp = valueMap.getOrElse("timestamp", timestamp).get.asInstanceOf[Timestamp],
        eventType = valueMap.getOrElse("eventType", eventType).get.asInstanceOf[String],
        causedByUserId = valueMap.getOrElse("causedByUserId", causedByUserId).asInstanceOf[Option[Long]],
        causedByAnonymousUser = valueMap.getOrElse("causedByAnonymousUser", causedByAnonymousUser).asInstanceOf[Option[Boolean]],
        causedByUserText = valueMap.getOrElse("causedByUserText", causedByUserText).asInstanceOf[Option[String]],
        oldUserText = valueMap.getOrElse("oldUserText", oldUserText).get.asInstanceOf[String],
        newUserText = valueMap.getOrElse("newUserText", newUserText).get.asInstanceOf[String],
        oldUserGroups = valueMap.getOrElse("oldUserGroups", oldUserGroups).get.asInstanceOf[Seq[String]],
        newUserGroups = valueMap.getOrElse("newUserGroups", newUserGroups).get.asInstanceOf[Seq[String]],
        newUserBlocks = valueMap.getOrElse("newUserBlocks", newUserBlocks).get.asInstanceOf[Seq[String]],
        blockExpiration = valueMap.getOrElse("blockExpiration", blockExpiration).asInstanceOf[Option[String]],
        createdBySystem = valueMap.getOrElse("autoCreate", autoCreate).get.asInstanceOf[Boolean],
        sourceLogId = valueMap.getOrElse("sourceLogId", sourceLogId).get.asInstanceOf[Long],
        sourceLogComment = valueMap.getOrElse("sourceLogComment", sourceLogComment).get.asInstanceOf[String],
        sourceLogParams = valueMap.getOrElse("sourceLogParams", sourceLogParams).get.asInstanceOf[Map[String, String]],
        parsingErrors = parsingErrors
      )
    }
  }

  def userStateSet(
                    wikiDb: Option[String] = Some("testwiki"),
                    startTimestamp: Option[Timestamp] = None,
                    endTimestamp: Option[Timestamp] = None,
                    causedByEventType: Option[String] = Some(null),
                    causedByUserId: Option[Long] = Some(1L),
                    causedByUserText: Option[String] = Some("User"),
                    causedByAnonymousUser: Option[Boolean] = Some(false),
                    userId: Option[Long] = Some(1L),
                    userTextHistorical: Option[String] = Some("User"),
                    userText: Option[String] = None,
                    userGroupsHistorical: Option[Seq[String]] = Some(Seq.empty),
                    userGroups: Option[Seq[String]] = Some(Seq.empty),
                    userBlocksHistorical: Option[Seq[String]] = Some(Seq.empty),
                    userBlocks: Option[Seq[String]] = Some(Seq.empty),
                    userRegistration: Option[Timestamp] = TimestampHelpers.makeMediawikiTimestampOption("20010115000000"),
                    userCreation: Option[Timestamp] = TimestampHelpers.makeMediawikiTimestampOption("20010115000000"),
                    userFirstEdit: Option[Timestamp] = TimestampHelpers.makeMediawikiTimestampOption("20010115000000"),
                    autoCreate: Option[Boolean] = Some(false),
                    causedByBlockExpiration: Option[String] = None,
                    inferredFrom: Option[String] = None,
                    sourceLogId: Option[Long] = Some(0L),
                    sourceLogComment: Option[String] = Some("comment"),
                    sourceLogParams: Option[Map[String, String]] = Some(Map.empty)

  )(
    headerLine: String,
    stateLines: String*
  ): Seq[UserState] = {
    val headers = headerLine.split(" ").filter(_.nonEmpty)
    stateLines.map { line =>
      val values = line.split(" ").filter(_.nonEmpty)
      val valueMap = headers.zip(values).map { case (h, v) =>
        h match {
          case "wiki" | "db" | "wikiDb" => ("wikiDb" -> string(v))
          case "start" | "startTimestamp" => ("startTimestamp" -> timestamp(v))
          case "end" | "endTimestamp" => ("endTimestamp" -> timestamp(v))
          case "type" | "eventType" | "causedByEventType" => ("causedByEventType" -> string(v))
          case "adminId" | "causedByUserId" => ("causedByUserId" -> long(v))
          case "adminText" | "causedByUserText" => ("causedByUserText" -> string(v))
          case "adminAnon" | "causedByAnonymousUser" => ("causedByAnonymousUser" -> boolean(v))
          case "id" | "userId" => ("userId" -> long(v))
          case "textH" | "userTextH" => ("userTextHistorical" -> string(v))
          case "text" | "userText" => ("userText" -> string(v))
          case "groupsH" | "userGroupsH" => ("userGroupsHistorical" -> list(v))
          case "groups" | "userGroups" => ("userGroups" -> list(v))
          case "blocksH" | "userBlocksH" => ("userBlocksHistorical" -> list(v))
          case "blocks" | "userBlocks" => ("userBlocks" -> list(v))
          case "registration" | "userRegistration" => ("userRegistration" -> timestamp(v))
          case "creation" | "userCreation" => ("userCreation" -> timestamp(v))
          case "firstEdit" | "userFirstEdit" => ("userFirstEdit" -> timestamp(v))
          case "auto" | "autoCreate" => ("autoCreate" -> boolean(v))
          case "expiration" | "causedByBlockExpiration" => ("causedByBlockExpiration" -> string(v))
          case "inferred" | "inferredFrom" => ("inferredFrom" -> string(v))
          case "log" | "logId" | "sourceLogId" => ("sourceLogId" -> long(v))
          case "commentText" | "sourceLogComment" => ("sourceLogComment" -> string(v))
          case "params" | "logParams" | "sourceLogParams" => ("sourceLogParams" -> map(v))
        }
      }.toMap
      val userTextHistoricalVal = valueMap.getOrElse("userTextHistorical", userTextHistorical).get.asInstanceOf[String]
      val userGroupsHistoricalVal = valueMap.getOrElse("userGroupsHistorical", userGroupsHistorical).get.asInstanceOf[Seq[String]]
      val userBlocksHistoricalVal = valueMap.getOrElse("userBlocksHistorical", userBlocksHistorical).get.asInstanceOf[Seq[String]]
      new UserState(
        wikiDb = valueMap.getOrElse("wikiDb", wikiDb).get.asInstanceOf[String],
        startTimestamp = valueMap.getOrElse("startTimestamp", startTimestamp).asInstanceOf[Option[Timestamp]],
        endTimestamp = valueMap.getOrElse("endTimestamp", endTimestamp).asInstanceOf[Option[Timestamp]],
        causedByEventType = valueMap.getOrElse("causedByEventType", causedByEventType).get.asInstanceOf[String],
        causedByUserId = valueMap.getOrElse("causedByUserId", causedByUserId).asInstanceOf[Option[Long]],
        causedByUserText = valueMap.getOrElse("causedByUserText", causedByUserText).asInstanceOf[Option[String]],
        causedByAnonymousUser = valueMap.getOrElse("causedByAnonymousUser", causedByAnonymousUser).asInstanceOf[Option[Boolean]],
        userId = valueMap.getOrElse("userId", userId).get.asInstanceOf[Long],
        userTextHistorical = userTextHistoricalVal,
        userText = valueMap.getOrElse("userText", Some(userTextHistoricalVal)).get.asInstanceOf[String],
        userGroupsHistorical = userGroupsHistoricalVal,
        userGroups = valueMap.getOrElse("userGroups", Some(userGroupsHistoricalVal)).get.asInstanceOf[Seq[String]],
        userBlocksHistorical = userBlocksHistoricalVal,
        userBlocks = valueMap.getOrElse("userBlocks", Some(userBlocksHistoricalVal)).get.asInstanceOf[Seq[String]],
        userRegistrationTimestamp = valueMap.getOrElse("userRegistration", userRegistration).asInstanceOf[Option[Timestamp]],
        userCreationTimestamp = valueMap.getOrElse("userCreation", userCreation).asInstanceOf[Option[Timestamp]],
        userFirstEditTimestamp = valueMap.getOrElse("userFirstEdit", userFirstEdit).asInstanceOf[Option[Timestamp]],
        createdBySystem = valueMap.getOrElse("autoCreate", autoCreate).get.asInstanceOf[Boolean],
        causedByBlockExpiration = valueMap.getOrElse("causedByBlockExpiration", causedByBlockExpiration).asInstanceOf[Option[String]],
        inferredFrom = valueMap.getOrElse("inferredFrom", inferredFrom).asInstanceOf[Option[String]],
        sourceLogId = valueMap.getOrElse("sourceLogId", sourceLogId).asInstanceOf[Option[Long]],
        sourceLogComment = valueMap.getOrElse("sourceLogComment", sourceLogComment).asInstanceOf[Option[String]],
        sourceLogParams = valueMap.getOrElse("eventLogParams", sourceLogParams).asInstanceOf[Option[Map[String, String]]]
      )
    }
  }
}
