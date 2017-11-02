package org.wikimedia.analytics.refinery.job.mediawikihistory.user

import java.sql.Timestamp

import org.wikimedia.analytics.refinery.job.mediawikihistory.TestHelpers
import org.wikimedia.analytics.refinery.job.mediawikihistory.TestHelpers._
import org.wikimedia.analytics.refinery.job.mediawikihistory.utils.TimestampHelpers

object TestUserHistoryHelpers {

  def userEventSet(
    wikiDb: Option[String] = Some("testwiki"),
    timestamp: Option[Timestamp] = None,
    eventType: Option[String] = None,
    causedByUserId: Option[Long] = Some(0L),
    oldUserName: Option[String] = Some("User"),
    newUserName: Option[String] = Some("User"),
    oldUserGroups: Option[Seq[String]] = Some(Seq.empty),
    newUserGroups: Option[Seq[String]] = Some(Seq.empty),
    newUserBlocks: Option[Seq[String]] = Some(Seq.empty),
    blockExpiration: Option[String] = None,
    autoCreate: Option[Boolean] = Some(false),
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
          case "oldName" | "oldUserName" => ("oldUserName" -> string(v))
          case "newName" | "newUserName" => ("newUserName" -> string(v))
          case "oldGroups" | "oldUserGroups" => ("oldUserGroups" -> list(v))
          case "newGroups" | "newUserGroups" => ("newUserGroups" -> list(v))
          case "newBlocks" | "newUserBlocks" => ("newUserBlocks" -> list(v))
          case "expiration" | "blockExpiration" => ("blockExpiration" -> string(v))
          case "auto" | "autoCreate" => ("autoCreate" -> boolean(v))
        }
      }.toMap
      new UserEvent(
        wikiDb = valueMap.getOrElse("wikiDb", wikiDb).get.asInstanceOf[String],
        timestamp = valueMap.getOrElse("timestamp", timestamp).get.asInstanceOf[Timestamp],
        eventType = valueMap.getOrElse("eventType", eventType).get.asInstanceOf[String],
        causedByUserId = valueMap.getOrElse("causedByUserId", causedByUserId).asInstanceOf[Option[Long]],
        oldUserName = valueMap.getOrElse("oldUserName", oldUserName).get.asInstanceOf[String],
        newUserName = valueMap.getOrElse("newUserName", newUserName).get.asInstanceOf[String],
        oldUserGroups = valueMap.getOrElse("oldUserGroups", oldUserGroups).get.asInstanceOf[Seq[String]],
        newUserGroups = valueMap.getOrElse("newUserGroups", newUserGroups).get.asInstanceOf[Seq[String]],
        newUserBlocks = valueMap.getOrElse("newUserBlocks", newUserBlocks).get.asInstanceOf[Seq[String]],
        blockExpiration = valueMap.getOrElse("blockExpiration", blockExpiration).asInstanceOf[Option[String]],
        createdBySystem = valueMap.getOrElse("autoCreate", autoCreate).get.asInstanceOf[Boolean],
        parsingErrors = parsingErrors
      )
    }
  }

  def userStateSet(
                    wikiDb: Option[String] = Some("testwiki"),
                    startTimestamp: Option[Timestamp] = None,
                    endTimestamp: Option[Timestamp] = None,
                    causedByEventType: Option[String] = Some(null),
                    causedByUserId: Option[Long] = Some(0L),
                    userId: Option[Long] = Some(1L),
                    userNameHistorical: Option[String] = Some("User"),
                    userName: Option[String] = None,
                    userGroupsHistorical: Option[Seq[String]] = Some(Seq.empty),
                    userGroups: Option[Seq[String]] = Some(Seq.empty),
                    userBlocksHistorical: Option[Seq[String]] = Some(Seq.empty),
                    userBlocks: Option[Seq[String]] = Some(Seq.empty),
                    userRegistration: Option[Timestamp] = TimestampHelpers.makeMediawikiTimestamp("20010115000000"),
                    autoCreate: Option[Boolean] = Some(false),
                    causedByBlockExpiration: Option[String] = None,
                    inferredFrom: Option[String] = None
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
          case "id" | "userId" => ("userId" -> long(v))
          case "nameH" | "userNameH" => ("userNameHistorical" -> string(v))
          case "name" | "userName" => ("userName" -> string(v))
          case "groupsH" | "userGroupsH" => ("userGroupsHistorical" -> list(v))
          case "groups" | "userGroups" => ("userGroups" -> list(v))
          case "blocksH" | "userBlocksH" => ("userBlocksHistorical" -> list(v))
          case "blocks" | "userBlocks" => ("userBlocks" -> list(v))
          case "registration" | "userRegistration" => ("userRegistration" -> timestamp(v))
          case "auto" | "autoCreate" => ("autoCreate" -> boolean(v))
          case "expiration" | "causedByBlockExpiration" => ("causedByBlockExpiration" -> string(v))
          case "inferred" | "inferredFrom" => ("inferredFrom" -> string(v))
        }
      }.toMap
      val userNameHistoricalVal = valueMap.getOrElse("userNameHistorical", userNameHistorical).get.asInstanceOf[String]
      val userGroupsHistoricalVal = valueMap.getOrElse("userGroupsHistorical", userGroupsHistorical).get.asInstanceOf[Seq[String]]
      val userBlocksHistoricalVal = valueMap.getOrElse("userBlocksHistorical", userBlocksHistorical).get.asInstanceOf[Seq[String]]
      new UserState(
        wikiDb = valueMap.getOrElse("wikiDb", wikiDb).get.asInstanceOf[String],
        startTimestamp = valueMap.getOrElse("startTimestamp", startTimestamp).asInstanceOf[Option[Timestamp]],
        endTimestamp = valueMap.getOrElse("endTimestamp", endTimestamp).asInstanceOf[Option[Timestamp]],
        causedByEventType = valueMap.getOrElse("causedByEventType", causedByEventType).get.asInstanceOf[String],
        causedByUserId = valueMap.getOrElse("causedByUserId", causedByUserId).asInstanceOf[Option[Long]],
        userId = valueMap.getOrElse("userId", userId).get.asInstanceOf[Long],
        userNameHistorical = userNameHistoricalVal,
        userName = valueMap.getOrElse("userName", Some(userNameHistoricalVal)).get.asInstanceOf[String],
        userGroupsHistorical = userGroupsHistoricalVal,
        userGroups = valueMap.getOrElse("userGroups", Some(userGroupsHistoricalVal)).get.asInstanceOf[Seq[String]],
        userBlocksHistorical = userBlocksHistoricalVal,
        userBlocks = valueMap.getOrElse("userBlocks", Some(userBlocksHistoricalVal)).get.asInstanceOf[Seq[String]],
        userRegistrationTimestamp = valueMap.getOrElse("userRegistration", userRegistration).asInstanceOf[Option[Timestamp]],
        createdBySystem = valueMap.getOrElse("autoCreate", autoCreate).get.asInstanceOf[Boolean],
        causedByBlockExpiration = valueMap.getOrElse("causedByBlockExpiration", causedByBlockExpiration).asInstanceOf[Option[String]],
        inferredFrom = valueMap.getOrElse("inferredFrom", inferredFrom).asInstanceOf[Option[String]]
      )
    }
  }
}
