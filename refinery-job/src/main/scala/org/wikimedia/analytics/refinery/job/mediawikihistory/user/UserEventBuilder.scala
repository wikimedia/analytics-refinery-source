package org.wikimedia.analytics.refinery.job.mediawikihistory.user

/**
  * This object contains utility functions to parse user data
  * from the logging table.
  * It uses [[org.wikimedia.analytics.refinery.job.mediawikihistory.utils.PhpUnserializer]].
  */
object UserEventBuilder extends Serializable {

  import org.apache.spark.sql.Row
  import org.joda.time.DateTime
  import org.joda.time.format.DateTimeFormat
  import org.wikimedia.analytics.refinery.job.mediawikihistory.utils.PhpUnserializer

  val userRenamePattern = """^[^\[]*\[\[[^:]*:([^|]*)\|.*\]\][^\[]*\[\[[^:]*:(.*)\|.*$""".r
  val userCreatePattern = """^[^\[]*\[\[[^:]*:([^|]*)\|.*\]\].*$""".r
  val durationSeparatorPattern = """[0-9]+\s[a-z]+""".r
  val durationParsingPattern = """^([0-9]+)\s([a-z]+?)s?$""".r
  val ipPattern = """^.*[0-9]{1,3}(\.[0-9]{1,3}){3}.*$""".r
  val macPattern = """^.*[0-9A-F]{1,4}(:[0-9A-F]{1,4}){7}.*$""".r
  val weirdIdPattern = """^(#[0-9]+)$""".r
  val botUsernamePattern = """(?i)^.*bot([^a-z].*$|$)""".r

  def isBotByName(userName: String): Boolean = {
    if (userName != null)
      botUsernamePattern.findFirstIn(userName).isDefined
    else
      false
  }

  def getOldAndNewUserNames(
      logParams: String,
      logComment: String,
      logTitle: String
  ): (String, String, Option[String]) = {
    try {
      val logParamsMap =
        PhpUnserializer.unserialize(logParams).asInstanceOf[Map[String, Any]]
      (
          logParamsMap("4::olduser").asInstanceOf[String],
          logParamsMap("5::newuser").asInstanceOf[String],
          None
      )
    } catch {
      case _: Throwable =>
        try {
          logComment match {
            case userRenamePattern(oldName, newName) =>
              (oldName, newName, None)
          }
        } catch {
          case _: Throwable =>
            if ((logTitle != null) && (logParams != null))
              (logTitle.replaceAll("_", " "), logParams, None)
            else
              (null, null, Some("Could not get old username from null logTitle or logParams"))
        }
    }
  }

  def getCreationNames(
      logComment: String,
      logTitle: String
  ): (String, String, Option[String]) = {
    try {
      logComment match {
        case userCreatePattern(name) => (name, name, None)
      }
    } catch {
      case _: Throwable =>
        if (logTitle != null) {
          val userName = logTitle.replaceAll("_", " ")
          (userName, userName, None)
        } else
          (null, null, Some("Could not get creation names from null logtitle"))
    }
  }

  def csvToSeq(csv: String): Seq[String] = {
    val trimmed = csv.trim
    if (trimmed == "") Seq.empty else trimmed.split(",").map(_.trim)
  }

  def getOldAndNewUserGroups(
      logParams: String,
      logComment: String
  ): (Seq[String], Seq[String], Option[String]) = {
    try {
      val paramsMap =
        PhpUnserializer.unserialize(logParams).asInstanceOf[Map[String, Any]]
      def paramToSeq(param: String): Seq[String] = {
        paramsMap(param).asInstanceOf[Map[String, String]].values.toList
      }
      (paramToSeq("4::oldgroups"), paramToSeq("5::newgroups"), None)
    } catch {
      case _: Throwable =>
        try {
          if (!logParams.contains("\n")) { throw new Exception }
          val splitParams = logParams.split("\n")
          if (splitParams.size == 1) {
            (csvToSeq(splitParams(0)), Seq.empty[String], None)
          } else if (splitParams.size == 2) {
            (csvToSeq(splitParams(0)),
             csvToSeq(splitParams(1)),
             None)
          } else { throw new Exception }
        } catch {
          case _: Throwable =>
            if (logParams != null && logParams != "") {
              (Seq.empty[String], Seq.empty[String], Some("Could not parse groups from: " + logParams))
            } else if (logComment != null && logComment.startsWith("=")) {
              (Seq.empty[String],
               logComment.replaceAll("=", "").split(",").map(_.trim),
               None)
            } else if (logComment != null && logComment.contains("+")) {
              (Seq.empty[String],
               logComment.split(" ").map(_.replace("+", "").trim),
               None)
            } else {
              (Seq.empty[String], Seq.empty[String], Some("Could not parse groups from: " + logComment))
            }
        }
    }
  }

  def applyDurationToTimestamp(timestamp: String, duration: String): String = {
    duration.trim match {
      case "indefinite" | "infinite" | "never" | "" => "indefinite"
      case nonIndefiniteDuration =>
        val durationUnits = durationSeparatorPattern
          .findAllIn(nonIndefiniteDuration)
        if (durationUnits.isEmpty) { throw new IllegalStateException }
        val offsetInSeconds = durationUnits
          .map { durationUnit =>
            durationUnit.trim match {
              case durationParsingPattern(number, period) =>
                val factor = period match {
                  case "second" => 1
                  case "minute" => 60
                  case "hour" => 3600
                  case "day" => 86400
                  case "week" => 604800
                  case "month" => 2592000
                  case "year" => 31536000
                }
                factor * number.toInt
            }
          }
          .sum
        val timestampFormat = DateTimeFormat.forPattern("yyyyMMddHHmmss").withZoneUTC()
        val dateTime = DateTime.parse(timestamp, timestampFormat)
        timestampFormat.print(dateTime.plusSeconds(offsetInSeconds))
    }
  }

  def getNewUserBlocksAndBlockExpiration(
      logParams: String,
      timestamp: String
  ): (Seq[String], Option[String], Option[String]) = {
    val (newUserBlocksStr, blockExpirationStr) = try {
      val paramsMap =
        PhpUnserializer.unserialize(logParams).asInstanceOf[Map[String, Any]]
      (
          paramsMap("6::flags").asInstanceOf[String],
          paramsMap("5::duration").asInstanceOf[String]
      )
    } catch {
      case _: Throwable =>
        if (logParams != null) {
          val eolCount = logParams.count(_ == '\n')
          if (eolCount == 0) {
            ("", logParams.trim)
          } else if (eolCount == 1) {
            val splitParams = logParams.split("\n", -1).map(_.trim)
            (splitParams(1), splitParams(0))
          } else {
            return (Seq.empty[String],
              None,
              Some("Could not parse blocks from: " + logParams))
          }
        } else {
          return (Seq.empty[String],
            None,
            Some("Could not parse blocks from: " + logParams))
        }
  }
    val newUserBlocks = csvToSeq(newUserBlocksStr)
    val blockExpiration = try {
      applyDurationToTimestamp(timestamp, blockExpirationStr)
    } catch {
      case _: Throwable =>
        val dateTime = try {
          DateTime.parse(blockExpirationStr,
                         DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss").withZoneUTC())
        } catch {
          case _: Throwable =>
            try {
              DateTime.parse(
                  blockExpirationStr,
                  DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZoneUTC())
            } catch {
              case _: Throwable =>
                try {
                  DateTime.parse(blockExpirationStr,
                                 DateTimeFormat.forPattern(
                                     "E, dd MMM yyyy HH:mm:ss 'GMT'").withZoneUTC())
                } catch {
                  case _: Throwable =>
                    try {
                      DateTime.parse(blockExpirationStr,
                                     DateTimeFormat.forPattern(
                                         "dd MMM yyyy HH:mm:ss 'GMT'").withZoneUTC())
                    } catch {
                      case _: Throwable =>
                        try {
                          DateTime.parse(
                              blockExpirationStr,
                              DateTimeFormat.forPattern("dd MMM yyyy").withZoneUTC())
                        } catch {
                          case _: Throwable =>
                            try {
                              DateTime.parse(
                                  timestamp
                                    .substring(0, 8) + blockExpirationStr,
                                  DateTimeFormat.forPattern("yyyyMMddHH:mm").withZoneUTC())
                            } catch {
                              case _: Throwable =>
                                return (Seq.empty[String],
                                        None,
                                        Some(
                                            "Could not parse blocks from: " + logParams))
                            }
                        }
                    }
                }
            }
        }
        DateTimeFormat.forPattern("yyyyMMddHHmmss").withZoneUTC().print(dateTime)
    }
    (newUserBlocks, Some(blockExpiration), None)
  }

  def isValidLog(log: Row): Boolean = {
    log.getString(4) match {
      case ipPattern(_) => false
      case macPattern(_) => false
      case weirdIdPattern(_) => false
      case _ => true
    }
  }

  def buildUserEvent(log: Row): UserEvent = {
    val logType = log.getString(0)
    val logAction = log.getString(1)
    val logTimestamp = log.getString(2)
    val logUser = if (log.isNullAt(3)) None else Some(log.getLong(3))
    val logTitle = log.getString(4)
    val logComment = log.getString(5)
    val logParams = log.getString(6)
    val wikiDb = log.getString(7)

    val eventType = logType match {
      case "renameuser" => "rename"
      case "rights" => "altergroups"
      case "block" => "alterblocks"
      case "newusers" => "create"
    }

    val (oldUserName, newUserName, namesError) = eventType match {
      case "rename" =>
        getOldAndNewUserNames(logParams, logComment, logTitle)
      case "create" =>
        getCreationNames(logComment, logTitle)
      case _ =>
        if (logTitle != null) {
          val userName = logTitle.replaceAll("_", " ")
          (userName, userName, None)
        } else
          (null, null, Some("Could not get names from null logtitle"))
    }

    val (oldUserGroups, newUserGroups, groupsError) = eventType match {
      case "altergroups" => getOldAndNewUserGroups(logParams, logComment)
      case _ => (Seq.empty, Seq.empty, None)
    }

    val (newUserBlocks, blockExpiration, blocksError) = eventType match {
      case "alterblocks" if logAction != "unblock" =>
        getNewUserBlocksAndBlockExpiration(logParams, logTimestamp)
      case _ => (Seq.empty[String], None, None)
    }

    val createEvent = eventType == "create"
    val createdBySelf = createEvent && (logAction == "create")
    val createdBySystem = createEvent && (logAction == "autocreate")
    val createdByPeer = createEvent && ((logAction == "create2") || (logAction == "byemail"))

    val parsingErrors = if (groupsError.isDefined || blocksError.isDefined || namesError.isDefined) {
      Seq(groupsError, blocksError, namesError).flatMap {
        case None => Seq.empty[String]
        case Some(error) => Seq(error)
      }
    } else Seq.empty[String]

    new UserEvent(
        wikiDb = wikiDb,
        timestamp = logTimestamp,
        eventType = eventType,
        causedByUserId = logUser,
        oldUserName = oldUserName,
        newUserName = newUserName,
        oldUserGroups = oldUserGroups,
        newUserGroups = newUserGroups,
        newUserBlocks = newUserBlocks,
        blockExpiration = blockExpiration,
        createdBySelf = createdBySelf,
        createdBySystem = createdBySystem,
        createdByPeer = createdByPeer,
        parsingErrors = parsingErrors
    )
  }
}
