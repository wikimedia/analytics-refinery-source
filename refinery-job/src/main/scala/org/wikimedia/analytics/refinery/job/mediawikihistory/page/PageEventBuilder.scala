package org.wikimedia.analytics.refinery.job.mediawikihistory.page

import org.apache.spark.sql.Row

/**
  * This class contains utility functions to parse page data
  * from the logging table.
  * It uses [[org.wikimedia.analytics.refinery.core.PhpUnserializer]].
  *
  * @param canonicalNamespaceMap A map providing canonical namespace name for each project/namespace
  * @param localizedNamespaceMap A map providing localized namespace name for each project/namespace
  * @param isContentNamespaceMap A map providing isContent value for each project/namespace
  *
  */
class PageEventBuilder(
                        canonicalNamespaceMap: Map[(String, String), Int],
                        localizedNamespaceMap: Map[(String, String), Int],
                        isContentNamespaceMap: Map[(String, Int), Boolean]
                      ) extends Serializable {

  import org.apache.spark.sql.Row
  import org.wikimedia.analytics.refinery.core.PhpUnserializer
  import org.wikimedia.analytics.refinery.core.TimestampHelpers

  /**
    *  Try to extract new title from logParams, normalizes both old and new titles
    *  and return them as a pair of String
    *
    *
    * @param logTitle The old title to be normalized
    * @param logParams The string to extract new title from
    * @return A pair containing (old title, new title)
    */
  def getOldAndNewTitles(logTitle: String,
                         logParams: Either[Map[String, Any], String]): (String, String) = {
    (
      PageEventBuilder.normalizeTitle(logTitle),
      PageEventBuilder.normalizeTitle(logParams.fold(m => m.getOrElse("4::target", logParams).toString, s => s))
    )
  }

  /**
    * Builds a move [[PageEvent]] from a row following this schema:
    *   0 log_type,
    *   1 log_action,
    *   2 log_page,
    *   3 log_timestamp,
    *   4 actor_user,
    *   5 actor_name,
    *   6 actor_is_anon,
    *   7 actor_is_temp,
    *   8 log_title,
    *   9 log_params,
    *  10 log_namespace,
    *  11 wiki_db,
    *  12 log_id,
    *  13 comment_text
    *
    * Notes: user_id is the one of the user at the origin of the event.
    *        log_type is so far use on.ly with move value in this function. See [[buildSimplePageEvent]].
    *
    * @param log The row containing the move data
    * @return the move [[PageEvent]] built
    */
  def buildMovePageEvent(log: Row): PageEvent = {
    val logType = log.getString(0)
    // Only valid timestamps accepted in SQL - no need to check parsing here
    val logTimestamp = TimestampHelpers.makeMediawikiTimestamp(log.getString(3))
    // we check actor_name because that's a non-nullable field so a null value would mean the join failed
    val actorUser = if (log.isNullAt(4) || log.isNullAt(5)) None else Some(log.getLong(4))
    val actorName = Option(log.getString(5))
    val logTitle = log.getString(8)
    val logParams = PhpUnserializer.tryUnserializeMap(log.getString(9))
    val logNamespace = if (log.isNullAt(10)) Integer.MIN_VALUE else log.getInt(10)
    val wikiDb = log.getString(11)
    val pageId = if (log.isNullAt(2)) None else Some(log.getLong(2))
    // logId always defined
    val logId = log.getLong(12)
    val commentText = log.getString(13)

    // Assert that actor_is_anon is not true while actor_is_temp is defined and viceversa.
    assert(log.isNullAt(6) || !log.getBoolean(6) || log.isNullAt(7))

    // Get old and new titles
    if (logTitle == null || (logParams.isRight && logParams.right.get == null))
      new PageEvent(
        wikiDb = wikiDb,
        timestamp = logTimestamp,
        eventType = logType,
        oldTitle = "",
        newTitle = "",
        newTitlePrefix = "",
        newTitleWithoutPrefix = "",
        oldNamespace = Int.MinValue,
        oldNamespaceIsContent = false,
        newNamespace = Int.MinValue,
        newNamespaceIsContent = false,
        pageId = pageId,
        causedByUserId = actorUser,
        causedByAnonymousUser = PageEventBuilder.getActorIsAnonymous(log, 6, 7),
        causedByTemporaryUser = PageEventBuilder.getActorIsTemporary(log, 6, 7),
        causedByPermanentUser = PageEventBuilder.getActorIsPermanent(log, 6, 7),
        causedByUserText = actorName,
        sourceLogId = logId,
        sourceLogComment = commentText,
        sourceLogParams = PageEventBuilder.normalizeLogParams(logParams),
        parsingErrors = Seq("Could not parse old and new titles from null logTitle or logParams")
      )
    else {
      val (oldTitle, newTitle) = getOldAndNewTitles(logTitle, logParams)
      val firstColon = newTitle.indexOf(":")
      val (newTitlePrefix, newTitleWithoutPrefix) = firstColon match {
        case x: Int if x >= 0 && x <= newTitle.length =>
          (
              newTitle.substring(0, firstColon),
              newTitle.substring(firstColon + 1)
          )
        case _ => ("", newTitle)
      }

      val (newNamespace: Int, errors: Seq[String]) = newTitlePrefix match {
        case "" => (0, Seq.empty[String])
        case prefix =>
          if (localizedNamespaceMap.contains((wikiDb, prefix))) {
            (localizedNamespaceMap((wikiDb, prefix)), Seq.empty[String])
          } else if (canonicalNamespaceMap.contains((wikiDb, prefix))) {
            (canonicalNamespaceMap((wikiDb, prefix)), Seq.empty[String])
          } else {
            (Int.MinValue, Seq(s"Could not find new-namespace value '$prefix' in namespace maps"))
          }
      }
      new PageEvent(
          wikiDb = wikiDb,
          oldTitle = oldTitle,
          newTitle = newTitle,
          newTitlePrefix = newTitlePrefix,
          newTitleWithoutPrefix = newTitleWithoutPrefix,
          oldNamespace = logNamespace,
          oldNamespaceIsContent = isContentNamespaceMap((wikiDb, logNamespace)),
          newNamespace = newNamespace,
          newNamespaceIsContent = isContentNamespaceMap.getOrElse((wikiDb, newNamespace), false),
          pageId = pageId,
          timestamp = logTimestamp,
          eventType = logType,
          causedByUserId = actorUser,
          causedByAnonymousUser = PageEventBuilder.getActorIsAnonymous(log, 6, 7),
          causedByTemporaryUser = PageEventBuilder.getActorIsTemporary(log, 6, 7),
          causedByPermanentUser = PageEventBuilder.getActorIsPermanent(log, 6, 7),
          causedByUserText = actorName,
          sourceLogId = logId,
          sourceLogComment = commentText,
          sourceLogParams = PageEventBuilder.normalizeLogParams(logParams),
          parsingErrors = errors
      )
    }
  }

  /**
    * Builds a [[PageEvent]] from a map isContent value for each project/namespace
    * and a row following this schema:
    *   0 log_type,
    *   1 log_action,
    *   2 log_page,
    *   3 log_timestamp,
    *   4 actor_user,
    *   5 actor_name,
    *   6 actor_is_anon,
    *   7 actor_is_temp,
    *   8 log_title,
    *   9 log_params,
    *  10 log_namespace,
    *  11 wiki_db,
    *  12 log_id,
    *  13 comment_text
    *
    * Notes: user_id is the one of the user at the origin of the event.
    *        log_type is to be either delete or restore in this function. See [[buildMovePageEvent]]
    *
    * @param log The row containing the data
    * @return The [[PageEvent]] built
    */
  def buildSimplePageEvent(log: Row): PageEvent = {
    val logType = log.getString(0)
    val logAction = log.getString(1)
    // see [[PageHistoryRunner]] to make SURE this logic stays in sync with what log rows are selected
    val eventType = logAction match {
      case "delete_redir" => logType
      // Make an explicit create-page event to differentiate from the first-revision creation one
      // as the metrics use the latter.
      case "create" => "create-page"
      case _ =>  logAction
    }
    // Only valid timestamps accepted in SQL - no need to check parsing here
    val logTimestamp = TimestampHelpers.makeMediawikiTimestamp(log.getString(3))
    // we check actor_name because that's a non-nullable field so a null value would mean the join failed
    val actorUser = if (log.isNullAt(4) || log.isNullAt(5)) None else Some(log.getLong(4))
    val actorName = Option(log.getString(5))
    val title = log.getString(8)
    val logParams = PhpUnserializer.tryUnserializeMap(log.getString(9))
    val namespace = if (log.isNullAt(10)) Integer.MIN_VALUE else log.getInt(10)
    val wikiDb = log.getString(11)
    val logId = log.getLong(12)
    val commentText = log.getString(13)

    val namespaceIsContent = isContentNamespaceMap((wikiDb, namespace))
    // Handle possible title error
    val titleError = if (title == null) Seq("Could not get title from null logTitle") else Seq.empty[String]

    // Assert that actor_is_anon is not true while actor_is_temp is defined and viceversa.
    assert(log.isNullAt(6) || !log.getBoolean(6) || log.isNullAt(7))

    new PageEvent(
      pageId = if (log.isNullAt(2)) None else Some(log.getLong(2)),
      oldTitle = title,
      // in delete and restore events, old title = new title
      newTitle = title,
      newTitlePrefix = "",
      newTitleWithoutPrefix = title,
      oldNamespace = namespace,
      oldNamespaceIsContent = namespaceIsContent,
      newNamespace = namespace,
      newNamespaceIsContent = namespaceIsContent,
      timestamp = logTimestamp,
      eventType = eventType,
      causedByUserId = actorUser,
      causedByAnonymousUser = PageEventBuilder.getActorIsAnonymous(log, 6, 7),
      causedByTemporaryUser = PageEventBuilder.getActorIsTemporary(log, 6, 7),
      causedByPermanentUser = PageEventBuilder.getActorIsPermanent(log, 6, 7),
      causedByUserText = actorName,
      wikiDb = wikiDb,
      sourceLogId = logId,
      sourceLogComment = commentText,
      sourceLogParams = PageEventBuilder.normalizeLogParams(logParams),
      parsingErrors = titleError
    )
  }
}

object PageEventBuilder {

  /**
    * Page title normalization (trims whitespaces, swaps spaces for underscore,
    * and removes \n1 artifact)
    *
    * @param title The title to normalize
    * @return The normalized title
    */
  def normalizeTitle(title: String): String = {
    title.trim.replaceAll(" ", "_").stripSuffix("\n1")
  }

  def normalizeLogParams(logParams: Either[Map[String, Any], String]): Map[String, String] =
    logParams.fold[Map[String,String]](
      m => m.mapValues(_.toString), // The map with string values if parsed
      s => if (s != null) Map("unparsed" -> s) else Map.empty
    )

  def getActorIsAnonymous(log: Row, isAnonIndex: Int, isTempIndex: Int): Option[Boolean] = {
    if (!log.isNullAt(isAnonIndex)) {
      Some(log.getBoolean(isAnonIndex))
    } else {
      if (!log.isNullAt(isTempIndex)) Some(false)
      else None
    }
  }

  def getActorIsTemporary(log: Row, isAnonIndex: Int, isTempIndex: Int): Option[Boolean] = {
    if (!log.isNullAt(isTempIndex)) {
      Some(log.getBoolean(isTempIndex))
    } else {
      if (!log.isNullAt(isAnonIndex) && log.getBoolean(isAnonIndex)) Some(false)
      else None
    }
  }

  def getActorIsPermanent(log: Row, isAnonIndex: Int, isTempIndex: Int): Option[Boolean] = {
    if (!log.isNullAt(isTempIndex)) {
      Some(!log.getBoolean(isTempIndex))
    } else {
      if (!log.isNullAt(isAnonIndex) && log.getBoolean(isAnonIndex)) Some(false)
      else None
    }
  }

}
