package org.wikimedia.analytics.refinery.job.mediawikihistory.page

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
    *   log_type
    *   log_action
    *   log_page
    *   log_timestamp
    *   log_user
    *   log_user_text
    *   log_title
    *   log_params
    *   log_namespace
    *   wiki_db
    *   log_id
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
    val logUser = if (log.isNullAt(4)) None else Some(log.getLong(4))
    val logUserText = Option(log.getString(5))
    val logTitle = log.getString(6)
    val logParams = PhpUnserializer.tryUnserializeMap(log.getString(7))
    val logNamespace = if (log.isNullAt(8)) Integer.MIN_VALUE else log.getInt(8)
    val wikiDb = log.getString(9)
    val pageIdNum = if (log.isNullAt(2)) 0L else log.getLong(2)
    val pageId = if (pageIdNum <= 0L) None else Some(pageIdNum)
    // logId always defined
    val logId = log.getLong(10)
    val logComment = log.getString(11)

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
        causedByUserId = logUser,
        causedByUserText = logUserText,
        sourceLogId = logId,
        sourceLogComment = logComment,
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
          causedByUserId = logUser,
          causedByUserText = logUserText,
          sourceLogId = logId,
          sourceLogComment = logComment,
          sourceLogParams = PageEventBuilder.normalizeLogParams(logParams),
          parsingErrors = errors
      )
    }
  }

  /**
    * Builds a [[PageEvent]] from a map isContent value for each project/namespace
    * and a row following this schema:
    *   log_type
    *   log_action
    *   log_page
    *   log_timestamp
    *   log_user
    *   log_user_text
    *   log_title
    *   log_params
    *   log_namespace
    *   wiki_db,
    *   log_id
    *
    * Notes: user_id is the one of the user at the origin of the event.
    *        log_type is to be either delete or restore in this function. See [[buildMovePageEvent]]
    *
    * @param log The row containing the data
    * @return The [[PageEvent]] built
    */
  def buildSimplePageEvent(log: Row): PageEvent = {
    val wikiDb = log.getString(9)

    // Handle possible title error
    val title = log.getString(6)
    val titleError = if (title == null) Seq("Could not get title from null logTitle") else Seq.empty[String]

    val namespace = if (log.isNullAt(8)) Integer.MIN_VALUE else log.getInt(8)
    val namespaceIsContent = isContentNamespaceMap((wikiDb, namespace))
    // Only valid timestamps accepted in SQL - no need to check parsing here
    val logTimestamp = TimestampHelpers.makeMediawikiTimestamp(log.getString(3))
    val pageIdNum = if (log.isNullAt(2)) 0L else log.getLong(2)
    val eventType = log.getString(1)
    val logParams = PhpUnserializer.tryUnserializeMap(log.getString(7))
    new PageEvent(
      pageId = if (pageIdNum <= 0L) None else Some(pageIdNum),
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
      causedByUserId = if (log.isNullAt(4)) None else Some(log.getLong(4)),
      causedByUserText = Option(log.getString(5)),
      wikiDb = wikiDb,
      sourceLogId = log.getLong(10),
      sourceLogComment = log.getString(11),
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

}
