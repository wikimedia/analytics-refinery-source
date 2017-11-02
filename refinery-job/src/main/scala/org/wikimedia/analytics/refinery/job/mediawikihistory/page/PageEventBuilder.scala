package org.wikimedia.analytics.refinery.job.mediawikihistory.page

/**
  * This object contains utility functions to parse page data
  * from the logging table.
  * It uses [[org.wikimedia.analytics.refinery.job.mediawikihistory.utils.PhpUnserializer]].
  */
object PageEventBuilder extends Serializable {

  import org.apache.spark.sql.Row
  import org.wikimedia.analytics.refinery.job.mediawikihistory.utils.PhpUnserializer
  import java.sql.Timestamp
  import org.wikimedia.analytics.refinery.job.mediawikihistory.utils.TimestampHelpers

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

  /**
    * Regular expression matching a string that should contain a map of php serialized
    * values (starting with: a:NUMBER:{...})
    */
  val serializedParams = """^(a\:\d+\:\{.*\})$""".r

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
                         logParams: String): (String, String) = {
    val logParamsParsed = logParams match {
      case serializedParams(logParamsMatched) =>
        val logParamsMap = PhpUnserializer
          .unserialize(logParamsMatched)
          .asInstanceOf[Map[String, Any]]
        logParamsMap("4::target").asInstanceOf[String]
      case _ =>
        logParams
    }
    (normalizeTitle(logTitle), normalizeTitle(logParamsParsed))
  }

  /**
    * Builds a move [[PageEvent]] from project/namespace maps and a row following this schema:
    * (log_type, log_timestamp, user_id, old_page_title, log_params, old_page_namespace, wiki_db, event_type)
    *
    * Notes: user_id is the one of the user at the origin of the event.
    *        log_type is so far use on.ly with move value in this function. See [[buildSimplePageEvent]].
    *
    * @param canonicalNamespaceMap A map providing canonical namespace name for each project/namespace
    * @param localizedNamespaceMap A map providing localized namespace name for each project/namespace
    * @param isContentNamespaceMap A map providing isContent value for each project/namespace
    * @param log The row containing the move data
    * @return the move [[PageEvent]] built
    */
  def buildMovePageEvent(
      canonicalNamespaceMap: Map[(String, String), Int],
      localizedNamespaceMap: Map[(String, String), Int],
      isContentNamespaceMap: Map[(String, Int), Boolean]
  )(log: Row): PageEvent = {
    val logType = log.getString(0)
    val logTimestampUnchecked = TimestampHelpers.makeMediawikiTimestamp(log.getString(1))
    val logUser = if (log.isNullAt(2)) None else Some(log.getLong(2))
    val logTitle = log.getString(3)
    val logParams = log.getString(4)
    val logNamespace = if (log.isNullAt(5)) Integer.MIN_VALUE else log.getInt(5)
    val wikiDb = log.getString(6)

    // Handle timestamp possible error
    val logTimestamp = logTimestampUnchecked.getOrElse(new Timestamp(0L))
    val timestampError = if (logTimestampUnchecked.isEmpty) Seq("Could not parse timestamp") else Seq.empty[String]

    // Get old and new titles
    if (logTitle == null || logParams == null)
      new PageEvent(
        wikiDb = wikiDb,
        timestamp = logTimestamp,
        eventType = logType,
        oldTitle = "",
        newTitle = "",
        newTitlePrefix = "",
        newTitleWithoutPrefix = "",
        oldNamespace = Integer.MIN_VALUE,
        oldNamespaceIsContent = false,
        newNamespace = Integer.MIN_VALUE,
        newNamespaceIsContent = false,
        parsingErrors = timestampError ++ Seq("Could not parse old and new titles from null logTitle or logParams")
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

      val newNamespace = newTitlePrefix match {
        case "" => 0
        case prefix =>
          localizedNamespaceMap.getOrElse((wikiDb, prefix),
            canonicalNamespaceMap.getOrElse((wikiDb, prefix), 0))
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
          newNamespaceIsContent = isContentNamespaceMap((wikiDb, newNamespace)),
          timestamp = logTimestamp,
          eventType = logType,
          causedByUserId = logUser,
          parsingErrors = timestampError
      )
    }
  }

  /**
    * Builds a [[PageEvent]] from a map isContent value for each project/namespace
    * and a row following this schema:
    * (page_id, page_title, page_namespace, log_timestamp, user_id, wiki_db, log_type)
    *
    * Notes: user_id is the one of the user at the origin of the event.
    *        log_type is to be either delete or restore in this function. See [[buildMovePageEvent]]
    *
    * @param isContentNamespaceMap A map providing isContent value for each project/namespace
    * @param log The row containing the data
    * @return The [[PageEvent]] built
    */
  def buildSimplePageEvent(isContentNamespaceMap: Map[(String, Int), Boolean])(log: Row): PageEvent = {
    val wikiDb = log.getString(5)

    // Handle possible title error
    val title = log.getString(1)
    val titleError = if (title == null) Seq("Could not get title from null logTitle") else Seq.empty[String]

    val namespace = if (log.isNullAt(2)) Integer.MIN_VALUE else log.getInt(2)
    val namespaceIsContent = isContentNamespaceMap((wikiDb, namespace))
    val logTimestampUnchecked = TimestampHelpers.makeMediawikiTimestamp(log.getString(3))

    // Handle possible timestamp error
    val logTimestamp = logTimestampUnchecked.getOrElse(new Timestamp(0L))
    val timestampError = if (logTimestampUnchecked.isEmpty) Seq("Could not parse timestamp") else Seq.empty[String]

    new PageEvent(
      pageId = if (log.isNullAt(0)) None else Some(log.getLong(0)),
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
      eventType = log.getString(6),
      causedByUserId = if (log.isNullAt(4)) None else Some(log.getLong(4)),
      wikiDb = wikiDb,
      parsingErrors = titleError ++ timestampError
    )
  }

}
