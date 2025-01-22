package org.wikimedia.analytics.refinery.job.mediawikihistory.page

import java.sql.Timestamp

import org.wikimedia.analytics.refinery.core.TimestampHelpers
import org.wikimedia.analytics.refinery.spark.utils.Edge

/**
  * This case class represents a page event object, by opposition
  * to a page state object. It extends [[Edge]] (for graph partitioning)
  * with [[fromKey]] defined as (WikiDb, oldTitle, oldNamespace) and [[toKey]]
  * defined as (WikiDb, newTitle, newNamespace).
  */
case class PageEvent(
                      // Generic Fields
                      wikiDb: String,
                      timestamp: Timestamp,
                      eventType: String,
                      causedByUserId: Option[Long],
                      causedByAnonymousUser: Option[Boolean] = None,
                      causedByTemporaryUser: Option[Boolean] = None,
                      causedByPermanentUser: Option[Boolean] = None,
                      causedByUserText: Option[String],
                      parsingErrors: Seq[String] = Seq.empty[String],
                      // Specific fields
                      pageId: Option[Long],
                      oldTitle: String,
                      newTitle: String,
                      newTitlePrefix: String,
                      newTitleWithoutPrefix: String,
                      oldNamespace: Int,
                      oldNamespaceIsContent: Boolean,
                      newNamespace: Int,
                      newNamespaceIsContent: Boolean,
                      sourceLogId: Long,
                      sourceLogComment: String,
                      sourceLogParams: Map[String, String]
) extends Edge[(String, String, Int)] {
  override def fromKey: (String, String, Int) = (wikiDb, oldTitle, oldNamespace)
  override def toKey: (String, String, Int) = (wikiDb, newTitleWithoutPrefix, newNamespace)
  /**
   * Manage special cases:
   *  - move-events before 2014-09-25 store the redirect-page_id instead of the actually moved page_id.
   *    We therefore don't try to link move-events by id if the timestamp is before 2014-09-25.
   *      - https://www.mediawiki.org/wiki/Manual:Logging_table
   *      - https://gerrit.wikimedia.org/r/c/mediawiki/core/+/157872
   *      - https://www.mediawiki.org/wiki/MediaWiki_1.24/Roadmap#Schedule_for_the_deployments
   *    TODO: use the redirect page_id when generating the redirect page create event
   *
   *  - restore-events before 2016-05-05 were creating new page_id instead of recycling them.
   *    We don't try to link delete events nor restore events by id (to prevent having restore
   *    linked by id)for events before 2016-05-05, and we create a new page if there is an id
   *    that don't match the one before.
   *      - https://www.mediawiki.org/wiki/Manual:Page_table#page_id
   */
  val moveEventPageIdDate = "20140926000000"
  val restoreEventPageIdDate = "20160506000000"
  def hasKeyId: Boolean = {
    pageId.exists(_ > 0) &&
      !((eventType == "move") && timestamp.before(TimestampHelpers.makeMediawikiTimestamp(moveEventPageIdDate))) &&
      !(Set("delete", "restore").contains(eventType) && timestamp.before(TimestampHelpers.makeMediawikiTimestamp(restoreEventPageIdDate)))
  }
  def keyId: (String, Long) = (wikiDb, pageId.get)
}
