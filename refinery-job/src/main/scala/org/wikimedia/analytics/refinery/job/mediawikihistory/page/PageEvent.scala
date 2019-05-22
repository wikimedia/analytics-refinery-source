package org.wikimedia.analytics.refinery.job.mediawikihistory.page

import java.sql.Timestamp

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
}
