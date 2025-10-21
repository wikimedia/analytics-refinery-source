package org.wikimedia.analytics.refinery.job.mediawikihistory.user

import java.sql.Timestamp

import org.wikimedia.analytics.refinery.spark.utils.Edge

/**
  * This case class represents a user event object, by opposition
  * to a user state object. It extends [[Edge]] (for graph partitioning)
  * with [[fromKey]] defined as (WikiDb, oldUsername) and [[toKey]]
  * defined as (WikiDb, newUsername).
  */
case class UserEvent(
                      // Generic Fields
                      wikiDb: String,
                      timestamp: Timestamp,
                      eventType: String,
                      causedByUserId: Option[Long],
                      causedByUserCentralId: Option[Long],
                      causedByAnonymousUser: Option[Boolean] = None,
                      causedByTemporaryUser: Option[Boolean] = None,
                      causedByPermanentUser: Option[Boolean] = None,
                      causedByUserText: Option[String],
                      parsingErrors: Seq[String] = Seq.empty[String],
                      // Specific fields
                      oldUserText: String,
                      newUserText: String,
                      oldUserGroups: Seq[String] = Seq.empty[String],
                      newUserGroups: Seq[String] = Seq.empty[String],
                      newUserBlocks: Seq[String] = Seq.empty[String],
                      blockExpiration: Option[String],
                      createdBySelf: Boolean = false,
                      createdBySystem: Boolean = false,
                      createdByPeer: Boolean = false,
                      sourceLogId: Long,
                      sourceLogComment: String,
                      sourceLogParams: Map[String, String]
) extends Edge[(String, String)] {
  override def fromKey: (String, String) = (wikiDb, oldUserText)
  override def toKey: (String, String) = (wikiDb, newUserText)
}
