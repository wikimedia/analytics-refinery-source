package org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.wikimedia.analytics.refinery.core.TimestampHelpers
import org.wikimedia.analytics.refinery.job.mediawikihistory.page.PageState
import org.wikimedia.analytics.refinery.job.mediawikihistory.user.{UserEventBuilder, UserState}
import org.wikimedia.analytics.refinery.spark.utils.MapAccumulator

/**
  * This file defines case classes for denormalized ME Events objects.
  * Provides utility functions to update data and to read/write spark Rows.
  *
  * Page, user and revision information is split into subclasses
  * to overcome the 22-max parameters in case classes limitation of scala.
  */


case class MediawikiEventPageDetails(pageId: Option[Long] = None,
                                     pageArtificialId: Option[String] = None,
                                     pageTitleHistorical: Option[String] = None,
                                     pageTitle: Option[String] = None,
                                     pageNamespaceHistorical: Option[Int] = None,
                                     pageNamespaceIsContentHistorical: Option[Boolean] = None,
                                     pageNamespace: Option[Int] = None,
                                     pageNamespaceIsContent: Option[Boolean] = None,
                                     pageIsRedirect: Option[Boolean] = None,
                                     pageIsDeleted: Option[Boolean] = None,
                                     pageCreationTimestamp: Option[Timestamp] = None,
                                     pageFirstEditTimestamp: Option[Timestamp] = None,
                                     pageRevisionCount: Option[Long] = None,
                                     pageSecondsSincePreviousRevision: Option[Long] = None
                                    ) {
  def updateWithPageState(pageState: PageState) = {
    this.copy(
      pageArtificialId = pageState.pageArtificialId,
      pageTitleHistorical = Some(pageState.titleHistorical),
      pageTitle = Some(pageState.title),
      pageNamespaceHistorical = Some(pageState.namespaceHistorical),
      pageNamespaceIsContentHistorical = Some(pageState.namespaceIsContentHistorical),
      pageNamespace = Some(pageState.namespace),
      pageNamespaceIsContent = Some(pageState.namespaceIsContent),
      pageIsRedirect = pageState.isRedirect,
      pageIsDeleted = Some(pageState.isDeleted),
      pageCreationTimestamp = pageState.pageCreationTimestamp,
      pageFirstEditTimestamp = pageState.pageFirstEditTimestamp
    )
  }
}

case class MediawikiEventUserDetails(userId: Option[Long] = None,
                                     userTextHistorical: Option[String] = None,
                                     userText: Option[String] = None,
                                     userBlocksHistorical: Option[Seq[String]] = None,
                                     userBlocks: Option[Seq[String]] = None,
                                     userGroupsHistorical: Option[Seq[String]] = None,
                                     userGroups: Option[Seq[String]] = None,
                                     userIsBotByHistorical: Option[Seq[String]] = None,
                                     userIsBotBy: Option[Seq[String]] = None,
                                     userIsCreatedBySelf: Option[Boolean] = None,
                                     userIsCreatedBySystem: Option[Boolean] = None,
                                     userIsCreatedByPeer: Option[Boolean] = None,
                                     userIsAnonymous: Option[Boolean] = None,
                                     userRegistrationTimestamp: Option[Timestamp] = None,
                                     userCreationTimestamp: Option[Timestamp] = None,
                                     userFirstEditTimestamp: Option[Timestamp] = None,
                                     // Next two fields are used in event_user
                                     userRevisionCount: Option[Long] = None,
                                     userSecondsSincePreviousRevision: Option[Long] = None
                                    ) {
  def updateWithUserState(userState: UserState) = this.copy(
      userId = Some(userState.userId),
      userTextHistorical = Some(userState.userTextHistorical),
      userText = Some(userState.userText),
      userBlocksHistorical = Some(userState.userBlocksHistorical),
      userBlocks = Some(userState.userBlocks),
      userGroupsHistorical = Some(userState.userGroupsHistorical),
      userGroups = Some(userState.userGroups),
      userIsBotByHistorical = Some(userState.isBotByHistorical),
      userIsBotBy = Some(userState.isBotBy),
      userIsCreatedBySelf = Some(userState.createdBySelf),
      userIsCreatedBySystem = Some(userState.createdBySystem),
      userIsCreatedByPeer = Some(userState.createdByPeer),
      userIsAnonymous = Some(userState.anonymous),
      userRegistrationTimestamp = userState.userRegistrationTimestamp,
      userCreationTimestamp = userState.userCreationTimestamp,
      userFirstEditTimestamp = userState.userFirstEditTimestamp
    )
}

case class MediawikiEventRevisionDetails(revId: Option[Long] = None,
                                         revParentId: Option[Long] = None,
                                         revMinorEdit: Option[Boolean] = None,
                                         revDeletedParts: Option[Seq[String]] = None,
                                         revDeletedPartsAreSuppressed: Option[Boolean] = None,
                                         revTextBytes: Option[Long] = None,
                                         revTextBytesDiff: Option[Long] = None,
                                         revTextSha1: Option[String] = None,
                                         revContentModel: Option[String] = None,
                                         revContentFormat: Option[String] = None,
                                         revIsDeletedByPageDeletion: Option[Boolean] = None,
                                         revDeletedByPageDeletionTimestamp: Option[Timestamp] = None,
                                         revIsIdentityReverted: Option[Boolean] = None,
                                         revFirstIdentityRevertingRevisionId: Option[Long] = None,
                                         revSecondsToIdentityRevert: Option[Long] = None,
                                         revIsIdentityRevert: Option[Boolean] = None,
                                         revIsFromBeforePageCreation: Option[Boolean] = None,
                                         revTags: Option[Seq[String]] = None
                                        )

object MediawikiEventRevisionDetails {
  private val deletionFlags = Seq(
    (1, "text"),
    (2, "comment"),
    (4, "user")
  )

  def getRevDeletedParts(revDeleted: Int): Seq[String] = {
    deletionFlags.flatMap { case (flag, hiddenPart) =>
      if ((revDeleted & flag) == flag) {
        Seq(hiddenPart)
      } else {
        Seq.empty
      }
    }
  }

  def getRevDeletedPartsAreSuppressed(revDeleted: Int): Boolean = {
    (revDeleted & 8) == 8
  }
}

case class MediawikiEvent(
                           wikiDb: String,
                           eventEntity: String,
                           eventType: String,
                           eventTimestamp: Option[Timestamp] = None,
                           eventComment: Option[String] = None,
                           eventErrors: Seq[String] = Seq.empty,
                           eventUserDetails: MediawikiEventUserDetails = new MediawikiEventUserDetails,
                           pageDetails: MediawikiEventPageDetails = new MediawikiEventPageDetails,
                           userDetails: MediawikiEventUserDetails = new MediawikiEventUserDetails,
                           revisionDetails: MediawikiEventRevisionDetails = new MediawikiEventRevisionDetails
                      ) {
  def toRow: Row = Row(
    wikiDb,
    eventEntity,
    eventType,
    eventTimestamp.map(_.toString).orNull,
    //eventTimestamp.orNull,
    eventComment.orNull,
    eventUserDetails.userId.orNull,
    eventUserDetails.userTextHistorical.orNull,
    eventUserDetails.userText.orNull,
    eventUserDetails.userBlocksHistorical.orNull,
    eventUserDetails.userBlocks.orNull,
    eventUserDetails.userGroupsHistorical.orNull,
    eventUserDetails.userGroups.orNull,
    eventUserDetails.userIsBotByHistorical.orNull,
    eventUserDetails.userIsBotBy.orNull,
    eventUserDetails.userIsCreatedBySelf.orNull,
    eventUserDetails.userIsCreatedBySystem.orNull,
    eventUserDetails.userIsCreatedByPeer.orNull,
    eventUserDetails.userIsAnonymous.orNull,
    eventUserDetails.userRegistrationTimestamp.map(_.toString).orNull,
    //eventUserDetails.userRegistrationTimestamp.orNull,
    eventUserDetails.userCreationTimestamp.map(_.toString).orNull,
    //eventUserDetails.userCreationTimestamp.orNull,
    eventUserDetails.userFirstEditTimestamp.map(_.toString).orNull,
    //eventUserDetails.userFirstEditTimestamp.orNull,
    eventUserDetails.userRevisionCount.orNull,
    eventUserDetails.userSecondsSincePreviousRevision.orNull,

    pageDetails.pageId.orNull,
    pageDetails.pageArtificialId.orNull,
    pageDetails.pageTitleHistorical.orNull,
    pageDetails.pageTitle.orNull,
    pageDetails.pageNamespaceHistorical.orNull,
    pageDetails.pageNamespaceIsContentHistorical.orNull,
    pageDetails.pageNamespace.orNull,
    pageDetails.pageNamespaceIsContent.orNull,
    pageDetails.pageIsRedirect.orNull,
    pageDetails.pageIsDeleted.orNull,
    pageDetails.pageCreationTimestamp.map(_.toString).orNull,
    //pageDetails.pageCreationTimestamp.orNull,
    pageDetails.pageFirstEditTimestamp.map(_.toString).orNull,
    //pageDetails.pageFirstEditTimestamp.orNull,
    pageDetails.pageRevisionCount.orNull,
    pageDetails.pageSecondsSincePreviousRevision.orNull,

    userDetails.userId.orNull,
    userDetails.userTextHistorical.orNull,
    userDetails.userText.orNull,
    userDetails.userBlocksHistorical.orNull,
    userDetails.userBlocks.orNull,
    userDetails.userGroupsHistorical.orNull,
    userDetails.userGroups.orNull,
    userDetails.userIsBotByHistorical.orNull,
    userDetails.userIsBotBy.orNull,
    userDetails.userIsCreatedBySelf.orNull,
    userDetails.userIsCreatedBySystem.orNull,
    userDetails.userIsCreatedByPeer.orNull,
    userDetails.userIsAnonymous.orNull,
    userDetails.userRegistrationTimestamp.map(_.toString).orNull,
    //userDetails.userRegistrationTimestamp.orNull,
    userDetails.userCreationTimestamp.map(_.toString).orNull,
    //userDetails.userCreationTimestamp.orNull,
    userDetails.userFirstEditTimestamp.map(_.toString).orNull,
    //userDetails.userFirstEditTimestamp.orNull,

    revisionDetails.revId.orNull,
    revisionDetails.revParentId.orNull,
    revisionDetails.revMinorEdit.orNull,
    revisionDetails.revDeletedParts.orNull,
    revisionDetails.revDeletedPartsAreSuppressed.orNull,
    revisionDetails.revTextBytes.orNull,
    revisionDetails.revTextBytesDiff.orNull,
    revisionDetails.revTextSha1.orNull,
    revisionDetails.revContentModel.orNull,
    revisionDetails.revContentFormat.orNull,
    revisionDetails.revIsDeletedByPageDeletion.orNull,
    revisionDetails.revDeletedByPageDeletionTimestamp.map(_.toString).orNull,
    //revisionDetails.revDeletedTimestamp.orNull,
    revisionDetails.revIsIdentityReverted.orNull,
    revisionDetails.revFirstIdentityRevertingRevisionId.orNull,
    revisionDetails.revSecondsToIdentityRevert.orNull,
    revisionDetails.revIsIdentityRevert.orNull,
    revisionDetails.revIsFromBeforePageCreation.orNull,
    revisionDetails.revTags.orNull
  )
  def toTSVLine: String = {
      // Explicit typing to Seq[Any] needed to accept coalescing values to null
      val columns: Seq[Any] = Seq(
          wikiDb,
          eventEntity,
          eventType,
          eventTimestamp.map(_.toString).orNull,
          MediawikiEvent.escape(eventComment.orNull),
          eventUserDetails.userId.orNull,
          MediawikiEvent.escape(eventUserDetails.userTextHistorical.orNull),
          MediawikiEvent.escape(eventUserDetails.userText.orNull),
          MediawikiEvent.formatArray(eventUserDetails.userBlocksHistorical.orNull),
          MediawikiEvent.formatArray(eventUserDetails.userBlocks.orNull),
          MediawikiEvent.formatArray(eventUserDetails.userGroupsHistorical.orNull),
          MediawikiEvent.formatArray(eventUserDetails.userGroups.orNull),
          MediawikiEvent.formatArray(eventUserDetails.userIsBotByHistorical.orNull),
          MediawikiEvent.formatArray(eventUserDetails.userIsBotBy.orNull),
          eventUserDetails.userIsCreatedBySelf.orNull,
          eventUserDetails.userIsCreatedBySystem.orNull,
          eventUserDetails.userIsCreatedByPeer.orNull,
          eventUserDetails.userIsAnonymous.orNull,
          eventUserDetails.userRegistrationTimestamp.map(_.toString).orNull,
          eventUserDetails.userCreationTimestamp.map(_.toString).orNull,
          eventUserDetails.userFirstEditTimestamp.map(_.toString).orNull,
          eventUserDetails.userRevisionCount.orNull,
          eventUserDetails.userSecondsSincePreviousRevision.orNull,
          pageDetails.pageId.orNull,
          // Note pageArtificialId is not printed.
          MediawikiEvent.escape(pageDetails.pageTitleHistorical.orNull),
          MediawikiEvent.escape(pageDetails.pageTitle.orNull),
          pageDetails.pageNamespaceHistorical.orNull,
          pageDetails.pageNamespaceIsContentHistorical.orNull,
          pageDetails.pageNamespace.orNull,
          pageDetails.pageNamespaceIsContent.orNull,
          pageDetails.pageIsRedirect.orNull,
          pageDetails.pageIsDeleted.orNull,
          pageDetails.pageCreationTimestamp.map(_.toString).orNull,
          pageDetails.pageFirstEditTimestamp.map(_.toString).orNull,
          pageDetails.pageRevisionCount.orNull,
          pageDetails.pageSecondsSincePreviousRevision.orNull,
          userDetails.userId.orNull,
          MediawikiEvent.escape(userDetails.userTextHistorical.orNull),
          MediawikiEvent.escape(userDetails.userText.orNull),
          MediawikiEvent.formatArray(userDetails.userBlocksHistorical.orNull),
          MediawikiEvent.formatArray(userDetails.userBlocks.orNull),
          MediawikiEvent.formatArray(userDetails.userGroupsHistorical.orNull),
          MediawikiEvent.formatArray(userDetails.userGroups.orNull),
          MediawikiEvent.formatArray(userDetails.userIsBotByHistorical.orNull),
          MediawikiEvent.formatArray(userDetails.userIsBotBy.orNull),
          userDetails.userIsCreatedBySelf.orNull,
          userDetails.userIsCreatedBySystem.orNull,
          userDetails.userIsCreatedByPeer.orNull,
          userDetails.userIsAnonymous.orNull,
          userDetails.userRegistrationTimestamp.map(_.toString).orNull,
          userDetails.userCreationTimestamp.map(_.toString).orNull,
          userDetails.userFirstEditTimestamp.map(_.toString).orNull,
          revisionDetails.revId.orNull,
          revisionDetails.revParentId.orNull,
          revisionDetails.revMinorEdit.orNull,
          MediawikiEvent.formatArray(revisionDetails.revDeletedParts.orNull),
          revisionDetails.revDeletedPartsAreSuppressed.orNull,
          revisionDetails.revTextBytes.orNull,
          revisionDetails.revTextBytesDiff.orNull,
          revisionDetails.revTextSha1.orNull,
          revisionDetails.revContentModel.orNull,
          revisionDetails.revContentFormat.orNull,
          revisionDetails.revIsDeletedByPageDeletion.orNull,
          revisionDetails.revDeletedByPageDeletionTimestamp.map(_.toString).orNull,
          revisionDetails.revIsIdentityReverted.orNull,
          revisionDetails.revFirstIdentityRevertingRevisionId.orNull,
          revisionDetails.revSecondsToIdentityRevert.orNull,
          revisionDetails.revIsIdentityRevert.orNull,
          revisionDetails.revIsFromBeforePageCreation.orNull,
          MediawikiEvent.formatArray(revisionDetails.revTags.orNull)
      )
      columns.map((v: Any) => if (v == null) "" else v.toString).mkString("\t")
  }

  def textBytesDiff(value: Option[Long]) = this.copy(revisionDetails = this.revisionDetails.copy(revTextBytesDiff = value))
  def IsRevisionDeletedByPageDeletion(deleteTimestamp: Option[Timestamp]) = this.copy(
    revisionDetails = this.revisionDetails.copy(
      revIsDeletedByPageDeletion = Some(true),
      revDeletedByPageDeletionTimestamp = deleteTimestamp
    )
  )
  def isIdentityRevert = this.copy(revisionDetails = this.revisionDetails.copy(revIsIdentityRevert = Some(true)))
  def isIdentityReverted(
                          revertingRevisionId: Option[Long],
                          revTimeToRevert: Option[Long]
                        ) = this.copy(revisionDetails = this.revisionDetails.copy(
    revIsIdentityReverted = Some(true),
    revFirstIdentityRevertingRevisionId = revertingRevisionId,
    revSecondsToIdentityRevert = revTimeToRevert))
  def updateWithUserPreviousRevision(userPreviousRevision: Option[MediawikiEvent]) = this.copy(
    eventUserDetails = this.eventUserDetails.copy(
      userRevisionCount = Some(userPreviousRevision.map(_.eventUserDetails.userRevisionCount.getOrElse(0L)).getOrElse(0L) + 1),
      userSecondsSincePreviousRevision =
        TimestampHelpers.getTimestampDifference(this.eventTimestamp, userPreviousRevision.map(_.eventTimestamp).getOrElse(None))
    ))
  def updateWithPagePreviousRevision(pagePreviousRevision: Option[MediawikiEvent]) = this.copy(
    pageDetails = this.pageDetails.copy(
      pageRevisionCount = Some(pagePreviousRevision.map(_.pageDetails.pageRevisionCount.getOrElse(0L)).getOrElse(0L) + 1),
      pageSecondsSincePreviousRevision =
        TimestampHelpers.getTimestampDifference(this.eventTimestamp, pagePreviousRevision.map(_.eventTimestamp).getOrElse(None))
    ))
  def updateEventUserDetails(userState: UserState) = this.copy(
    eventUserDetails = this.eventUserDetails.updateWithUserState(userState)
  )
  def updatePageDetails(pageState: PageState) = {
    // Using pageCreationTimestamp with page events (not pageFirstEditTimestamp)
    val beforeCreation = {
      (eventTimestamp, pageState.pageCreationTimestamp) match {
        case (None, None) => false
        case (None, Some(_)) => true
        case (Some(_), None) => false
        case (Some(e), Some(p)) => e.before(p)
      }
    }
    this.copy(
      pageDetails = this.pageDetails.updateWithPageState(pageState),
      revisionDetails = this.revisionDetails.copy(revIsFromBeforePageCreation = Some(beforeCreation))
    )
  }
}

object MediawikiEvent {

  val schema = StructType(
    Seq(
      StructField("wiki_db", StringType, nullable = false),
      StructField("event_entity", StringType, nullable = false),
      StructField("event_type", StringType, nullable = false),
      StructField("event_timestamp", StringType, nullable = true),
      //StructField("event_timestamp", TimestampType, nullable = true),
      StructField("event_comment", StringType, nullable = true),
      StructField("event_user_id", LongType, nullable = true),
      StructField("event_user_text_historical", StringType, nullable = true),
      StructField("event_user_text", StringType, nullable = true),
      StructField("event_user_blocks_historical", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("event_user_blocks", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("event_user_groups_historical", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("event_user_groups", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("event_user_is_bot_by_historical", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("event_user_is_bot_by", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("event_user_is_created_by_self", BooleanType, nullable = true),
      StructField("event_user_is_created_by_system", BooleanType, nullable = true),
      StructField("event_user_is_created_by_peer", BooleanType, nullable = true),
      StructField("event_user_is_anonymous", BooleanType, nullable = true),
      StructField("event_user_registration_timestamp", StringType, nullable = true),
      //StructField("event_user_registration_timestamp", TimestampType, nullable = true),
      StructField("event_user_creation_timestamp", StringType, nullable = true),
      //StructField("event_user_creation_timestamp", TimestampType, nullable = true),
      StructField("event_user_first_edit_timestamp", StringType, nullable = true),
      //StructField("event_user_first_edit_timestamp", TimestampType, nullable = true),
      StructField("event_user_revision_count", LongType, nullable = true),
      StructField("event_user_seconds_since_previous_revision", LongType, nullable = true),

      StructField("page_id", LongType, nullable = true),
      StructField("page_artificial_id", StringType, nullable = true),
      StructField("page_title_historical", StringType, nullable = true),
      StructField("page_title", StringType, nullable = true),
      StructField("page_namespace_historical", IntegerType, nullable = true),
      StructField("page_namespace_is_content_historical", BooleanType, nullable = true),
      StructField("page_namespace", IntegerType, nullable = true),
      StructField("page_namespace_is_content", BooleanType, nullable = true),
      StructField("page_is_redirect", BooleanType, nullable = true),
      StructField("page_is_deleted", BooleanType, nullable = true),
      StructField("page_creation_timestamp", StringType, nullable = true),
      //StructField("page_creation_timestamp", TimestampType, nullable = true),
      StructField("page_first_edit_timestamp", StringType, nullable = true),
      //StructField("page_first_edit_timestamp", TimestampType, nullable = true),
      StructField("page_revision_count", LongType, nullable = true),
      StructField("page_seconds_since_previous_revision", LongType, nullable = true),

      StructField("user_id", LongType, nullable = true),
      StructField("user_text_historical", StringType, nullable = true),
      StructField("user_text", StringType, nullable = true),
      StructField("user_blocks_historical", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("user_blocks", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("user_groups_historical", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("user_groups", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("user_is_bot_by_historical", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("user_is_bot_by", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("user_is_created_by_self", BooleanType, nullable = true),
      StructField("user_is_created_by_system", BooleanType, nullable = true),
      StructField("user_is_created_by_peer", BooleanType, nullable = true),
      StructField("user_is_anonymous", BooleanType, nullable = true),
      StructField("user_registration_timestamp", StringType, nullable = true),
      //StructField("user_registration_timestamp", TimestampType, nullable = true),
      StructField("user_creation_timestamp", StringType, nullable = true),
      //StructField("user_creation_timestamp", TimestampType, nullable = true),
      StructField("user_first_edit_timestamp", StringType, nullable = true),
      //StructField("user_first_edit_timestamp", TimestampType, nullable = true),

      StructField("revision_id", LongType, nullable = true),
      StructField("revision_parent_id", LongType, nullable = true),
      StructField("revision_minor_edit", BooleanType, nullable = true),
      StructField("revision_deleted_parts", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("revision_deleted_parts_are_suppressed", BooleanType, nullable = true),
      StructField("revision_text_bytes", LongType, nullable = true),
      StructField("revision_text_bytes_diff", LongType, nullable = true),
      StructField("revision_text_sha1", StringType, nullable = true),
      StructField("revision_content_model", StringType, nullable = true),
      StructField("revision_content_format", StringType, nullable = true),
      StructField("revision_is_deleted_by_page_deletion", BooleanType, nullable = true),
      StructField("revision_deleted_by_page_deletion_timestamp", StringType, nullable = true),
      //StructField("revision_deleted_timestamp", TimestampType, nullable = true),
      StructField("revision_is_identity_reverted", BooleanType, nullable = true),
      StructField("revision_first_identity_reverting_revision_id", LongType, nullable = true),
      StructField("revision_seconds_to_identity_revert", LongType, nullable = true),
      StructField("revision_is_identity_revert", BooleanType, nullable = true),
      StructField("revision_is_from_before_page_creation", BooleanType, nullable = true),
      StructField("revision_tags", ArrayType(StringType, containsNull = true), nullable = true)
    )
  )

  def fromRow(row: Row): MediawikiEvent =
    new MediawikiEvent(
      wikiDb = row.getString(0),
      eventEntity = row.getString(1),
      eventType = row.getString(2),
      eventTimestamp = if (row.isNullAt(3)) None else Some(Timestamp.valueOf(row.getString(3))),
      eventComment = Option(row.getString(4)),
      eventUserDetails = new MediawikiEventUserDetails(
        userId = if (row.isNullAt(5)) None else Some(row.getLong(5)),
        userTextHistorical = Option(row.getString(6)),
        userText = Option(row.getString(7)),
        userBlocksHistorical = Option(row.getSeq[String](8)),
        userBlocks = Option(row.getSeq[String](9)),
        userGroupsHistorical = Option(row.getSeq[String](10)),
        userGroups = Option(row.getSeq[String](11)),
        userIsBotByHistorical = Option(row.getSeq[String](12)),
        userIsBotBy = Option(row.getSeq[String](13)),
        userIsCreatedBySelf = if (row.isNullAt(14)) None else Some(row.getBoolean(14)),
        userIsCreatedBySystem = if (row.isNullAt(15)) None else Some(row.getBoolean(15)),
        userIsCreatedByPeer = if (row.isNullAt(16)) None else Some(row.getBoolean(16)),
        userIsAnonymous = if (row.isNullAt(17)) None else Some(row.getBoolean(17)),
        userRegistrationTimestamp = if (row.isNullAt(18)) None else Some(Timestamp.valueOf(row.getString(18))),
        userCreationTimestamp = if (row.isNullAt(19)) None else Some(Timestamp.valueOf(row.getString(19))),
        userFirstEditTimestamp = if (row.isNullAt(20)) None else Some(Timestamp.valueOf(row.getString(20))),
        userRevisionCount = if (row.isNullAt(21)) None else Some(row.getLong(21)),
        userSecondsSincePreviousRevision = if (row.isNullAt(22)) None else Some(row.getLong(22))
      ),
      pageDetails = new MediawikiEventPageDetails(
        pageId = if (row.isNullAt(23)) None else Some(row.getLong(23)),
        pageArtificialId = Option(row.getString(24)),
        pageTitleHistorical = Option(row.getString(25)),
        pageTitle = Option(row.getString(26)),
        pageNamespaceHistorical = if (row.isNullAt(27)) None else Some(row.getInt(27)),
        pageNamespaceIsContentHistorical = if (row.isNullAt(28)) None else Some(row.getBoolean(28)),
        pageNamespace = if (row.isNullAt(29)) None else Some(row.getInt(29)),
        pageNamespaceIsContent = if (row.isNullAt(30)) None else Some(row.getBoolean(30)),
        pageIsRedirect = if (row.isNullAt(31)) None else Some(row.getBoolean(31)),
        pageIsDeleted = if (row.isNullAt(32)) None else Some(row.getBoolean(32)),
        pageCreationTimestamp = if (row.isNullAt(33)) None else Some(Timestamp.valueOf(row.getString(33))),
        pageFirstEditTimestamp = if (row.isNullAt(34)) None else Some(Timestamp.valueOf(row.getString(34))),
        pageRevisionCount = if (row.isNullAt(35)) None else Some(row.getLong(35)),
        pageSecondsSincePreviousRevision = if (row.isNullAt(36)) None else Some(row.getLong(36))
      ),
      userDetails = new MediawikiEventUserDetails(
        userId = if (row.isNullAt(37)) None else Some(row.getLong(37)),
        userTextHistorical = Option(row.getString(38)),
        userText = Option(row.getString(39)),
        userBlocksHistorical = Option(row.getSeq[String](40)),
        userBlocks = Option(row.getSeq[String](41)),
        userGroupsHistorical = Option(row.getSeq[String](42)),
        userGroups = Option(row.getSeq[String](43)),
        userIsBotByHistorical = Option(row.getSeq[String](44)),
        userIsBotBy = Option(row.getSeq[String](45)),
        userIsCreatedBySelf = if (row.isNullAt(46)) None else Some(row.getBoolean(46)),
        userIsCreatedBySystem = if (row.isNullAt(47)) None else Some(row.getBoolean(47)),
        userIsCreatedByPeer = if (row.isNullAt(48)) None else Some(row.getBoolean(48)),
        userIsAnonymous = if (row.isNullAt(49)) None else Some(row.getBoolean(49)),
        userRegistrationTimestamp = if (row.isNullAt(50)) None else Some(Timestamp.valueOf(row.getString(50))),
        userCreationTimestamp = if (row.isNullAt(51)) None else Some(Timestamp.valueOf(row.getString(51))),
        userFirstEditTimestamp = if (row.isNullAt(52)) None else Some(Timestamp.valueOf(row.getString(52)))
        // userRevisionCount -- Not relevant, user events only
        // userSecondsSincePreviousRevision -- ie
      ),
      revisionDetails = new MediawikiEventRevisionDetails(
        revId = if (row.isNullAt(53)) None else Some(row.getLong(53)),
        revParentId = if (row.isNullAt(54)) None else Some(row.getLong(54)),
        revMinorEdit = if (row.isNullAt(55)) None else Some(row.getBoolean(55)),
        revDeletedParts = Option(row.getSeq[String](56)),
        revDeletedPartsAreSuppressed = if (row.isNullAt(57)) None else Some(row.getBoolean(57)),
        revTextBytes = if (row.isNullAt(58)) None else Some(row.getLong(58)),
        revTextBytesDiff = if (row.isNullAt(59)) None else Some(row.getLong(59)),
        revTextSha1 = Option(row.getString(60)),
        revContentModel = Option(row.getString(61)),
        revContentFormat = Option(row.getString(62)),
        revIsDeletedByPageDeletion = if (row.isNullAt(63)) None else Some(row.getBoolean(63)),
        revDeletedByPageDeletionTimestamp = if (row.isNullAt(64)) None else Some(Timestamp.valueOf(row.getString(64))),
        revIsIdentityReverted = if (row.isNullAt(65)) None else Some(row.getBoolean(65)),
        revFirstIdentityRevertingRevisionId = if (row.isNullAt(66)) None else Some(row.getLong(66)),
        revSecondsToIdentityRevert = if (row.isNullAt(67)) None else Some(row.getLong(67)),
        revIsIdentityRevert = if (row.isNullAt(68)) None else Some(row.getBoolean(68)),
        revIsFromBeforePageCreation = if (row.isNullAt(69)) None else Some(row.getBoolean(69)),
        revTags = Option(row.getSeq[String](70))
      )
    )

  /* select like this and then map this function:
   *   0  wiki_db,
   *   1  rev_timestamp,
   *   2  comment_text,
   *   3  actor_user,
   *   4  actor_name,
   *   5  actor_is_anon,
   *   6  rev_page,
   *   7  rev_id,
   *   8  rev_parent_id,
   *   9  rev_minor_edit,
   *  10  rev_deleted,
   *  11  rev_len,
   *  12  rev_sha1,
   *  13  rev_content_model,
   *  14  rev_content_format,
   *  15  rev_tags
   *
   *  NOTE: actor_name is not nullable, so it's only null when the join failed
   *          row.isNullAt(4) means the join failed
   *        actor_user is null when actor is anon
   *          row.isNullAt(3) and the join succeeded (not row.isNullAt(4))
   */
  def fromRevisionRow(row: Row): MediawikiEvent = {
    val actorUser = if (row.isNullAt(3) || row.isNullAt(4)) None else Some(row.getLong(3))
    val actorName = row.getString(4)
    val actorIsAnon = if (row.isNullAt(5)) None else Some(row.getBoolean(5))
    val textBytes = if (row.isNullAt(11)) None else Some(row.getLong(11))
    val revDeletedFlag = if (row.isNullAt(10)) None else Some(row.getInt(10))
    new MediawikiEvent(
      wikiDb = row.getString(0),
      eventEntity = "revision",
      eventType = "create",
      eventTimestamp = TimestampHelpers.makeMediawikiTimestampOption(row.getString(1)),
      eventComment = Option(row.getString(2)),
      eventUserDetails = new MediawikiEventUserDetails(
        userId = actorUser,
        // userText should be the same as historical if user is anonymous
        // https://phabricator.wikimedia.org/T206883
        userText = if (actorIsAnon.getOrElse(false)) Option(actorName) else None,
        userTextHistorical = Option(actorName),
        userIsAnonymous = actorIsAnon,
        // Provide partial historical value based on usertext as userGroups are not available
        // No need to provide current value as only anonymous-usertext is propagated
        userIsBotByHistorical = Some(UserEventBuilder.isBotBy(actorName, Seq.empty))
      ),
      pageDetails = new MediawikiEventPageDetails(
        pageId = if (row.isNullAt(6)) None else Some(row.getLong(6))
        // pageTitleHistorical: need page history
        // pageTitle: need page history
        // pageNamespaceHistorical: need page history
        // pageNamespace: need page history
        // pageCreationTimestamp: need page history
      ),
      userDetails = new MediawikiEventUserDetails(
        // userId: Not Applicable (not a user centered event)
        // userTextHistorical: Not Applicable (not a user centered event)
        // userText = Not Applicable (not a user centered event)
        // userBlocksHistorical: need user history
        // userBlocks: need user history
        // userGroupsHistorical: need user history
        // userGroups: need user history
        // userCreationTimestamp: need user history
      ),
      revisionDetails = new MediawikiEventRevisionDetails(
        revId = if (row.isNullAt(7)) None else Some(row.getLong(7)),
        revParentId = if (row.isNullAt(8)) None else Some(row.getLong(8)),
        revMinorEdit = if (row.isNullAt(9)) None else Some(row.getBoolean(9)),
        revDeletedParts = revDeletedFlag.map(
          MediawikiEventRevisionDetails.getRevDeletedParts),
        revDeletedPartsAreSuppressed = revDeletedFlag.map(
          MediawikiEventRevisionDetails.getRevDeletedPartsAreSuppressed),
        revTextBytes = textBytes,
        // Initializing revTextBytesDiff to current textBytes, will be updated later
        revTextBytesDiff = textBytes,
        revTextSha1 = Option(row.getString(12)),
        revContentModel = Option(row.getString(13)),
        revContentFormat = Option(row.getString(14)),
        revIsDeletedByPageDeletion = Some(false),
        revIsIdentityReverted = Some(false),
        revSecondsToIdentityRevert = None,
        revIsIdentityRevert = Some(false),
        revIsFromBeforePageCreation = Some(false),
        revTags = Option(row.getSeq[String](15))
        // revDeletedTimestamp: NA
        // revRevertedTimestamp: need self join,
      )
    )
  }

  /* select like this and then map this function:
   *   0  wiki_db,
   *   1  ar_timestamp,
   *   2  comment_text,
   *   3  actor_user,
   *   4  actor_name,
   *   5  actor_is_anon,
   *   6  ar_page_id,
   *   7  ar_title,
   *   8  ar_namespace,
   *   9  ar_namespace_is_content,
   *  10  ar_rev_id,
   *  11  ar_parent_id,
   *  12  ar_minor_edit,
   *  13  ar_deleted,
   *  14  ar_len,
   *  15  ar_sha1,
   *  16  ar_content_model,
   *  17  ar_content_format,
   *  18  ar_tags
   *
   *  NOTE: actor_name is not nullable, so it's only null when the join failed
   *          row.isNullAt(4) means the join failed
   *        actor_user is null when actor is anon
   *          row.isNullAt(3) and the join succeeded (not row.isNullAt(4))
   */
  def fromArchiveRow(row: Row): MediawikiEvent = {
    val actorUser = if (row.isNullAt(3) || row.isNullAt(4)) None else Some(row.getLong(3))
    val actorName = row.getString(4)
    val actorIsAnon = if (row.isNullAt(5)) None else Some(row.getBoolean(5))
    val textBytes = if (row.isNullAt(14)) None else Some(row.getLong(14))
    val revDeletedFlag = if (row.isNullAt(13)) None else Some(row.getInt(13))
    MediawikiEvent(
      wikiDb = row.getString(0),
      eventEntity = "revision",
      eventType = "create",
      eventTimestamp = TimestampHelpers.makeMediawikiTimestampOption(row.getString(1)),
      eventComment = Option(row.getString(2)),
      eventUserDetails = new MediawikiEventUserDetails(
        userId = actorUser,
        // Anonymous users have same current and historical userText
        // https://phabricator.wikimedia.org/T206883
        userText = if (actorIsAnon.getOrElse(false)) Option(actorName) else None,
        userTextHistorical = Option(actorName),
        userIsAnonymous = actorIsAnon,
        // Provide partial historical value based on usertext as userGroups are not available
        // No need to provide current value as only anonymous-usertext is propagated
        userIsBotByHistorical = Some(UserEventBuilder.isBotBy(actorName, Seq.empty))
      ),
      pageDetails = new MediawikiEventPageDetails(
        pageId = if (row.isNullAt(6)) None else Some(row.getLong(6)),
        pageTitleHistorical = Option(row.getString(7)),
        pageNamespaceHistorical = if (row.isNullAt(8)) None else Some(row.getInt(8)),
        pageNamespaceIsContentHistorical = if (row.isNullAt(9)) None else Some(row.getBoolean(9))
      ),
      userDetails = new MediawikiEventUserDetails(
        // userId: NA
        // userTextHistorical: NA
        // userText = NA
        // userBlocksHistorical: need user history
        // userBlocks: need user history
        // userGroupsHistorical: need user history
        // userGroups: need user history
        // userCreationTimestamp: need user history
      ),
      revisionDetails = new MediawikiEventRevisionDetails(
        revId = if (row.isNullAt(10)) None else Some(row.getLong(10)),
        revParentId = if (row.isNullAt(11)) None else Some(row.getLong(11)),
        revMinorEdit = if (row.isNullAt(12)) None else Some(row.getBoolean(12)),
        revDeletedParts = revDeletedFlag.map(
          MediawikiEventRevisionDetails.getRevDeletedParts),
        revDeletedPartsAreSuppressed = revDeletedFlag.map(
          MediawikiEventRevisionDetails.getRevDeletedPartsAreSuppressed),
        revTextBytes = textBytes,
        // Initializing revTextBytesDiff to current textBytes, will be updated later
        revTextBytesDiff = textBytes,
        revTextSha1 = Option(row.getString(15)),
        revContentModel = Option(row.getString(16)),
        revContentFormat = Option(row.getString(17)),
        revIsDeletedByPageDeletion = Option(true),
        revIsIdentityReverted = Some(false),
        revSecondsToIdentityRevert = None,
        revIsIdentityRevert = Some(false),
        revIsFromBeforePageCreation = Some(false),
        revTags = Option(row.getSeq[String](18))
        // revRevertedTimestamp: need self join

      )
    )
  }

  def fromUserState(userState: UserState): MediawikiEvent = {
    MediawikiEvent(
      wikiDb = userState.wikiDb,
      eventEntity = "user",
      eventType = userState.causedByEventType,
      eventTimestamp = userState.startTimestamp,
      eventComment = None,
      eventUserDetails = new MediawikiEventUserDetails(
        userId = userState.causedByUserId,
        userTextHistorical = userState.causedByUserText,
        // Make historical username current one for anonymous users
        userText = if (userState.causedByAnonymousUser.getOrElse(false)) userState.causedByUserText else None,
        userIsAnonymous = userState.causedByAnonymousUser,
        // Provide partial historical value based on usertext as userGroups are not available
        // No need to provide current value as only anonymous-usertext is propagated
        userIsBotByHistorical = Some(UserEventBuilder.isBotBy(userState.causedByUserText.getOrElse(""), Seq.empty))
      ),
      pageDetails = new MediawikiEventPageDetails(
        // pageId: NA
        // pageTitleHistorical: NA
        // pageTitle: NA
        // pageNamespaceHistorical: NA
        // pageNamespace: NA
        // pageCreationTimestamp: NA
      ),
      userDetails = new MediawikiEventUserDetails(
        userId = Some(userState.userId),
        userTextHistorical = Some(userState.userTextHistorical),
        userText = Some(userState.userText),
        userBlocksHistorical = Some(userState.userBlocksHistorical),
        userBlocks = Some(userState.userBlocks),
        userGroupsHistorical = Some(userState.userGroupsHistorical),
        userGroups = Some(userState.userGroups),
        userIsBotByHistorical = Some(userState.isBotByHistorical),
        userIsBotBy = Some(userState.isBotBy),
        userIsCreatedBySelf = Some(userState.createdBySelf),
        userIsCreatedBySystem = Some(userState.createdBySystem),
        userIsCreatedByPeer = Some(userState.createdByPeer),
        userIsAnonymous = Some(userState.anonymous),
        userRegistrationTimestamp = userState.userRegistrationTimestamp,
        userCreationTimestamp = userState.userCreationTimestamp,
        userFirstEditTimestamp = userState.userFirstEditTimestamp
      ),
      revisionDetails = new MediawikiEventRevisionDetails(
        // revId: NA
        // revParentId: NA
        // revMinorEdit: NA
        // revTextBytes: NA
        // revTextBytesDiff: NA
        // revTextSha1: NA
        // revContentModel: NA
        // revContentFormat: NA

        // revDeletedTimestamp: NA
        // revRevertedTimestamp: NA
      )
    )
  }

  def fromPageState(pageState: PageState): MediawikiEvent = {
    MediawikiEvent(
      wikiDb = pageState.wikiDb,
      eventEntity = "page",
      eventType = pageState.causedByEventType,
      eventTimestamp = pageState.startTimestamp,
      eventComment = None,
      eventUserDetails = new MediawikiEventUserDetails(
        userId = pageState.causedByUserId,
        userTextHistorical = pageState.causedByUserText,
        // Make historical username current one for anonymous users
        userText = if (pageState.causedByAnonymousUser.getOrElse(false)) pageState.causedByUserText else None,
        userIsAnonymous = pageState.causedByAnonymousUser
      ),
      pageDetails = new MediawikiEventPageDetails(
        pageId = pageState.pageId,
        pageArtificialId = pageState.pageArtificialId,
        pageTitleHistorical = Some(pageState.titleHistorical),
        pageTitle = Some(pageState.title),
        pageNamespaceHistorical = Some(pageState.namespaceHistorical),
        pageNamespaceIsContentHistorical = Some(pageState.namespaceIsContentHistorical),
        pageNamespace = Some(pageState.namespace),
        pageNamespaceIsContent = Some(pageState.namespaceIsContent),
        pageIsRedirect = pageState.isRedirect,
        pageIsDeleted = Some(pageState.isDeleted),
        pageCreationTimestamp = pageState.pageCreationTimestamp,
        pageFirstEditTimestamp = pageState.pageFirstEditTimestamp
      ),
      userDetails = new MediawikiEventUserDetails(
        // userId: NA
        // userTextHistorical: NA
        // userText: NA
        // userBlocksHistorical: NA
        // userBlocks: NA
        // userGroupsHistorical: NA
        // userGroups: NA
        // userCreationTimestamp: NA
      ),
      revisionDetails = new MediawikiEventRevisionDetails(
        // revId: NA
        // revParentId: NA
        // revMinorEdit: NA
        // revTextBytes: NA
        // revTextBytesDiff: NA
        // revTextSha1: NA
        // revContentModel: NA
        // revContentFormat: NA

        // revDeletedTimestamp: NA
        // revRevertedTimestamp: NA
      )
    )
  }

  /**
    * Object functions out of methods for passing them as parameters
    */

  def updateWithUserState(mwEvent: MediawikiEvent, userState: UserState) = mwEvent.updateEventUserDetails(userState)
  def updateWithPageState(mwEvent: MediawikiEvent, pageState: PageState) = mwEvent.updatePageDetails(pageState)

  def updateWithOptionalUserPreviousRevision(mwEvent: MediawikiEvent, optionalUserPreviousRevision: Option[MediawikiEvent]) =
    mwEvent.updateWithUserPreviousRevision(optionalUserPreviousRevision)
  def updateWithOptionalPagePreviousRevision(mwEvent: MediawikiEvent, optionalPagePreviousRevision: Option[MediawikiEvent]) =
    mwEvent.updateWithPagePreviousRevision(optionalPagePreviousRevision)

  /**
    * Filters a key-and-MW-Event and optional key-and-state to update
    * MW Event with state only in valid cases (MW Event key defined and
    * state defined)
    * @param updateWithState The function to update mw_event with state
    *                        when everything is defined
    * @param stateName The name of the state for error messaging
    * @param keyAndMwEvent The key-and-MW-Event to potentially update
    * @param potentialKeyAndState The optional key-and-state to update with
    * @tparam S The state type
    * @return The (potentially) updated mw_event
    */
  def updateWithOptionalState[S](
                                  updateWithState: (MediawikiEvent, S) => MediawikiEvent,
                                  stateName: String,
                                  statsAccumulator: Option[MapAccumulator[String, Long]] = None
                                 )
                                (
                                  keyAndMwEvent: (MediawikiEventKey, MediawikiEvent),
                                  potentialKeyAndState: Option[(StateKey, S)]
                                 ): MediawikiEvent = {
    val (mwKey, mwEvent) = keyAndMwEvent
    val metricHead = s"${mwEvent.wikiDb}.denormalize.${mwEvent.eventEntity}.${stateName}Updates"
    if (mwKey.partitionKey.id <= 0L) {
      // negative or 0 ids are fake (used to shuffle among workers)  -- Don't update
      statsAccumulator.foreach(_.add(s"$metricHead.fakeId", 1L))
      mwEvent.copy(eventErrors = mwEvent.eventErrors :+ s"Negative MW Event id for potential $stateName update")
    } else if (potentialKeyAndState.isEmpty) {
      // No state key was found equal to this mw event key
      // Don't update MW Event content (except error messages)
      statsAccumulator.foreach(_.add(s"$metricHead.noState", 1L))
      mwEvent.copy(eventErrors = mwEvent.eventErrors :+ s"No $stateName match for this MW Event")
    } else {
      statsAccumulator.foreach(_.add(s"$metricHead.update", 1L))
      val (_, state) = potentialKeyAndState.get
      updateWithState(mwEvent, state)
    }
  }

  def updateWithOptionalPrevious(
                                  updateWithOptionalPreviousInner: (MediawikiEvent, Option[MediawikiEvent]) => MediawikiEvent,
                                  statsAccumulator: Option[MapAccumulator[String, Long]] = None,
                                  groupByMetricDescription: String
                                )(
                                  keyAndMwEvent: (MediawikiEventKey, MediawikiEvent),
                                  previousKeyAndMwEvent: Option[(MediawikiEventKey, MediawikiEvent)]
                                ): (MediawikiEventKey, MediawikiEvent) = {
    val (mwKey, mwEvent) = keyAndMwEvent
    val metricHead = s"${mwEvent.wikiDb}.denormalize.${mwEvent.eventEntity}.$groupByMetricDescription.withPrevious"
    if (mwKey.partitionKey.id <= 0L) {
      // negative or 0 ids are fake (used to shuffle among workers)  -- Don't update
      statsAccumulator.foreach(_.add(s"$metricHead.fakeId", 1L))
      (mwKey, mwEvent)
    } else {
      statsAccumulator.foreach(_.add(s"$metricHead.update", 1L))
      (mwKey, updateWithOptionalPreviousInner(mwEvent, previousKeyAndMwEvent.map(_._2)))
    }
  }


  /**
    * Predefine user and page optional update functions
    */
  def updateWithOptionalUser(statsAccumulator: Option[MapAccumulator[String, Long]] = None) =
    updateWithOptionalState[UserState](updateWithUserState, "userState", statsAccumulator) _
  def updateWithOptionalPage(statsAccumulator: Option[MapAccumulator[String, Long]] = None) =
    updateWithOptionalState[PageState](updateWithPageState, "pageState", statsAccumulator) _

  def updateWithOptionalUserPrevious(statsAccumulator: Option[MapAccumulator[String, Long]] = None) =
    updateWithOptionalPrevious(updateWithOptionalUserPreviousRevision, statsAccumulator, "byUser") _
  def updateWithOptionalPagePrevious(statsAccumulator: Option[MapAccumulator[String, Long]] = None) =
    updateWithOptionalPrevious(updateWithOptionalPagePreviousRevision, statsAccumulator, "byPage") _


  /**
    * Methods for TSV line formatting.
    */
  private def escape(s: String): String = {
    if (s == null) null
    else s.replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")
  }
  private def formatArray(array: Seq[String]): String = {
    if (array == null) null
    else MediawikiEvent.escape(array.map(item => item.replace(",", "\\,")).mkString(","))
  }
}
