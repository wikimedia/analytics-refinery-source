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
                                     pageRevisionCount: Option[Long] = None,
                                     pageSecondsSincePreviousRevision: Option[Long] = None
                                    ) {
  def updateWithPageState(pageState: PageState, eventTimestamp: Option[Timestamp]) = {
    val thisTimestamp = eventTimestamp.getOrElse(new Timestamp(Long.MinValue))
    val pageCreationTimestamp = pageState.pageCreationTimestamp.getOrElse(new Timestamp(Long.MaxValue))
    val beforeCreation = thisTimestamp.before(pageCreationTimestamp)
    this.copy(
      pageArtificialId = pageState.pageArtificialId,
      pageTitleHistorical = if (beforeCreation) None else Some(pageState.titleHistorical),
      pageTitle = Some(pageState.title),
      pageNamespaceHistorical = if (beforeCreation) None else Some(pageState.namespaceHistorical),
      pageNamespaceIsContentHistorical = if (beforeCreation) None else Some(pageState.namespaceIsContentHistorical),
      pageNamespace = Some(pageState.namespace),
      pageNamespaceIsContent = Some(pageState.namespaceIsContent),
      pageIsRedirect = pageState.isRedirect,
      pageIsDeleted = Some(pageState.isDeleted),
      pageCreationTimestamp = pageState.pageCreationTimestamp
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
    revisionDetails.revTags.orNull
  )
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
  def updatePageDetails(pageState: PageState) = this.copy(
    pageDetails = this.pageDetails.updateWithPageState(pageState, this.eventTimestamp)
  )
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
        pageRevisionCount = if (row.isNullAt(34)) None else Some(row.getLong(34)),
        pageSecondsSincePreviousRevision = if (row.isNullAt(35)) None else Some(row.getLong(35))
      ),
      userDetails = new MediawikiEventUserDetails(
        userId = if (row.isNullAt(36)) None else Some(row.getLong(36)),
        userTextHistorical = Option(row.getString(37)),
        userText = Option(row.getString(38)),
        userBlocksHistorical = Option(row.getSeq[String](39)),
        userBlocks = Option(row.getSeq[String](40)),
        userGroupsHistorical = Option(row.getSeq[String](41)),
        userGroups = Option(row.getSeq[String](42)),
        userIsBotByHistorical = Option(row.getSeq[String](43)),
        userIsBotBy = Option(row.getSeq[String](44)),
        userIsCreatedBySelf = if (row.isNullAt(45)) None else Some(row.getBoolean(45)),
        userIsCreatedBySystem = if (row.isNullAt(46)) None else Some(row.getBoolean(46)),
        userIsCreatedByPeer = if (row.isNullAt(47)) None else Some(row.getBoolean(47)),
        userIsAnonymous = if (row.isNullAt(48)) None else Some(row.getBoolean(48)),
        userRegistrationTimestamp = if (row.isNullAt(49)) None else Some(Timestamp.valueOf(row.getString(49))),
        userCreationTimestamp = if (row.isNullAt(50)) None else Some(Timestamp.valueOf(row.getString(50))),
        userFirstEditTimestamp = if (row.isNullAt(51)) None else Some(Timestamp.valueOf(row.getString(51)))
        // userRevisionCount -- Not relevant, user events only
        // userSecondsSincePreviousRevision -- ie
      ),
      revisionDetails = new MediawikiEventRevisionDetails(
        revId = if (row.isNullAt(52)) None else Some(row.getLong(52)),
        revParentId = if (row.isNullAt(53)) None else Some(row.getLong(53)),
        revMinorEdit = if (row.isNullAt(54)) None else Some(row.getBoolean(54)),
        revDeletedParts = Option(row.getSeq[String](55)),
        revDeletedPartsAreSuppressed = if (row.isNullAt(56)) None else Some(row.getBoolean(56)),
        revTextBytes = if (row.isNullAt(57)) None else Some(row.getLong(57)),
        revTextBytesDiff = if (row.isNullAt(58)) None else Some(row.getLong(58)),
        revTextSha1 = Option(row.getString(59)),
        revContentModel = Option(row.getString(60)),
        revContentFormat = Option(row.getString(61)),
        revIsDeletedByPageDeletion = if (row.isNullAt(62)) None else Some(row.getBoolean(62)),
        revDeletedByPageDeletionTimestamp = if (row.isNullAt(63)) None else Some(Timestamp.valueOf(row.getString(63))),
        revIsIdentityReverted = if (row.isNullAt(64)) None else Some(row.getBoolean(64)),
        revFirstIdentityRevertingRevisionId = if (row.isNullAt(65)) None else Some(row.getLong(65)),
        revSecondsToIdentityRevert = if (row.isNullAt(66)) None else Some(row.getLong(66)),
        revIsIdentityRevert = if (row.isNullAt(67)) None else Some(row.getBoolean(67)),
        revTags = Option(row.getSeq[String](68))
      )
    )

  /* select like this and then map this function:
   select wiki_db,
          rev_timestamp,
          rev_comment,
          rev_user,
          rev_user_text,
          rev_page,
          rev_id,
          rev_parent_id,
          rev_minor_edit,
          rev_deleted,
          rev_len,
          rev_sha1,
          rev_content_model,
          rev_content_format,
          rev_tags
     from revision
   */
  def fromRevisionRow(row: Row): MediawikiEvent = {
    // We assume no userId is negative in tables
    val userId = if (row.isNullAt(3)) None else Some(row.getLong(3))
    val userIsAnonymous = userId.map(id => id == 0L)
    val userText = row.getString(4)
    val textBytes = if (row.isNullAt(10)) None else Some(row.getLong(10))
    val revDeletedFlag = if (row.isNullAt(9)) None else Some(row.getInt(9))
    new MediawikiEvent(
      wikiDb = row.getString(0),
      eventEntity = "revision",
      eventType = "create",
      eventTimestamp = TimestampHelpers.makeMediawikiTimestampOption(row.getString(1)),
      eventComment = Option(row.getString(2)),
      eventUserDetails = new MediawikiEventUserDetails(
        userId = userId,
        // userText should be the same as historical if user is anonymous
        // https://phabricator.wikimedia.org/T206883
        userText = if (userIsAnonymous.getOrElse(false)) Option(userText) else None,
        userTextHistorical = Option(userText),
        userIsAnonymous = userIsAnonymous,
        // Provide partial historical value based on usertext as userGroups are not available
        // No need to provide current value as only anonymous-usertext is propagated
        userIsBotByHistorical = Some(UserEventBuilder.isBotBy(userText, Seq.empty))
      ),
      pageDetails = new MediawikiEventPageDetails(
        pageId = if (row.isNullAt(5)) None else Some(row.getLong(5))
        // pageTitleHistorical: need page history
        // pageTitle: need page history
        // pageNamespaceHistorical: need page history
        // pageNamespace: need page history
        // pageCreationTimestamp: need page history
      ),
      userDetails = new MediawikiEventUserDetails(
        // userId: Not Applicable (not a user centered event) - See TODO comment above
        // userTextHistorical: Not Applicable (not a user centered event)
        // userText = Not Applicable (not a user centered event)
        // userBlocksHistorical: need user history
        // userBlocks: need user history
        // userGroupsHistorical: need user history
        // userGroups: need user history
        // userCreationTimestamp: need user history
      ),
      revisionDetails = new MediawikiEventRevisionDetails(
        revId = if (row.isNullAt(6)) None else Some(row.getLong(6)),
        revParentId = if (row.isNullAt(7)) None else Some(row.getLong(7)),
        revMinorEdit = if (row.isNullAt(8)) None else Some(row.getBoolean(8)),
        revDeletedParts = revDeletedFlag.map(
          MediawikiEventRevisionDetails.getRevDeletedParts),
        revDeletedPartsAreSuppressed = revDeletedFlag.map(
          MediawikiEventRevisionDetails.getRevDeletedPartsAreSuppressed),
        revTextBytes = textBytes,
        // Initializing revTextBytesDiff to current textBytes, will be updated later
        revTextBytesDiff = textBytes,
        revTextSha1 = Option(row.getString(11)),
        revContentModel = Option(row.getString(12)),
        revContentFormat = Option(row.getString(13)),
        revIsDeletedByPageDeletion = Some(false),
        revIsIdentityReverted = Some(false),
        revSecondsToIdentityRevert = None,
        revIsIdentityRevert = Some(false),
        revTags = Option(row.getSeq[String](14))
        // revDeletedTimestamp: NA
        // revRevertedTimestamp: need self join,
      )
    )
  }

  /* select like this and then map this function:
   select wiki_db,
          ar_timestamp,
          ar_comment,
          ar_user,
          ar_user_text,
          ar_page_id,
          ar_title,
          ar_namespace,
          ar_rev_id,
          ar_parent_id,
          ar_minor_edit,
          ar_deleted,
          ar_len,
          ar_sha1,
          ar_content_model,
          ar_content_format,
          ar_tags
     from archive
   */
  def fromArchiveRow(row: Row): MediawikiEvent = {
    val userId = if (row.isNullAt(3)) None else Some(row.getLong(3))
    val userIsAnonymous = userId.map(id => id == 0L)
    val userText = row.getString(4)
    val textBytes = if (row.isNullAt(12)) None else Some(row.getLong(12))
    val revDeletedFlag = if (row.isNullAt(11)) None else Some(row.getInt(11))
    MediawikiEvent(
      wikiDb = row.getString(0),
      eventEntity = "revision",
      eventType = "create",
      eventTimestamp = TimestampHelpers.makeMediawikiTimestampOption(row.getString(1)),
      eventComment = Option(row.getString(2)),
      eventUserDetails = new MediawikiEventUserDetails(
        userId = userId,
        // Anonymous users have same current and historical userText
        // https://phabricator.wikimedia.org/T206883
        userText = if (userIsAnonymous.getOrElse(false)) Option(userText) else None,
        userTextHistorical = Option(userText),
        userIsAnonymous = userIsAnonymous,
        // Provide partial historical value based on usertext as userGroups are not available
        // No need to provide current value as only anonymous-usertext is propagated
        userIsBotByHistorical = Some(UserEventBuilder.isBotBy(userText, Seq.empty))
      ),
      pageDetails = new MediawikiEventPageDetails(
        pageId = if (row.isNullAt(5)) None else Some(row.getLong(5)),
        // pageTitle: need page history
        pageTitle = Option(row.getString(6)),
        // pageNamespace: need page history
        pageNamespace = if (row.isNullAt(7)) None else Some(row.getInt(7))
        // pageCreationTimestamp: need page history
      ),
      userDetails = new MediawikiEventUserDetails(
        // userId: NA - See TODO comment above
        // userTextHistorical: NA
        // userText = NA
        // userBlocksHistorical: need user history
        // userBlocks: need user history
        // userGroupsHistorical: need user history
        // userGroups: need user history
        // userCreationTimestamp: need user history
      ),
      revisionDetails = new MediawikiEventRevisionDetails(
        revId = if (row.isNullAt(8)) None else Some(row.getLong(8)),
        revParentId = if (row.isNullAt(9)) None else Some(row.getLong(9)),
        revMinorEdit = if (row.isNullAt(10)) None else Some(row.getBoolean(10)),
        revDeletedParts = revDeletedFlag.map(
          MediawikiEventRevisionDetails.getRevDeletedParts),
        revDeletedPartsAreSuppressed = revDeletedFlag.map(
          MediawikiEventRevisionDetails.getRevDeletedPartsAreSuppressed),
        revTextBytes = textBytes,
        // Initializing revTextBytesDiff to current textBytes, will be updated later
        revTextBytesDiff = textBytes,
        revTextSha1 = Option(row.getString(13)),
        revContentModel = Option(row.getString(14)),
        revContentFormat = Option(row.getString(15)),
        revIsDeletedByPageDeletion = Option(true),
        revIsIdentityReverted = Some(false),
        revSecondsToIdentityRevert = None,
        revIsIdentityRevert = Some(false),
        revTags = Option(row.getSeq[String](16))
        // revRevertedTimestamp: need self join

      )
    )
  }

  def fromUserState(userState: UserState): MediawikiEvent = {
    val userId = userState.causedByUserId
    val userIsAnonymous = userId.map(id => id == 0L)
    val userText = userState.causedByUserText
    MediawikiEvent(
      wikiDb = userState.wikiDb,
      eventEntity = "user",
      eventType = userState.causedByEventType,
      eventTimestamp = userState.startTimestamp,
      eventComment = None,
      eventUserDetails = new MediawikiEventUserDetails(
        userId = userId,
        userTextHistorical = userText,
        // Make historical username current one for anonymous users
        userText = if (userIsAnonymous.getOrElse(false)) userText else None,
        userIsAnonymous = userIsAnonymous,
        // Provide partial historical value based on usertext as userGroups are not available
        // No need to provide current value as only anonymous-usertext is propagated
        userIsBotByHistorical = Some(UserEventBuilder.isBotBy(userText.getOrElse(""), Seq.empty))
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
    val userId = pageState.causedByUserId
    val userIsAnonymous = userId.map(id => id == 0L)
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
        userText = if (userIsAnonymous.getOrElse(false)) pageState.causedByUserText else None,
        userIsAnonymous = userIsAnonymous
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
        pageCreationTimestamp = pageState.pageCreationTimestamp
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


}
