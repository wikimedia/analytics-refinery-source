package org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.wikimedia.analytics.refinery.job.mediawikihistory.page.PageState
import org.wikimedia.analytics.refinery.job.mediawikihistory.user.UserState
import org.wikimedia.analytics.refinery.job.mediawikihistory.user.UserEventBuilder

/**
  * This file defines case classes for denormalized ME Events objects.
  * Provides utility functions to update data and to read/write spark Rows.
  *
  * Page, user and revision information is split into subclasses
  * to overcome the 22-max parameters in case classes limitation of scala.
  */


case class MediawikiEventPageDetails(pageId: Option[Long] = None,
                                     pageTitle: Option[String] = None,
                                     pageTitleLatest: Option[String] = None,
                                     pageNamespace: Option[Int] = None,
                                     pageNamespaceIsContent: Option[Boolean] = None,
                                     pageNamespaceLatest: Option[Int] = None,
                                     pageNamespaceIsContentLatest: Option[Boolean] = None,
                                     pageIsRedirectLatest: Option[Boolean] = None,
                                     pageCreationTimestamp: Option[String] = None)

case class MediawikiEventUserDetails(userId: Option[Long] = None,
                                     userText: Option[String] = None,
                                     userTextLatest: Option[String] = None,
                                     userBlocks: Option[Seq[String]] = None,
                                     userBlocksLatest: Option[Seq[String]] = None,
                                     userGroups: Option[Seq[String]] = None,
                                     userGroupsLatest: Option[Seq[String]] = None,
                                     userIsCreatedBySelf: Option[Boolean] = None,
                                     userIsCreatedBySystem: Option[Boolean] = None,
                                     userIsCreatedByPeer: Option[Boolean] = None,
                                     userIsAnonymous: Option[Boolean] = None,
                                     userIsBotByName: Option[Boolean] = None,
                                     userCreationTimestamp: Option[String] = None)

case class MediawikiEventRevisionDetails(revId: Option[Long] = None,
                                         revParentId: Option[Long] = None,
                                         revMinorEdit: Option[Boolean] = None,
                                         revTextBytes: Option[Long] = None,
                                         revTextBytesDiff: Option[Long] = None,
                                         revTextSha1: Option[String] = None,
                                         revContentModel: Option[String] = None,
                                         revContentFormat: Option[String] = None,
                                         revIsDeleted: Option[Boolean] = None,
                                         revDeletedTimestamp: Option[String] = None,
                                         revIsIdentityReverted: Option[Boolean] = None,
                                         revFirstIdentityRevertingRevisionId: Option[Long] = None,
                                         revFirstIdentityRevertTimestamp: Option[String] = None,
                                         revIsProductive: Option[Boolean] = None,
                                         revIsIdentityRevert: Option[Boolean] = None)

case class MediawikiEvent(
                           wikiDb: String,
                           eventEntity: String,
                           eventType: String,
                           eventTimestamp: Option[String] = None,
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
    eventTimestamp.orNull,
    eventComment.orNull,
    eventUserDetails.userId.orNull,
    eventUserDetails.userText.orNull,
    eventUserDetails.userTextLatest.orNull,
    eventUserDetails.userBlocks.orNull,
    eventUserDetails.userBlocksLatest.orNull,
    eventUserDetails.userGroups.orNull,
    eventUserDetails.userGroupsLatest.orNull,
    eventUserDetails.userIsCreatedBySelf.orNull,
    eventUserDetails.userIsCreatedBySystem.orNull,
    eventUserDetails.userIsCreatedByPeer.orNull,
    eventUserDetails.userIsAnonymous.orNull,
    eventUserDetails.userIsBotByName.orNull,
    eventUserDetails.userCreationTimestamp.orNull,
    pageDetails.pageId.orNull,
    pageDetails.pageTitle.orNull,
    pageDetails.pageTitleLatest.orNull,
    pageDetails.pageNamespace.orNull,
    pageDetails.pageNamespaceIsContent.orNull,
    pageDetails.pageNamespaceLatest.orNull,
    pageDetails.pageNamespaceIsContentLatest.orNull,
    pageDetails.pageIsRedirectLatest.orNull,
    pageDetails.pageCreationTimestamp.orNull,
    userDetails.userId.orNull,
    userDetails.userText.orNull,
    userDetails.userTextLatest.orNull,
    userDetails.userBlocks.orNull,
    userDetails.userBlocksLatest.orNull,
    userDetails.userGroups.orNull,
    userDetails.userGroupsLatest.orNull,
    userDetails.userIsCreatedBySelf.orNull,
    userDetails.userIsCreatedBySystem.orNull,
    userDetails.userIsCreatedByPeer.orNull,
    userDetails.userIsAnonymous.orNull,
    userDetails.userIsBotByName.orNull,
    userDetails.userCreationTimestamp.orNull,
    revisionDetails.revId.orNull,
    revisionDetails.revParentId.orNull,
    revisionDetails.revMinorEdit.orNull,
    revisionDetails.revTextBytes.orNull,
    revisionDetails.revTextBytesDiff.orNull,
    revisionDetails.revTextSha1.orNull,
    revisionDetails.revContentModel.orNull,
    revisionDetails.revContentFormat.orNull,
    revisionDetails.revIsDeleted.orNull,
    revisionDetails.revDeletedTimestamp.orNull,
    revisionDetails.revIsIdentityReverted.orNull,
    revisionDetails.revFirstIdentityRevertingRevisionId.orNull,
    revisionDetails.revFirstIdentityRevertTimestamp.orNull,
    revisionDetails.revIsProductive.orNull,
    revisionDetails.revIsIdentityRevert.orNull
  )
  def textBytesDiff(value: Option[Long]) = this.copy(revisionDetails = this.revisionDetails.copy(revTextBytesDiff = value))
  def isDeleted(deleteTimestamp: Option[String]) = this.copy(revisionDetails = this.revisionDetails.copy(
    revIsDeleted = Some(true),
    revDeletedTimestamp = deleteTimestamp))
  def isIdentityRevert = this.copy(revisionDetails = this.revisionDetails.copy(revIsIdentityRevert = Some(true)))
  def isIdentityReverted(
                          revertingTimestamp: Option[String],
                          revertingRevisionId: Option[Long],
                          isProductive: Option[Boolean]
                        ) = this.copy(revisionDetails = this.revisionDetails.copy(
    revIsIdentityReverted = Some(true),
    revFirstIdentityRevertTimestamp = revertingTimestamp,
    revFirstIdentityRevertingRevisionId = revertingRevisionId,
    revIsProductive = isProductive))
  def updateEventUserDetails(userState: UserState) = this.copy(
    eventUserDetails = this.eventUserDetails.copy(
      userId = Some(userState.userId),
      userText = Some(userState.userName),
      userTextLatest = Some(userState.userNameLatest),
      userBlocks = Some(userState.userBlocks),
      userBlocksLatest = Some(userState.userBlocksLatest),
      userGroups = Some(userState.userGroups),
      userGroupsLatest = Some(userState.userGroupsLatest),
      userIsCreatedBySelf = Some(userState.createdBySelf),
      userIsCreatedBySystem = Some(userState.createdBySystem),
      userIsCreatedByPeer = Some(userState.createdByPeer),
      userIsAnonymous = Some(userState.anonymous),
      userIsBotByName = Some(userState.botByName),
      userCreationTimestamp = userState.userRegistrationTimestamp
    ))
  def updatePageDetails(pageState: PageState) = this.copy(
    pageDetails = this.pageDetails.copy(
      pageTitle = Some(pageState.title),
      pageTitleLatest = Some(pageState.titleLatest),
      pageNamespace = Some(pageState.namespace),
      pageNamespaceIsContent = Some(pageState.namespaceIsContent),
      pageNamespaceLatest = Some(pageState.namespaceLatest),
      pageNamespaceIsContentLatest = Some(pageState.namespaceIsContentLatest),
      pageIsRedirectLatest = pageState.isRedirectLatest,
      pageCreationTimestamp = pageState.pageCreationTimestamp
    ))

}

object MediawikiEvent {

  val schema = StructType(
    Seq(
      StructField("wiki_db", StringType, nullable = false),
      StructField("event_entity", StringType, nullable = false),
      StructField("event_type", StringType, nullable = false),
      StructField("event_timestamp", StringType, nullable = true),
      StructField("event_comment", StringType, nullable = true),
      StructField("event_user_id", LongType, nullable = false),
      StructField("event_user_text", StringType, nullable = false),
      StructField("event_user_text_latest", StringType, nullable = false),
      StructField("event_user_blocks", ArrayType(StringType, containsNull = true), nullable = false),
      StructField("event_user_blocks_latest", ArrayType(StringType, containsNull = true), nullable = false),
      StructField("event_user_groups", ArrayType(StringType, containsNull = true), nullable = false),
      StructField("event_user_groups_latest", ArrayType(StringType, containsNull = true), nullable = false),
      StructField("event_user_is_created_by_self", BooleanType, nullable = false),
      StructField("event_user_is_created_by_system", BooleanType, nullable = false),
      StructField("event_user_is_created_by_peer", BooleanType, nullable = false),
      StructField("event_user_is_anonymous", BooleanType, nullable = false),
      StructField("event_user_is_bot_by_name", BooleanType, nullable = false),
      StructField("event_user_creation_timestamp", StringType, nullable = true),
      StructField("page_id", LongType, nullable = true),
      StructField("page_title", StringType, nullable = true),
      StructField("page_title_latest", StringType, nullable = true),
      StructField("page_namespace", IntegerType, nullable = true),
      StructField("page_namespace_is_content", BooleanType, nullable = true),
      StructField("page_namespace_latest", IntegerType, nullable = true),
      StructField("page_namespace_is_content_latest", BooleanType, nullable = true),
      StructField("page_is_redirect_latest", BooleanType, nullable = true),
      StructField("page_creation_timestamp", StringType, nullable = true),
      StructField("user_id", LongType, nullable = true),
      StructField("user_text", StringType, nullable = true),
      StructField("user_text_latest", StringType, nullable = true),
      StructField("user_blocks", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("user_blocks_latest", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("user_groups", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("user_groups_latest", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("user_is_created_by_self", BooleanType, nullable = true),
      StructField("user_is_created_by_system", BooleanType, nullable = true),
      StructField("user_is_created_by_peer", BooleanType, nullable = true),
      StructField("user_is_anonymous", BooleanType, nullable = true),
      StructField("user_is_bot_by_name", BooleanType, nullable = true),
      StructField("user_creation_timestamp", StringType, nullable = true),
      StructField("revision_id", LongType, nullable = true),
      StructField("revision_parent_id", LongType, nullable = true),
      StructField("revision_minor_edit", BooleanType, nullable = true),
      StructField("revision_text_bytes", LongType, nullable = true),
      StructField("revision_text_bytes_diff", LongType, nullable = true),
      StructField("revision_text_sha1", StringType, nullable = true),
      StructField("revision_content_model", StringType, nullable = true),
      StructField("revision_content_format", StringType, nullable = true),
      StructField("revision_is_deleted", BooleanType, nullable = true),
      StructField("revision_deleted_timestamp", StringType, nullable = true),
      StructField("revision_is_identity_reverted", BooleanType, nullable = true),
      StructField("revision_first_identity_reverting_revision_id", LongType, nullable = true),
      StructField("revision_first_identity_revert_timestamp", StringType, nullable = true),
      StructField("revision_is_productive", BooleanType, nullable = true),
      StructField("revision_is_identity_revert", BooleanType, nullable = true)
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
          rev_len,
          rev_sha1,
          rev_content_model,
          rev_content_format
     from revision
   */
  def fromRevisionRow(row: Row): MediawikiEvent = new MediawikiEvent(
    wikiDb = row.getString(0),
    eventEntity = "revision",
    eventType = "create",
    // Only accept valid timestamps
    eventTimestamp = if (row.isNullAt(1) || row.getString(1).length != 14) None else Some(row.getString(1)),
    eventComment = Option(row.getString(2)),
    eventUserDetails = new MediawikiEventUserDetails(
      // TODO: When userId = 0, it does make no sense to store eventUserTextLatest.
      userId = if (row.isNullAt(3)) None else Some(row.getLong(3)),
      // Next fields: Needed in case userId is 0, overwritten otherwise
      userText = Option(row.getString(4)),
      userIsAnonymous = Some(row.isNullAt(3) || row.getLong(3) <= 0),
      userIsBotByName = Some(!row.isNullAt(4) && UserEventBuilder.isBotByName(row.getString(4)))
    ),
    pageDetails = new MediawikiEventPageDetails(
      pageId = if (row.isNullAt(5)) None else Some(row.getLong(5))
      // pageTitle: need page history
      // pageTitleLatest: need page history
      // pageNamespace: need page history
      // pageNamespaceLatest: need page history
      // pageCreationTimestamp: need page history
    ),
    userDetails = new MediawikiEventUserDetails(
      // userId: Not Applicable (not a user centered event) - See TODO comment above
      // userText: Not Applicable (not a user centered event)
      // userTextLatest = Not Applicable (not a user centered event)
      // userBlocks: need user history
      // userBlocksLatest: need user history
      // userGroups: need user history
      // userGroupsLatest: need user history
      // userCreationTimestamp: need user history
    ),
    revisionDetails = new MediawikiEventRevisionDetails(
      revId = if (row.isNullAt(6)) None else Some(row.getLong(6)),
      revParentId = if (row.isNullAt(7)) None else Some(row.getLong(7)),
      revMinorEdit = if (row.isNullAt(8)) None else Some(row.getBoolean(8)),
      revTextBytes = if (row.isNullAt(9)) None else Some(row.getLong(9)),
      // Initializing revTextBytesDiff to current textBytes, will be updated later
      revTextBytesDiff = if (row.isNullAt(9)) None else Some(row.getLong(9)),
      revTextSha1 = Option(row.getString(10)),
      revContentModel = Option(row.getString(11)),
      revContentFormat = Option(row.getString(12)),
      revIsDeleted = Some(false),
      revIsIdentityReverted = Some(false),
      revIsProductive = Some(true), // updated at revert computation if not productive
      revIsIdentityRevert = Some(false)
      // revDeletedTimestamp: NA
      // revRevertedTimestamp: need self join
    )
  )

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
          ar_len,
          ar_sha1,
          ar_content_model,
          ar_content_format
     from archive
   */
  def fromArchiveRow(row: Row): MediawikiEvent = MediawikiEvent(
    wikiDb = row.getString(0),
    eventEntity = "revision",
    eventType = "create",
    // Only accept valid timestamps
    eventTimestamp = if (row.isNullAt(1) || row.getString(1).length != 14) None else Some(row.getString(1)),
    eventComment = Option(row.getString(2)),
    eventUserDetails = new MediawikiEventUserDetails(
      // TODO: When userId = 0, it does make no sense to store eventUserTextLatest.
      userId = if (row.isNullAt(3)) None else Some(row.getLong(3)),
      // Next fields -- Needed in case userId is 0, overwritten otherwise
      userText = Option(row.getString(4)),
      userIsAnonymous = Some(row.isNullAt(3) || row.getLong(3) <= 0),
      userIsBotByName = Some(!row.isNullAt(4) && UserEventBuilder.isBotByName(row.getString(4)))
    ),
    pageDetails = new MediawikiEventPageDetails(
      pageId = if (row.isNullAt(5)) None else Some(row.getLong(5)),
      // pageTitle: need page history
      pageTitleLatest = Option(row.getString(6)),
      // pageNamespace: need page history
      pageNamespaceLatest = if (row.isNullAt(7)) None else Some(row.getInt(7))
      // pageCreationTimestamp: need page history
    ),
    userDetails = new MediawikiEventUserDetails(
      // userId: NA - See TODO comment above
      // userText: NA
      // userTextLatest = NA
      // userBlocks: need user history
      // userBlocksLatest: need user history
      // userGroups: need user history
      // userGroupsLatest: need user history
      // userCreationTimestamp: need user history
    ),
    revisionDetails = new MediawikiEventRevisionDetails(
      revId = if (row.isNullAt(8)) None else Some(row.getLong(8)),
      revParentId = if (row.isNullAt(9)) None else Some(row.getLong(9)),
      revMinorEdit = if (row.isNullAt(10)) None else Some(row.getBoolean(10)),
      revTextBytes = if (row.isNullAt(11)) None else Some(row.getLong(11)),
      // Initializing revTextBytesDiff to current textBytes, will be updated later
      revTextBytesDiff = if (row.isNullAt(11)) None else Some(row.getLong(11)),
      revTextSha1 = Option(row.getString(12)),
      revContentModel = Option(row.getString(13)),
      revContentFormat = Option(row.getString(14)),
      revIsDeleted = Option(true),
      revDeletedTimestamp = Option("TBD"), // Hack to prevent having to join with pageStates 2 times
      revIsIdentityReverted = Some(false),
      revIsProductive = Some(true),
      revIsIdentityRevert = Some(false)
      // revRevertedTimestamp: need self join
    )
  )

  def fromUserState(userState: UserState): MediawikiEvent = MediawikiEvent(
    wikiDb = userState.wikiDb,
    eventEntity = "user",
    eventType = userState.causedByEventType,
    eventTimestamp = userState.startTimestamp,
    eventComment = None,
    eventUserDetails = new MediawikiEventUserDetails(
      userId = userState.causedByUserId
    ),
    pageDetails = new MediawikiEventPageDetails(
      // pageId: NA
      // pageTitle: NA
      // pageTitleLatest: NA
      // pageNamespace: NA
      // pageNamespaceLatest: NA
      // pageCreationTimestamp: NA
    ),
    userDetails = new MediawikiEventUserDetails(
      userId = Some(userState.userId),
      userText = Some(userState.userName),
      userTextLatest = Some(userState.userNameLatest),
      userBlocks = Some(userState.userBlocks),
      userBlocksLatest = Some(userState.userBlocksLatest),
      userGroups = Some(userState.userGroups),
      userGroupsLatest = Some(userState.userGroupsLatest),
      userIsCreatedBySelf = Some(userState.createdBySelf),
      userIsCreatedBySystem = Some(userState.createdBySystem),
      userIsCreatedByPeer = Some(userState.createdByPeer),
      userIsAnonymous = Some(userState.anonymous),
      userIsBotByName = Some(userState.botByName),
      userCreationTimestamp = userState.userRegistrationTimestamp
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

  def fromPageState(pageState: PageState): MediawikiEvent = MediawikiEvent(
    wikiDb = pageState.wikiDb,
    eventEntity = "page",
    eventType = pageState.causedByEventType,
    eventTimestamp = pageState.startTimestamp,
    eventComment = None,
    eventUserDetails = new MediawikiEventUserDetails(
      userId = pageState.causedByUserId
    ),
    pageDetails = new MediawikiEventPageDetails(
      pageId = pageState.pageId, // TODO: what to do with artificial pages?
      pageTitle = Some(pageState.title),
      pageTitleLatest = Some(pageState.titleLatest),
      pageNamespace = Some(pageState.namespace),
      pageNamespaceIsContent = Some(pageState.namespaceIsContent),
      pageNamespaceLatest = Some(pageState.namespaceLatest),
      pageNamespaceIsContentLatest = Some(pageState.namespaceIsContentLatest),
      pageIsRedirectLatest = pageState.isRedirectLatest,
      pageCreationTimestamp = pageState.pageCreationTimestamp
    ),
    userDetails = new MediawikiEventUserDetails(
      // userId: NA
      // userText: NA
      // userTextLatest: NA
      // userBlocks: NA
      // userBlocksLatest: NA
      // userGroups: NA
      // userGroupsLatest: NA
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

  /**
    * Object functions out of methods for passing them as parameters
    */
  def updateWithUserState(mwEvent: MediawikiEvent, userState: UserState) = mwEvent.updateEventUserDetails(userState)
  def updateWithPageState(mwEvent: MediawikiEvent, pageState: PageState) = mwEvent.updatePageDetails(pageState)


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
                                  stateName: String
                                 )
                                (
                                  keyAndMwEvent: (MediawikiEventKey, MediawikiEvent),
                                  potentialKeyAndState: Option[(StateKey, S)]
                                 ): MediawikiEvent = {
    val (mwKey, mwEvent) = keyAndMwEvent
    if (mwKey.partitionKey.id <= 0L) {
      // negative or 0 ids are fake (used to shuffle among workers)  -- Don't update
      mwEvent.copy(eventErrors = mwEvent.eventErrors :+ s"Negative MW Event id for potential $stateName update")
    } else if (potentialKeyAndState.isEmpty) {
      // No state key was found equal to this mw event key
      // Don't update MW Event content (except error messages)
      mwEvent.copy(eventErrors = mwEvent.eventErrors :+ s"No $stateName match for this MW Event")
    } else {
      val (_, state) = potentialKeyAndState.get
      updateWithState(mwEvent, state)
    }
  }

  /**
    * Predefine user and page optional update functions
    */
  def updateWithOptionalUser = updateWithOptionalState[UserState](updateWithUserState, "userState") _
  def updateWithOptionalPage = updateWithOptionalState[PageState](updateWithPageState, "pageState") _


}