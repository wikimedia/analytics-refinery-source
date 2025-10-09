package org.wikimedia.analytics.refinery.job.mediawikihistory.page

import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.spark.utils.{StatsHelper, MapAccumulator}


/**
  * This class implements the core algorithm of the page history reconstruction.

  * The [[run]] function first partitions the [[PageState]] and [[PageEvent]] RDDs
  * using [[org.wikimedia.analytics.refinery.spark.utils.SubgraphPartitioner]],
  * then applies its [[processSubgraph]] method to every partition.
  *
  * It returns the [[PageState]] RDD of joined results of every partition and either
  * the errors found on the way, or their count.
  *
  * The page reconstruction algorithm works with a set of page states, to which it joins
  * a set of page events going backward in time. At the beginning of the algorithm the
  * set of page states is the current state of all non-deleted pages, augmented by the
  * set of deleted ones rebuilt from archived revisions, when their page-title and page-id
  * don't collide with existing ones.
 *
  * The algorithm goes backward in time, joining current states to their previous instance
  * through page events, using deleted-states when a delete event has no existing
  * lineage but a matching deleted one (otherwise, a fake lineage is created with a pageArtificialId).
  * The join is made using either pageId or (pageTitle, pageNamespace). If a page event has a defined
  * pageId, then it is used for the join, except in special cases describe below.
  * If a page event doesn't have a pageId, the join is made using the pageTitle and namespace.
 *
  * There are two special cases preventing us to use pageIds even they are defined:
  *   - Before 2016-05-06, when a page was deleted then restored, it was assigned a new pageId.
  *     After 2016-05-06, the page conserves its Id when restored. To overcome this issue,
  *     we don't use Ids to link delete nor restore events when they occur before 2016-05-06.
  *  - Before 2014-09-26 move events were referencing pageIds of potentially created redirects,
  *    not of the moved page. To overcome this issue, we don't use Ids to link move events when
  *    they occur before 2014-09-26
  * Those two special cases are defined in [[PageEvent.hasKeyId]]
  *
  */
class PageHistoryBuilder(
                          val spark: SparkSession,
                          val statsAccumulator: Option[MapAccumulator[String, Long]]
                        ) extends StatsHelper with Serializable {

  import org.apache.log4j.Logger
  import java.util.UUID.randomUUID
  import java.sql.Timestamp
  import org.apache.spark.rdd.RDD
  import org.wikimedia.analytics.refinery.spark.utils.SubgraphPartitioner

  @transient
  lazy val log: Logger = Logger.getLogger(this.getClass)

  val METRIC_EVENTS_MATCHING_OK_BY_TITLE = "pageHistory.eventsMatching.OK.byTitle"
  val METRIC_EVENTS_MATCHING_OK_BY_ID = "pageHistory.eventsMatching.OK.byId"
  val METRIC_EVENTS_MATCHING_KO_BY_TITLE = "pageHistory.eventsMatching.KO.byTitle"
  val METRIC_EVENTS_MATCHING_KO_BY_ID = "pageHistory.eventsMatching.KO.byId"

  /**
   * Object encoding ProcessingStatus possible store destination.
   */
  private object StatusDestination {
    sealed trait EnumVal
    // Store the state in potential states maps
    case object Potential extends EnumVal
    // Store the state in restore states maps
    case object Restore extends EnumVal
  }

  type TitleMap = Map[PageHistoryBuilder.KEY, PageState]
  type IdMap = Map[PageHistoryBuilder.KEYID, PageState]

  /**
   * This case class contains the various state dictionaries and lists needed to
   * reconstruct the page history, as well as errors. It also defines
   * the various needed functions to process a pair of state-list/event-list
   * through the main [[processEvent]] entry-point.
   *
   * @param potentialStatesByTitle Regular states-store where states can be joined to events
   *                               by title and namespace
   * @param potentialStatesById Regular states-store where states can be joined to events
   *                            by page_id
   * @param restoredStatesByTitle Special states-store for restore events to be possibly linked to
   *                              delete events by title and namespace
   * @param restoredStatesById Special states-store for restore events to be possibly linked to
   *                           delete events by page_id
   * @param baseDeletedStatesByTitle Special states-store containing states of deleted pages rebuilt
   *                                 from the archive table, retrieved by title and namespace.
   *                                 They are used to start lineages from deleted pages with real
   *                                 information (noticeably page_id) instead of creating an empty
   *                                 fake Lineage.
   * @param baseDeletedStatesById Special states-store containing states of deleted pages rebuilt
   *                              from the archive table, retrieved by page_id.
   *                              They are used to start lineages from deleted pages with real
   *                              information (noticeably page_id) instead of creating an empty
   *                              fake Lineage.
   * @param knownStates List of states having already been joined to events
   * @param unmatchedEvents List of events having not matched any state
   */
  private case class ProcessingStatus(
    potentialStatesByTitle: TitleMap,
    potentialStatesById: IdMap,
    restoredStatesByTitle: TitleMap,
    restoredStatesById: IdMap,
    baseDeletedStatesByTitle: TitleMap,
    baseDeletedStatesById: IdMap,
    knownStates: Seq[PageState],
    unmatchedEvents: Seq[PageEvent]
  ) {


    /**
     * Type representing a page key as either:
     *  - KeyId (wikiDb, pageId)
     *  - Key (wikiDb, pageTitle, pageNamespace)
     */
    private type PageKey = Either[PageHistoryBuilder.KEYID, PageHistoryBuilder.KEY]


    /**
     * Class conveying a possibly matched state, with its key, optional potential-state
     * an optional restore-state
     *
     * @param key The PageKey used to match the state - pageId if event.hasKeyId is true,
     *            title and namespace otherwise.
     * @param potentialState The state retrieved from potential states-store using the key (if any)
     * @param restoreState The state retrieved from restore states-store using the key (if any)
     *
     * Note: potentialState and restoreState can be both defined, and in that case the [[get]]
     *       method returns the potential state, as it is the one to use in the default case
     *       (restore is to be used preferentially only for delete events).
     */
    private case class MatchedState(
      key: PageKey,
      potentialState: Option[PageState],
      restoreState: Option[PageState]
    ) {
      def isDefined: Boolean = potentialState.isDefined || restoreState.isDefined
      def isEmpty: Boolean = potentialState.isEmpty && restoreState.isEmpty
      def inPotential: Boolean = potentialState.isDefined
      def inRestore: Boolean = restoreState.isDefined
      def byId: Boolean = key.isLeft
      def get: PageState = {
        // Fail with a nice message if state is called on an undefined MatchedState
        assert(isDefined, "MatchedState must be defined to provide a state")
        potentialState.getOrElse(restoreState.get)
      }
    }


    /**
     * Utility Function adding an unmatched event to the given processingStatus
     * and updating statistics at the same time.
     *
     * @param event The event to add to the unmatched event list
     * @return The updated ProcessingStatus object
     */
    private def addUnmatchedEvent(
      event: PageEvent
    ): ProcessingStatus = {
      if (event.hasKeyId) addOptionalStat(s"${event.wikiDb}.$METRIC_EVENTS_MATCHING_KO_BY_ID", 1)
      else addOptionalStat(s"${event.wikiDb}.$METRIC_EVENTS_MATCHING_KO_BY_TITLE", 1)
      this.copy(unmatchedEvents = this.unmatchedEvents :+ event)
    }


    /**
     * Function used in [[flushEventBeforeCreation]].
     * It facilitates flushing events both in potentialStates and restoredStates.
     *
     * @param statesByTitle The byTitle states-store (either potential or restore)
     * @param statesById The byId states-store (either potential or restore)
     * @param event The event used as base to flush a potential conflicting state
     * @return the updated maps and the flushed state (named conflicting state later), if any
     */
    private def flushEventBeforeCreationInMaps(
      statesByTitle: TitleMap,
      statesById: IdMap,
      event: PageEvent
    ): (TitleMap, IdMap, Option[PageState]) = {

      // Flush potentialStates if its creation is greater than the event's timestamp.
      val foundById = event.hasKeyId && statesById.contains(event.keyId)
      val toKey = event.toKey

      if ((foundById && statesById(event.keyId).pageCreationTimestamp.isDefined
        && event.timestamp.before(statesById(event.keyId).pageCreationTimestamp.get))
        || (statesByTitle.contains(toKey) && statesByTitle(toKey).pageCreationTimestamp.isDefined &&
        event.timestamp.before(statesByTitle(toKey).pageCreationTimestamp.get))) {

        // We are sure state will be defined, as the `if` statement above enforced a match
        // in either statesById or statesByTitle
        val state = if (foundById) statesById(event.keyId) else statesByTitle(toKey)
        (
          statesByTitle - state.key,
          if (state.hasKeyId) statesById - state.keyId else statesById,
          Some(state.copy(
            startTimestamp = state.pageCreationTimestamp,
            inferredFrom = Some("event-before-page-creation"),
            sourceLogId = Some(event.sourceLogId),
            sourceLogComment = Some(event.sourceLogComment),
            sourceLogParams = Some(event.sourceLogParams)
          ))
          )
      } else (statesByTitle, statesById, None)
    }


    /**
     * Flushes a potentialState and/or restoreState to knownStates if:
     *  - it matches the given event (by page_id or title)
     *  - the matching event's timestamp is before the state pageCreationTimestamp
     *
     * We enforce this to prevent linking past history to a page which is known to have
     * been created later in time.
     *
     * @param event The event used as base to flush a potential conflicting state
     * @return The updated processing status
     */
    private def flushEventBeforeCreation(
      event: PageEvent
    ): ProcessingStatus = {

      // Flush from potentialStates (but not yet stored in known states)
      val (newPotentialStatesByTitle, newPotentialStatesById, knownState1) =
        flushEventBeforeCreationInMaps(this.potentialStatesByTitle, this.potentialStatesById, event)

      // Flush from restoredStates (but not yet stored in known states)
      val (newRestoreStatesByTitle, newRestoreStatesById, knownState2) =
        flushEventBeforeCreationInMaps(this.restoredStatesByTitle, this.restoredStatesById, event)

      // Return status with updated maps and knownStates
      this.copy(
        potentialStatesByTitle = newPotentialStatesByTitle,
        potentialStatesById = newPotentialStatesById,
        restoredStatesByTitle = newRestoreStatesByTitle,
        restoredStatesById = newRestoreStatesById,
        knownStates = this.knownStates ++ knownState1 ++ knownState2
      )
    }

    /**
     * Builds the MatchedState for the event.
     *  - by id if the event has a PageId
     *  - by title if the event doesn't have a pageId
     *
     *  @param event The event to build the MatchedState for
     *  @return Returns the MatchedState for the given event
     */
    private def getMatchedState(
      event: PageEvent
    ): MatchedState = {
      if (event.hasKeyId) {
        val keyId = event.keyId
        MatchedState(
          key = Left(keyId),
          potentialState = this.potentialStatesById.get(keyId),
          restoreState = this.restoredStatesById.get(keyId)
        )
      } else {
        val key = event.toKey
        MatchedState(
          key = Right(key),
          potentialState = this.potentialStatesByTitle.get(key),
          restoreState = this.restoredStatesByTitle.get(key)
        )
      }
    }

    /**
     * Flushes a potentialState if the event fromKey conflicts by Title. This can happen
     * in two cases, for move or deleted events.
     *
     * For instance if we need to process a move event from title A to title B. This method
     * flushes an existing potential state with title A, to prevent having two pages with
     * the same title after the move event is processed.
     * Similarly, if a delete event with title A is to be processed, it means a page with
     * title A was existing before the delete event, and therefore if one exists currently
     * they will conflict, so we flush the currently existing one if it exists.
     *
     * @param event The event used to possibly flush a conflicting potential state
     * @param conflictType The conflict-type to assign to the flushed state
     *                     (move-conflict or delete-conflict)
     * @return The updated processing status and the conflicting PageState
     *         (useful to propagate information in case of delete)
     */
    private def flushConflictingPotentialState(
      event: PageEvent,
      conflictType: String
    ): (ProcessingStatus, Option[PageState]) = {
      val key = event.fromKey

      if (this.potentialStatesByTitle.contains(key)) {
        val matchedState = this.potentialStatesByTitle(key)

        // pageCreationTimestamp propagation -- Special case if pageId is defined but keyId is
        // undefined because of timestamp boundaries:
        //  - If pageId match between event and state, propagate state pageCreation as it is the same page!
        //  - otherwise use event timestamp to maintain lineage timestamp coherence
        val creationTimestamp = {
          if (event.pageId.exists(_ > 0) && event.pageId == matchedState.pageId)
            matchedState.pageCreationTimestamp
          else
            Some(event.timestamp)
        }

        val newKnownState = matchedState.copy(
          startTimestamp = Some(event.timestamp),
          pageCreationTimestamp = creationTimestamp,
          // if we have causedBy* fields on the state, they're the best guess as to the actor, so keep them
          inferredFrom = Some(conflictType),
          sourceLogId = Some(event.sourceLogId),
          sourceLogComment = Some(event.sourceLogComment),
          sourceLogParams = Some(event.sourceLogParams)
        )
        // Remove conflicting state from potentialStates and add new known state
        (
          this.copy(
            potentialStatesByTitle = this.potentialStatesByTitle - matchedState.key,
            potentialStatesById = {
              if (matchedState.hasKeyId)
                this.potentialStatesById - matchedState.keyId
              else
                this.potentialStatesById
            },
            knownStates = this.knownStates :+ newKnownState),
          Some(newKnownState)
        )
      } else (this, None)
    }

    /**
     * Wrapper to facilitate calling [[flushConflictingPotentialState]] for move events
     * @param event The move event to use as base for flushing
     * @return the updated ProcessingStatus
     */
    private def flushMoveConflictingPotentialState(
      event: PageEvent
    ): ProcessingStatus = {
      this.flushConflictingPotentialState(event, "move-conflict")._1
    }

    /**
     * Flushing function for delete events, handling restore flushing (see explanations below)
     * and potential flushing (see explanations in [[flushConflictingPotentialState]]).
     *
     * Similarly to flushing potential states (see [[flushConflictingPotentialState]]), we
     * need to to flush restore-states if they don't match by Id but match by title, as this
     * means the restore has generated a new pageId, and therefore page lineages are separate
     * for the restore and delete events.
     *
     * @param event The delete event to use as base for flush
     * @return The updated ProcessingStatus and the flushed conflicting state (if any,
     *         potential conflicting state if both potential and restore conflict)
     */
    private def flushDeleteConflictingStates(
      event: PageEvent
    ): (ProcessingStatus, Option[PageState]) = {

      // Flush potential restore matching by title and having a non-matching Id because before
      // pageId recycling change (meaning a restored page has a new pageId)
      val (status1, conflictingStateRestore) = {
        val key = event.fromKey
        if (this.restoredStatesByTitle.contains(key) && !event.hasKeyId) {
          val matchedState = this.restoredStatesByTitle(key)

          // pageCreationTimestamp propagation -- Special case if pageId is defined but keyId is
          // undefined because of timestamp boundaries:
          //  - If pageId match between event and state, propagate state pageCreation as it is the same page!
          //  - otherwise use restore state startTimestamp to maintain lineage timestamp coherence
          // Note : restore states have a defined startTimestamp, see [[addNewStateToStore]]
          val creationTimestamp = {
            if (event.pageId.exists(_ > 0) && event.pageId == matchedState.pageId)
              matchedState.pageCreationTimestamp
            else
              matchedState.startTimestamp
          }

          val newKnownState = matchedState.copy(
            pageCreationTimestamp = creationTimestamp,
            // if we have causedBy* fields on the state, they're the best guess as to the actor, so keep them
            inferredFrom = Some("delete-conflict"),
            sourceLogId = Some(event.sourceLogId),
            sourceLogComment = Some(event.sourceLogComment),
            sourceLogParams = Some(event.sourceLogParams)
          )
          // Remove conflicting state from restoreStates and add new known state
          (
            this.copy(
            restoredStatesByTitle = this.restoredStatesByTitle - matchedState.key,
            restoredStatesById = {
              if (matchedState.hasKeyId)
                this.restoredStatesById - matchedState.keyId
              else
                this.restoredStatesById
            },
            knownStates = this.knownStates :+ newKnownState),
            Some(newKnownState)
          )
        } else (this, None)
      }

      // Flush potential state matching by title
      val (status2, conflictingStatePotential) = status1.flushConflictingPotentialState(event, "delete-conflict")

      // Use conflicting state from potential if it exists, as this is the closest in time
      // (if there is both a potential and restore state, it means the potential has happened
      // after the restore, otherwise the restore would have flushed the potential).
      (status2, if (conflictingStatePotential.isDefined) conflictingStatePotential else conflictingStateRestore)
    }


    /**
     * Removes a matched state from either potential or restore states.
     * If state is both in potential and restore, remove from potential only.
     * Warning: This function will fail if matchedState is empty.
     * Note: Uses state.keyId and not matchedState.byId in order
     *       to clean byId states even if the match is made by title
     * @param matchedState The MatchedState to remove from states-store (must be defined)
     * @return The updated ProcessingStatus
     *
     */
    private def removeMatchedStateFromOrigin(
      matchedState: MatchedState
    ): ProcessingStatus = {
      // Will fail if matchedState is not defined
      val state = matchedState.get
      this.copy(
        potentialStatesByTitle = {
          if (matchedState.inPotential)
            this.potentialStatesByTitle - state.key
          else
            this.potentialStatesByTitle
        },
        potentialStatesById = {
          if (state.hasKeyId && matchedState.inPotential)
            this.potentialStatesById - state.keyId
          else
            this.potentialStatesById
        },
        restoredStatesByTitle = {
          if (matchedState.inRestore && !matchedState.inPotential)
            this.restoredStatesByTitle - state.key
          else
            this.restoredStatesByTitle
        },
        restoredStatesById = {
          if (state.hasKeyId && matchedState.inRestore && !matchedState.inPotential)
            this.restoredStatesById - state.keyId
          else
            this.restoredStatesById
        }
      )
    }

    /**
     * Create a new new known-state and adds it to the known-state-list.
     * @param event The event used to build the new known-state
     * @param matchedState The base state used for the new known-state
     * @return The updated ProcessingStatus
     */
    private def addKnownStateFromMatchedStateAndEvent(
      event: PageEvent,
      matchedState: PageState
    ): ProcessingStatus  = {
      this.copy(
        knownStates = this.knownStates :+ matchedState.copy(
          startTimestamp = Some(event.timestamp),
          causedByEventType = event.eventType,
          causedByUserId = event.causedByUserId,
          causedByUserCentralId = event.causedByUserCentralId,
          causedByAnonymousUser = event.causedByAnonymousUser,
          causedByTemporaryUser = event.causedByTemporaryUser,
          causedByPermanentUser = event.causedByPermanentUser,
          causedByUserText = event.causedByUserText,
          inferredFrom = None,
          sourceLogId = Some(event.sourceLogId),
          sourceLogComment = Some(event.sourceLogComment),
          sourceLogParams = Some(event.sourceLogParams)
        )
      )
    }

    /**
     * Create a new state and adds it to a destination store (potential, restore).
     * States are always stored in by-title stores,
     * and are stored in by-page_id stores only if the byId parameter is set to true.
     *
     * @param event The event used to build the new state
     * @param matchedState The base state used for the new state
     * @param newStateDestination Where to store the new state
     * @param byId Whether to store the new state in by-page_id stores or not
     *             (in addition to by-title-stores, always updated)
     * @return The updated ProcessingStatus
     */
    private def addNewStateToStore(
      event: PageEvent,
      matchedState: PageState,
      newStateDestination: StatusDestination.EnumVal,
      byId: Boolean
    ): ProcessingStatus = {
      // Special case for restore events: Assign a startTimestamp at restore time
      val startTimestamp = if (event.eventType == "restore") Some(event.timestamp) else matchedState.startTimestamp

      // New state to store in newStateDestination
      val newState = matchedState.copy(
        titleHistorical = event.oldTitle,
        namespaceHistorical = event.oldNamespace,
        namespaceIsContentHistorical = event.oldNamespaceIsContent,
        startTimestamp = startTimestamp,
        endTimestamp = Some(event.timestamp)
      )

      newStateDestination match {
        case StatusDestination.Potential => this.copy(
          potentialStatesByTitle = this.potentialStatesByTitle + (newState.key -> newState),
          potentialStatesById = if (byId) this.potentialStatesById + (newState.keyId -> newState)
          else this.potentialStatesById)
        case StatusDestination.Restore => this.copy(
          restoredStatesByTitle = this.restoredStatesByTitle + (newState.key -> newState),
          restoredStatesById = if (byId) this.restoredStatesById + (newState.keyId -> newState)
          else this.restoredStatesById)
      }
    }

    /**
     * Updates the current processingStatus with the given event, matchedState and destination.
     *
     * If a state is matched:
     *  - it is removed from its origin states-store
     *  - A new known-state is built using state and event, and added to knownStates store.
     *  - A new state is built using state and event and stored to the destionation parameter
     *    (possibly nowhere)
     *
     * Whether a state is matched or not, statistics are updated.
     *
     * @param event The event used to update the MatchedState
     * @param matchedState The MatchedState used as base-state for updating the ProcessingStatus
     * @param newStateDestination The StatusDestination where to store the new generated state
     * @return the updated ProcessingStatus
     */
    private def updateStatusWithEvent(
      event: PageEvent,
      matchedState: MatchedState,
      newStateDestination: StatusDestination.EnumVal
    ): ProcessingStatus = {
      if (matchedState.isDefined) { // There is a matching state
        val state = matchedState.get

        // Update OK statistics
        if (matchedState.byId) addOptionalStat(s"${event.wikiDb}.$METRIC_EVENTS_MATCHING_OK_BY_ID", 1)
        else addOptionalStat(s"${event.wikiDb}.$METRIC_EVENTS_MATCHING_OK_BY_TITLE", 1)

        this
          .removeMatchedStateFromOrigin(matchedState)
          .addKnownStateFromMatchedStateAndEvent(event, state)
          .addNewStateToStore(event, state, newStateDestination, matchedState.byId)

      } else {
        // No matching state - Update KO statistics and return unchanged ProcessingStatus
        if (matchedState.byId) addOptionalStat(s"${event.wikiDb}.$METRIC_EVENTS_MATCHING_KO_BY_ID", 1)
        else addOptionalStat(s"${event.wikiDb}.$METRIC_EVENTS_MATCHING_KO_BY_TITLE", 1)
        this
      }
    }

    /**
     * Looks for a deleted-state to initialize a deleted-lineage.
     *
     * When a page is deleted and never restored, it's information is lost (noticeably
     * its page_id). We rebuild some of this lost information using the archive table
     * in the form of original create PageStates (as if the page was live, but with the
     * is_deleted flag set to true, see
     * [[org.wikimedia.analytics.refinery.job.mediawikihistory.sql.DeletedPageViewRegistrar]]).
     * Those states are stored in the baseDeleted states-store and we link them link back
     * to delete events using this function.
     *
     * @param event The delete event looking for a create event match or the already existing
     *              conflicting state if it exists
     * @return The create-event match (if any) and the updated ProcessingStatus (with
     *         create-event removed from baseDeleted states-store)
     */
    private def getBaseDeletedPotentialState(
      event: PageEvent
    ): (Option[PageState], ProcessingStatus) = {
      // Using pageId if defined without time boundaries, as delete always have
      // the correct id (the one of the deleted page)
      if (event.pageId.exists(_ > 0) && this.baseDeletedStatesById.contains(event.keyId)) {
        val potentialState = this.baseDeletedStatesById(event.keyId)
        val newStatus = this.copy(
          baseDeletedStatesById = this.baseDeletedStatesById - potentialState.keyId,
          baseDeletedStatesByTitle = this.baseDeletedStatesByTitle - potentialState.key
        )
        (Some(potentialState), newStatus)
      } else if (this.baseDeletedStatesByTitle.contains(event.toKey)) {
        val potentialState = this.baseDeletedStatesByTitle(event.toKey)
        val newStatus = this.copy(
          baseDeletedStatesById = {
            if (potentialState.hasKeyId) this.baseDeletedStatesById - potentialState.keyId
            else this.baseDeletedStatesById
          },
          baseDeletedStatesByTitle = this.baseDeletedStatesByTitle - potentialState.key
        )
        (Some(potentialState), newStatus)
      } else (None, this)
    }

    /**
     * Update the given processingStatus with delete event:
     *  - Add a delete event to known-state (with existing or artificial pageId)
     *  - Add a new create event in potentialStates to match the delete event, either
     *    retrieved from baseDeleted states-store, or newly created.
     *
     * @param event The delete event for which a new lineage must be created
     * @param conflictState The (most ancient) conflicting state flushed from potential or restore
     *                      states-store (used to continue lineage in case of matching ids)
     * @return The updated processingStatus
     */
    private def updateStatusWithNewDeleteLineage(
      event: PageEvent,
      conflictState: Option[PageState]
    ): ProcessingStatus = {
      val byId = event.hasKeyId

      if (byId) addOptionalStat(s"${event.wikiDb}.$METRIC_EVENTS_MATCHING_OK_BY_ID", 1)
      else addOptionalStat(s"${event.wikiDb}.$METRIC_EVENTS_MATCHING_OK_BY_TITLE", 1)

      // Use conflictState if it is defined and matching byId (check pageId
      // existence instead of hasKeyId to cover cases where Id match even
      // if keyId is undefined because of time boundaries
      val useConflictState = conflictState.isDefined &&
        conflictState.get.pageId.exists(_ > 0) &&
        conflictState.get.pageId == event.pageId

      // Get a possible baseDeleted state and update the status (remove the baseDeleted from store)
      // Warning: This step is necessary even if a conflicting state exists, to clean the
      // baseDeleted states-store
      val (baseDeletedPotentialState, newStatus) = getBaseDeletedPotentialState(event)


      // Values to propagate as the new lineage matches an existing one
      // We define those values even if the potentialCreate event exists
      // to fill the new delete event
      val (isDeleted, creationTimestamp, firstEditTimestamp) = {
        if (useConflictState) {
          val cs = conflictState.get
          (cs.isDeleted, cs.pageCreationTimestamp, cs.pageFirstEditTimestamp)
        } else if (baseDeletedPotentialState.isDefined) {
          val ds = baseDeletedPotentialState.get
          (ds.isDeleted, ds.pageCreationTimestamp, ds.pageFirstEditTimestamp)
        } else (true, None, None)
      }

      // Special case for end timestamp: We use the start date of the conflicting state as end date
      // even if it doesn't match byId, since the delete `has an end`
      val deleteEndTimestamp = if (conflictState.isDefined) conflictState.get.startTimestamp else None

      // pageid and fakeId depend on baseDeletedState being defined
      val pageId = {
        if (baseDeletedPotentialState.isDefined) baseDeletedPotentialState.get.pageId
        else event.pageId
      }
      val fakeId = if (pageId.exists(_ > 0)) None else Some(randomUUID.toString)

      // New create-potential state is either from baseDeleted, or created anew
      val newPotentialCreateState: PageState = {
        if (baseDeletedPotentialState.isDefined) {
          baseDeletedPotentialState.get.copy(
            pageArtificialId = fakeId,
            endTimestamp = Some(event.timestamp)
          )
        } else {
            new PageState(
              wikiDb = event.wikiDb,
              pageId = pageId,
              pageArtificialId = fakeId,
              titleHistorical = event.oldTitle,
              title = event.oldTitle,
              namespaceHistorical = event.oldNamespace,
              namespaceIsContentHistorical = event.oldNamespaceIsContent,
              namespace = event.oldNamespace,
              namespaceIsContent = event.oldNamespaceIsContent,
              pageCreationTimestamp = creationTimestamp,
              pageFirstEditTimestamp = firstEditTimestamp,
              endTimestamp = Some(event.timestamp),
              causedByEventType = "create",
              inferredFrom = Some("delete"),
              sourceLogId = Some(event.sourceLogId),
              sourceLogComment = Some(event.sourceLogComment),
              sourceLogParams = Some(event.sourceLogParams),
              isDeleted = isDeleted)
        }
      }
      // New known delete state using values extracted from conflicting or baseDeleted states
      val newKnownDeleteState = new PageState(
        wikiDb = event.wikiDb,
        pageId = pageId,
        pageArtificialId = fakeId,
        titleHistorical = event.oldTitle,
        title = event.oldTitle,
        namespaceHistorical = event.oldNamespace,
        namespaceIsContentHistorical = event.oldNamespaceIsContent,
        namespace = event.oldNamespace,
        namespaceIsContent = event.oldNamespaceIsContent,
        pageCreationTimestamp = creationTimestamp,
        pageFirstEditTimestamp = firstEditTimestamp,
        startTimestamp = Some(event.timestamp),
        endTimestamp = deleteEndTimestamp,
        causedByEventType = "delete",
        causedByUserId = event.causedByUserId,
        causedByUserCentralId = event.causedByUserCentralId,
        causedByAnonymousUser = event.causedByAnonymousUser,
        causedByTemporaryUser = event.causedByTemporaryUser,
        causedByPermanentUser = event.causedByPermanentUser,
        causedByUserText = event.causedByUserText,
        isDeleted = isDeleted,
        sourceLogId = Some(event.sourceLogId),
        sourceLogComment = Some(event.sourceLogComment),
        sourceLogParams = Some(event.sourceLogParams)
      )
      newStatus.copy(
        potentialStatesByTitle = newStatus.potentialStatesByTitle + (event.fromKey -> newPotentialCreateState),
        potentialStatesById = {
          if (byId) newStatus.potentialStatesById + (event.keyId -> newPotentialCreateState)
          else newStatus.potentialStatesById
        },
        knownStates = newStatus.knownStates :+ newKnownDeleteState
      )
    }

    /**
     * Updates a processing status with an event (Used in [[processSubgraph]]
     * through a foldLeft).
     *
     * The function first flushes (moves to known states) a joinable state
     * (event.toKey == state.key) if its page creation timestamp is after the event timestamp.
     * It then applies the correct function depending on event type and return its result.
     *
     * @param event The event used to update the processing status.
     * @return The updated processing status.
     */
    def processEvent(
      event: PageEvent
    ): ProcessingStatus = {
      // Flush state if its creation is greater than the event's timestamp.
      val status1 = this.flushEventBeforeCreation(event)

      // Apply event type related function
      event.eventType match {

        case "move"  =>
          val status2 = status1.flushMoveConflictingPotentialState(event)
          val matchedState = status2.getMatchedState(event)
          status2.updateStatusWithEvent(event, matchedState, StatusDestination.Potential)

        case "delete" =>
          val (status2, conflictState) = status1.flushDeleteConflictingStates(event)
          val matchedState = status2.getMatchedState(event)
          // If the delete can be joined to a restore, force it by setting potentialState to None
          if (matchedState.inRestore)
            status2.updateStatusWithEvent(event, matchedState.copy(potentialState=None), StatusDestination.Potential)
          // Else create a new page-lineage associated to the delete
          else
            status2.updateStatusWithNewDeleteLineage(event, conflictState)

        case "restore" =>
          // When a restore happens, store it in restoredState to be matched against a delete
          // Exception: if there is already a restoredState for that title AND a potentialState, store
          // the restored state as potential state: the currently ongoing lineage is a different page
          // than the one in restoredState which might be linked to a delete later on.
          val matchedState = status1.getMatchedState(event)
          val dest = {
            if (matchedState.inPotential && matchedState.inRestore)
              StatusDestination.Potential
            else
              StatusDestination.Restore
          }
          status1.updateStatusWithEvent(event, matchedState, dest)

        case "merge" =>
          val matchedState = status1.getMatchedState(event)
          status1.updateStatusWithEvent(event, matchedState, StatusDestination.Potential)

        case "create-page" =>
          val matchedState = status1.getMatchedState(event)
          status1.updateStatusWithEvent(event, matchedState, StatusDestination.Potential)

        case _ =>
          // Update statistics and add events to unmatched
          val matchedStateOrigin = status1.getMatchedState(event)
          if (matchedStateOrigin.byId) addOptionalStat(s"${event.wikiDb}.$METRIC_EVENTS_MATCHING_KO_BY_ID", 1)
          else addOptionalStat(s"${event.wikiDb}.$METRIC_EVENTS_MATCHING_KO_BY_TITLE", 1)
          status1.addUnmatchedEvent(event)
      }
    }
  }

  /**
    * Propagates page creation and first edit timestamps. It groups by page id
    * (or artificial page id) and sort states by startTimestamp, endTimestamp
    * in an ascending way. It then checks for pageFirstEditTimestamp correctness:
    * first state of the sorted list (normally a create state) should have its
    * startTimestamp matching the pageFirstEditTimestamp. If it doesn't, update
    * pageFirstEditTimestamp to the state startTimestamp. the function finally
    * loops through all the states, propagating the page creation timestamp
    * and first edit timestamp values.
    *
    * @param states The states sequence to propagate page creation and first edit on
    * @return The state sequence with update page creation and first edit timestamps
    */
  def propagatePageCreationAndFirstEdit(states: Seq[PageState]): Seq[PageState] = {
    states
      .groupBy(s => (s.pageId, s.pageArtificialId))
      .flatMap {
        case (pageIds, pageStates) =>
          val sortedStates = pageStates.toList.sortBy(state =>
            (state.startTimestamp.getOrElse(new Timestamp(Long.MinValue)).getTime,
              state.endTimestamp.getOrElse(new Timestamp(Long.MaxValue)).getTime))
          // Flag first state using pageFirstState
          val firstState = sortedStates.head.copy(pageFirstState = true)
          // Values to propagate, enforcing pageCreation to match firstState timestamp if empty
          val pageCreation = {
            if (firstState.pageCreationTimestamp.isEmpty) firstState.startTimestamp
            else firstState.pageCreationTimestamp
          }
          val firstEdit = firstState.pageFirstEditTimestamp

          // Loop over all states to update first one as well
          (Seq(firstState) ++ sortedStates.tail).map(s => s.copy(
            // [[PageState]]
            pageCreationTimestamp = pageCreation,
            pageFirstEditTimestamp = firstEdit
          ))
      }
      .toSeq
  }

  /**
    * This function rebuilds page history for a single
    * partition given as events and states iterators.
    *
    * It does so by
    *  - building an initial [[ProcessingStatus]] from the given
    *    states. Every state not being deleted (is_deleted = false) is a potential one,
   *     and every state being deleted is a baseDeleted one (to be used with delete events)
    *  - sorting events by timestamp in descending orders
    *  - Iterate through the sorted events and join them to the states.
    *    Done using foldLeft with a [[ProcessingStatus]] as aggregated value.
    *    Depending on the event type (move, delete, restore), status is
    *    updated (new potential state created, current mark as complete ...)
    *  - Update still-to-join states to known states when there is no more events
    *  - Reiterate over resulting states in time-ascending order to propagate
    *    values that can't be propagated backward.
    *
    * @param events The page events iterable
    * @param states The page states iterable
    * @return The reconstructed page state history (for the given partition) and errors
    */
  def processSubgraph(
    events: Iterable[PageEvent],
    states: Iterable[PageState]
  ): (
    Seq[PageState], // processed states
    Seq[PageEvent] // unmatched events
  ) = {
    val sortedEvents = events.toList.sortWith {
      case (a, b) =>
        a.timestamp.after(b.timestamp) ||
        (
          a.timestamp.equals(b.timestamp) &&
          (
            // Sort move events to be processed first,
            // thus avoiding confusion around "move_redir" events.
            (a.eventType == "move" && b.eventType != "move") ||
            // If not in the case move/other events, use sourceLogId
            (!(a.eventType != "move" && b.eventType == "move") && a.sourceLogId > b.sourceLogId)
          )
        )
    }
    val (fStates: Seq[PageState], unmatchedEvents: Seq[PageEvent]) = {
      if (sortedEvents.isEmpty) {
        val finalStates = states.map(s => s.copy(
          // [[PageState]]
          startTimestamp = s.pageFirstEditTimestamp,
          inferredFrom = Some("unclosed")
        )).toSeq
        (finalStates, Seq.empty[PageEvent])
      } else {
        // Differenciate baseDeleted states from regular ones
        val nonDeletedStates = states.filter(!_.isDeleted)
        val deletedStates = states.filter(_.isDeleted)
            // TODO: Update algorithm to use deleted-states having the same title
            //       https://phabricator.wikimedia.org/T320860
            //       As a mitigation, sort deleted states to provide consistent result
            .toList.sortBy(s => {
              (s.key, s.startTimestamp.map(_.getTime).getOrElse(0L), s.pageId)
            })

        val initialStatus = new ProcessingStatus(
          potentialStatesByTitle = nonDeletedStates.map(s => s.key -> s).toMap,
          potentialStatesById = nonDeletedStates.filter(_.hasKeyId).map(s => s.keyId -> s).toMap,
          restoredStatesByTitle = Map.empty[PageHistoryBuilder.KEY, PageState],
          restoredStatesById = Map.empty[PageHistoryBuilder.KEYID, PageState],
          baseDeletedStatesByTitle = deletedStates.map(s => s.key -> s).toMap,
          baseDeletedStatesById = deletedStates.filter(_.hasKeyId).map(s => s.keyId -> s).toMap,
          knownStates = Seq.empty[PageState],
          unmatchedEvents = Seq.empty[PageEvent]
        )
        val finalStatus = sortedEvents.foldLeft(initialStatus)((status, event) => status.processEvent(event))
        val unmatchedStates = finalStatus.potentialStatesByTitle.values ++
                              finalStatus.restoredStatesByTitle.values ++
                              finalStatus.baseDeletedStatesByTitle.values
        val finalStates = finalStatus.knownStates ++
          unmatchedStates.map(s => s.copy(startTimestamp = s.pageFirstEditTimestamp))
        (finalStates, finalStatus.unmatchedEvents)
      }
    }
    (propagatePageCreationAndFirstEdit(fStates), unmatchedEvents)

  }

  /**
    * This function is the entry point of this class.
    * It first partitions events and states RDDs using
    * [[org.wikimedia.analytics.refinery.spark.utils.SubgraphPartitioner]],
    * then applies its [[processSubgraph]] method to every partition, and finally returns joined
    * states results, along with error events or their count.
    *
    * @param events The page events RDD to be used for reconstruction
    * @param states The initial page states RDD to be used for reconstruction
    * @return The reconstructed page states history and errors.
    */
  def run(
    events: RDD[PageEvent],
    states: RDD[PageState]
  ): (
    RDD[PageState],
    RDD[PageEvent]
  ) = {
    log.info(s"Page history building jobs starting")

    val partitioner = new SubgraphPartitioner[
      PageHistoryBuilder.KEY,
      PageHistoryBuilder.STATS_GROUP,
      PageEvent,
      PageState
    ](
      spark,
      PageHistoryBuilder.PageRowKeyFormat,
      statsAccumulator
    )
    val subgraphs = partitioner.run(events, states)

    log.info(s"Processing partitioned page histories")
    val processedSubgraphs = subgraphs.map(g => processSubgraph(g._1, g._2))
    val processedStates = processedSubgraphs.flatMap(_._1)
    val matchingErrors = processedSubgraphs.flatMap(_._2)

    log.info(s"Page history building jobs done")

    (processedStates, matchingErrors)
  }
}

/**
  * This companion object defines a shortening for page reconstruction key type,
  * and the needed conversions of this type between RDD and dataframe to
  * be used in graph partitioning.
  */
object PageHistoryBuilder extends Serializable {

  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types._
  import org.wikimedia.analytics.refinery.spark.utils.RowKeyFormat

  type KEY = (String, String, Int)
  type KEYID = (String, Long)
  type STATS_GROUP = String

  val METRIC_SUBGRAPH_PARTITIONS = "pageHistory.subgraphPartitions"

  object PageRowKeyFormat extends RowKeyFormat[KEY, STATS_GROUP] with Serializable {
    val struct = StructType(Seq(
      StructField("wiki_db", StringType, nullable = false),
      StructField("page_title_historical", StringType, nullable = false),
      StructField("page_namespace_historical", IntegerType, nullable = false)
    ))
    def toRow(k: KEY): Row = Row.fromTuple(k)
    def toKey(r: Row): KEY = (r.getString(0), r.getString(1), r.getInt(2))
    def statsGroup(k: KEY): STATS_GROUP = s"${k._1}.$METRIC_SUBGRAPH_PARTITIONS"
  }

}
