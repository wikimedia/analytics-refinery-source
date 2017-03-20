package org.wikimedia.analytics.refinery.job.mediawikihistory.page

import org.apache.spark.sql.SQLContext


/**
  * This class implements the core algorithm of the page history reconstruction.

  * The [[run]] function first partitions the [[PageState]] and [[PageEvent]] RDDs
  * using [[org.wikimedia.analytics.refinery.job.mediawikihistory.utils.SubgraphPartitioner]],
  * then applies its [[processSubgraph]] method to every partition.
  *
  * It returns the [[PageState]] RDD of joined results of every partition and either
  * the errors found on the way, or their count.
  */
class PageHistoryBuilder(sqlContext: SQLContext) extends Serializable {

  import org.apache.log4j.Logger
  import java.util.UUID.randomUUID
  import org.apache.spark.rdd.RDD
  import org.wikimedia.analytics.refinery.job.mediawikihistory.utils.SubgraphPartitioner

  @transient
  lazy val log: Logger = Logger.getLogger(this.getClass)


  /**
    * This case class contains the various state dictionary and list needed to
    * reconstruct the page history, as well as errors.
    * An object of this class is updated and passed along reconstruction,
    * allowing to move a single object instead of the four it contains.
    *
    * @param potentialStates States that can be joined to events
    * @param restoredStates States from restore events
    * @param knownStates States that have already been joined to events
    * @param unmatchedEvents Events having not match any state (or their count)
    */
  case class ProcessingStatus(
    potentialStates: Map[PageHistoryBuilder.KEY, PageState],
    restoredStates: Map[PageHistoryBuilder.KEY, PageState],
    knownStates: Seq[PageState],
    unmatchedEvents: Either[Long, Seq[PageEvent]]
  )


  /**
    * Updates a processing status with a move type event
    *
    *
    * @param status The processing status to update
    * @param event The move event to use
    * @return The updated processing status
    */
  private def processMoveEvent(
      status: ProcessingStatus,
      event: PageEvent
  ): ProcessingStatus = {
    // Flush potential state conflicting with the event (event.fromKey = state.key)
    val fromKey = event.fromKey
    val status1 = if (status.potentialStates.contains(fromKey)) {
      val state = status.potentialStates(fromKey)
      status.copy(
        potentialStates = status.potentialStates - fromKey,
        knownStates = status.knownStates :+ state.copy(
          startTimestamp = Some(event.timestamp),
          pageCreationTimestamp = Some(event.timestamp),
          causedByEventType = "create",
          causedByUserId = None,
          inferredFrom = Some("move-conflict")
        )
      )
    } else status

    val toKey = event.toKey
    if (status1.potentialStates.contains(toKey)) {
      // Event-state match, move potential to known state
      // and create new potentialState with old values
      val state = status1.potentialStates(toKey)
      val newPotentialState = state.copy(
          title = event.oldTitle,
          namespace = event.oldNamespace,
          namespaceIsContent = event.oldNamespaceIsContent,
          endTimestamp = Some(event.timestamp)
      )
      val newKnownState = state.copy(
          startTimestamp = Some(event.timestamp),
          causedByEventType = event.eventType,
          causedByUserId = event.causedByUserId,
          inferredFrom = None
      )
      status1.copy(
        potentialStates = status1.potentialStates - toKey + (event.fromKey -> newPotentialState),
        knownStates = status1.knownStates :+ newKnownState
      )
    } else if (status1.restoredStates.contains(toKey)) {
      // No potential state but a restore R (waiting for a matching delete).
      //
      // Create a new lineage for this move with:
      // - new potential state with move old values
      // - new known move state from old values to new ones
      // - new known deleted state with new values when R happens
      //  (when the R restore happens, the created page from this move
      //  has the same name --> it's deleted)
      val state = status1.restoredStates(toKey)
      val fakeId = randomUUID.toString
      val newPotentialState = new PageState(
        wikiDb = event.wikiDb,
        pageId = event.pageId,
        pageIdArtificial = Some(fakeId),
        title = event.oldTitle,
        titleLatest = event.newTitleWithoutPrefix,
        namespace = event.oldNamespace,
        namespaceIsContent = event.oldNamespaceIsContent,
        namespaceLatest = event.newNamespace,
        namespaceIsContentLatest = event.newNamespaceIsContent,
        endTimestamp = Some(event.timestamp),
        causedByEventType = "move"
      )
      val newKnownMoveState = new PageState(
        wikiDb = event.wikiDb,
        pageId = event.pageId,
        pageIdArtificial = Some(fakeId),
        title = event.newTitleWithoutPrefix,
        titleLatest = event.newTitleWithoutPrefix,
        namespace = event.newNamespace,
        namespaceIsContent = event.newNamespaceIsContent,
        namespaceLatest = event.newNamespace,
        namespaceIsContentLatest = event.newNamespaceIsContent,
        startTimestamp =Some(event.timestamp),
        endTimestamp = state.endTimestamp,
        causedByEventType = "move",
        causedByUserId = event.causedByUserId
      )
      val newKnownDeleteState = new PageState(
        wikiDb = event.wikiDb,
        pageId = event.pageId,
        pageIdArtificial = Some(fakeId),
        title = event.newTitleWithoutPrefix,
        titleLatest = event.newTitleWithoutPrefix,
        namespace = event.newNamespace,
        namespaceIsContent = event.newNamespaceIsContent,
        namespaceLatest = event.newNamespace,
        namespaceIsContentLatest = event.newNamespaceIsContent,
        startTimestamp = state.endTimestamp,
        causedByEventType = "delete",
        causedByUserId = event.causedByUserId,
        inferredFrom = Some("restore")
      )
      status1.copy(
        potentialStates = status1.potentialStates + (event.fromKey -> newPotentialState),
        knownStates = status1.knownStates :+ newKnownMoveState :+ newKnownDeleteState
      )

    } else {
      // No event match - updating errors
      status1.copy(unmatchedEvents = status1.unmatchedEvents match {
        case Left(c) => Left(c + 1)
        case Right(l) => Right(l :+ event)
      })
    }
  }


  /**
    * Updates a processing status with a delete type event
    *
    * @param status The processing status to update
    * @param event The delete event to use
    * @return The updated processing status
    */
  private def processDeleteEvent(
      status: ProcessingStatus,
      event: PageEvent
  ): ProcessingStatus = {
    val fromKey = event.fromKey
    // We assume this delete is the one that have been restored
    if (status.restoredStates.contains(fromKey)) {

      // If there is a current potential state, it is flushed by the restore one
      val status1 = if (status.potentialStates.contains(fromKey)) {
        val overwrittenState = status.potentialStates(fromKey)
        val newKnownState = overwrittenState.copy(
          startTimestamp = Some(event.timestamp),
          causedByEventType = event.eventType,
          causedByUserId = event.causedByUserId,
          inferredFrom = Some("restore")
        )

        status.copy(
          potentialStates = status.potentialStates - fromKey,
          knownStates = status.knownStates :+ newKnownState
        )
      } else status

      // Remove restored state and add it as delete in known states,
      // and add new potential state
      val restoredState = status1.restoredStates(fromKey)
      val deleteState = restoredState.copy(
        startTimestamp = Some(event.timestamp),
        causedByEventType = "delete",
        inferredFrom = None
      )
      val newPotentialState = restoredState.copy(
        endTimestamp = Some(event.timestamp),
        inferredFrom = None
      )
      status1.copy(
        potentialStates = status1.potentialStates + (event.fromKey -> newPotentialState),
        restoredStates = status.restoredStates - fromKey,
        knownStates = status1.knownStates :+ deleteState
      )

    } else {
      // This delete is not restored
      val status1 = if (status.potentialStates.contains(fromKey)) {
        // Flush potential state conflicting with the event (event.fromKey = state.key)
        val state = status.potentialStates(fromKey)
        status.copy(
          potentialStates = status.potentialStates - fromKey,
          knownStates = status.knownStates :+ state.copy(
            startTimestamp = Some(event.timestamp),
            pageCreationTimestamp = Some(event.timestamp),
            causedByEventType = "create",
            causedByUserId = None,
            inferredFrom = Some("delete-conflict")
          )
        )
      } else status

      // Since the deleted page has no restore, we can't retrieve it's real id
      // We therefore assign a new fake Id to its lineage
      val fakeId = randomUUID.toString

      // Create a new known delete state and new potential create one
      val newPotentialCreateState = new PageState(
        wikiDb = event.wikiDb,
        pageId = event.pageId,
        pageIdArtificial = Some(fakeId),
        title = event.oldTitle,
        titleLatest = event.oldTitle,
        namespace = event.oldNamespace,
        namespaceIsContent = event.oldNamespaceIsContent,
        namespaceLatest = event.oldNamespace,
        namespaceIsContentLatest = event.oldNamespaceIsContent,
        endTimestamp = Some(event.timestamp),
        causedByEventType = "create",
        inferredFrom = Some("delete")
      )
      val newKnownDeleteState = new PageState(
        wikiDb = event.wikiDb,
        pageId = event.pageId,
        pageIdArtificial = Some(fakeId),
        title = event.oldTitle,
        titleLatest = event.oldTitle,
        namespace = event.oldNamespace,
        namespaceIsContent = event.oldNamespaceIsContent,
        namespaceLatest = event.oldNamespace,
        namespaceIsContentLatest = event.oldNamespaceIsContent,
        startTimestamp = Some(event.timestamp),
        causedByEventType = "delete",
        causedByUserId = event.causedByUserId
      )
      status1.copy(
        potentialStates = status1.potentialStates + (event.fromKey -> newPotentialCreateState),
        knownStates = status1.knownStates :+ newKnownDeleteState
      )
    }
  }


  /**
    * Updates a processing status with a restore type event
    *
    * @param status The processing status to update
    * @param event The restore event to use
    * @return The updated processing status
    */
  private def processRestoreEvent(
                                  status: ProcessingStatus,
                                  event: PageEvent
                                ): ProcessingStatus = {
    val toKey = event.toKey
    if (status.restoredStates.contains(toKey)) {
      // Conflicting restored state, flush current and update to new
      val conflictingRestoredState = status.restoredStates(toKey)
      status.copy(
        restoredStates = status.restoredStates - toKey + (toKey -> conflictingRestoredState.copy(
          endTimestamp = Some(event.timestamp),
          causedByUserId = event.causedByUserId
        )),
        knownStates = status.knownStates :+ conflictingRestoredState.copy(
          startTimestamp = Some(event.timestamp),
          causedByEventType = "restore",
          inferredFrom = Some("restore-conflict")
        )
      )
    } else if (status.potentialStates.contains(toKey)) {
      // Event-state match, move state to known state,
      // remove it from potential states,
      // and create new restoreState
      val state = status.potentialStates(toKey)
      val newRestoredState = state.copy(
        endTimestamp = Some(event.timestamp),
        inferredFrom = Some(event.eventType)
      )
      val newKnownState = state.copy(
        startTimestamp = Some(event.timestamp),
        causedByEventType = event.eventType,
        causedByUserId = event.causedByUserId,
        inferredFrom = None
      )
      status.copy(
        potentialStates = status.potentialStates - toKey,
        restoredStates = status.restoredStates + (toKey -> newRestoredState),
        knownStates = status.knownStates :+ newKnownState
      )
    } else {
      // No event match - updating errors
      status.copy(unmatchedEvents = status.unmatchedEvents match {
        case Left(c) => Left(c + 1)
        case Right(l) => Right(l :+ event)
      })
    }
  }

  /**
    * Updates a processing status with an event (Used in [[processSubgraph]]
    * through a foldLeft).
    *
    * The function first flushes (moves to known states) a joinable
    * state (event.toKey == state.key) if its page creation timestamp
    * is after the event timestamp.
    * It then applies the correct function depending on event type and return
    * its result.
    *
    * @param status The processing status to update - It contains
    *               the states to potentially join to
    * @param event The event used to update the processing status.
    * @return The updated processing status.
    */
  def processEvent(
    status: ProcessingStatus,
    event: PageEvent
  ): ProcessingStatus = {
    // Flush state if its creation is greater than the event's timestamp.
    val toKey = event.toKey
    val status1 = if (
      status.potentialStates.contains(toKey) &&
      event.timestamp < status.potentialStates(toKey).pageCreationTimestamp.getOrElse("-1")
    ) {
      val state = status.potentialStates(toKey)
      status.copy(
        potentialStates = status.potentialStates - toKey,
        knownStates = status.knownStates :+ state.copy(
          startTimestamp = state.pageCreationTimestamp
        )
      )
    } else status
    // Apply event type related function
    event.eventType match {
      case "move" => processMoveEvent(status1, event)
      case "delete" => processDeleteEvent(status1, event)
      case "restore" => processRestoreEvent(status1, event)
      case _ => status1.copy(unmatchedEvents = status1.unmatchedEvents match {
        case Left(c) => Left(c + 1)
        case Right(l) => Right(l :+ event)
      })
    }
  }

  /**
    * Function propagating page creation timestamp. It groups by page id
    * (or artificial page id), sort states by startTimestamp, endTimestamp
    * in an ascending way, and loop through them propagating the page creation
    * timestamp value.
    *
    * @param states The states sequence to propagate page creation on
    * @return The state sequence with update page creation timestamps
    */
  def propagatePageCreation(states: Seq[PageState]): Seq[PageState] = {
    states
      .groupBy(s => (s.pageId, s.pageIdArtificial))
      .flatMap {
        case (pageIds, pageStates) =>
          val sortedStates = pageStates.toList.sortWith {
            case (a, b) =>
              a.startTimestamp.isEmpty || (
                  b.startTimestamp.isDefined &&
                    a.startTimestamp.get < b.startTimestamp.get
              )
          }
          val pageCreationTimestamp = sortedStates.head.pageCreationTimestamp
          sortedStates.map(s => s.copy(pageCreationTimestamp = pageCreationTimestamp))
      }
      .toSeq
  }


  /**
    * This function rebuilds page history for a single
    * partition given as events and states iterators.
    *
    * It does so by
    *  - building an initial [[ProcessingStatus]] from the given
    *    states (every state being a potential one)
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
    * @param outputErrors Whether to output error events or their count
    * @return The reconstructed page state history (for the given partition)
    *         and errors or their count
    */
  def processSubgraph(
    events: Iterable[PageEvent],
    states: Iterable[PageState],
    outputErrors: Boolean = false
  ): (
    Seq[PageState], // processed states
    Either[Long, Seq[PageEvent]] // unmatched events
  ) = {
    val statesMap = states.map(s => s.key -> s).toMap
    val sortedEvents = events.toList.sortWith {
      case (a, b) =>
        a.timestamp > b.timestamp ||
        // Sort move events to be processed first,
        // thus avoiding confusion around "move_redir" events.
        (
          a.timestamp == b.timestamp &&
          a.eventType == "move" &&
          b.eventType != "move"
        )
    }
    val initialStatus = new ProcessingStatus(
      potentialStates = statesMap,
      restoredStates = Map.empty[PageHistoryBuilder.KEY, PageState],
      knownStates = Seq.empty[PageState],
      unmatchedEvents = if (outputErrors) Right(Seq.empty[PageEvent]) else Left(0L)
    )
    val finalStatus = sortedEvents.foldLeft(initialStatus)(processEvent)
    val finalStates = finalStatus.knownStates ++
      finalStatus.potentialStates.values.map(s => s.copy(startTimestamp = s.pageCreationTimestamp))
    (propagatePageCreation(finalStates), finalStatus.unmatchedEvents)
  }

  /**
    * This function is the entry point of this class.
    * It first partitions events and states RDDs using
    * [[org.wikimedia.analytics.refinery.job.mediawikihistory.utils.SubgraphPartitioner]],
    * then applies its [[processSubgraph]] method to every partition, and finally returns joined
    * states results, along with error events or their count.
    *
    * @param events The page events RDD to be used for reconstruction
    * @param states The initial page states RDD to be used for reconstruction
    * @param outputErrors Whether to output error events or their count
    * @return The reconstructed page states history and errors or their count.
    */
  def run(
    events: RDD[PageEvent],
    states: RDD[PageState],
    outputErrors: Boolean = false
  ): (
    RDD[PageState],
    Either[Long, RDD[PageEvent]]
  ) = {
    log.info(s"Page history building jobs starting")

    val partitioner = new SubgraphPartitioner[PageHistoryBuilder.KEY, PageEvent, PageState](sqlContext, PageHistoryBuilder.PageRowKeyFormat)
    val subgraphs = partitioner.run(events, states)

    log.info(s"Processing partitioned page histories")
    val processedSubgraphs = subgraphs.map(g => processSubgraph(g._1, g._2, outputErrors))
    val processedStates = processedSubgraphs.flatMap(_._1)
    val matchingErrors =
      if (outputErrors) Right(processedSubgraphs.flatMap(_._2.right.get))
      else Left(processedSubgraphs.map(_._2.left.get).sum.toLong)

    log.info(s"Page history building jobs done")

    (processedStates, matchingErrors)
  }
}

/**
  * This companion object defines a shortening for page reconstruction key type,
  * and the needed conversions of this type between RDD and dataframe to
  * be used in graph partitioning.
  */
object PageHistoryBuilder extends Serializable{

  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types._
  import org.wikimedia.analytics.refinery.job.mediawikihistory.utils.RowKeyFormat

  type KEY = (String, String, Int)

  object PageRowKeyFormat extends RowKeyFormat[KEY] with Serializable {
    val struct = StructType(Seq(
      StructField("wiki_db", StringType, nullable = false),
      StructField("page_title", StringType, nullable = false),
      StructField("page_namespace", IntegerType, nullable = false)
    ))
    def toRow(k: KEY): Row = Row.fromTuple(k)
    def toKey(r: Row): KEY = (r.getString(0), r.getString(1), r.getInt(2))
  }
}