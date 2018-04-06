package org.wikimedia.analytics.refinery.job.mediawikihistory.page

import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.spark.utils.MapAccumulator


/**
  * This class implements the core algorithm of the page history reconstruction.

  * The [[run]] function first partitions the [[PageState]] and [[PageEvent]] RDDs
  * using [[org.wikimedia.analytics.refinery.spark.utils.SubgraphPartitioner]],
  * then applies its [[processSubgraph]] method to every partition.
  *
  * It returns the [[PageState]] RDD of joined results of every partition and either
  * the errors found on the way, or their count.
  */
class PageHistoryBuilder(
                          spark: SparkSession,
                          statsAccumulator: MapAccumulator[String, Long]
                        ) extends Serializable {

  import org.apache.log4j.Logger
  import java.util.UUID.randomUUID
  import java.sql.Timestamp
  import org.apache.spark.rdd.RDD
  import org.wikimedia.analytics.refinery.spark.utils.SubgraphPartitioner

  @transient
  lazy val log: Logger = Logger.getLogger(this.getClass)

  val METRIC_EVENTS_MATCHING_OK = "pages.eventsMatching.OK"
  val METRIC_EVENTS_MATCHING_KO = "pages.eventsMatching.KO"

  /**
    * This case class contains the various state dictionary and list needed to
    * reconstruct the page history, as well as errors.
    * An object of this class is updated and passed along reconstruction,
    * allowing to move a single object instead of the four it contains.
    *
    * @param potentialStates States that can be joined to events
    * @param restoredStates States from restore events
    * @param knownStates States that have already been joined to events
    * @param unmatchedEvents Events having not match any state
    */
  case class ProcessingStatus(
    potentialStates: Map[PageHistoryBuilder.KEY, PageState],
    restoredStates: Map[PageHistoryBuilder.KEY, PageState],
    knownStates: Seq[PageState],
    unmatchedEvents: Seq[PageEvent]
  )


  /**
    * Updates a processing status with a move type event
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
    // Uses original pageCreationTimestamp if it happens 2 seconds or less before move event timestamp
    val fromKey = event.fromKey
    val (status1, event1) = if (status.potentialStates.contains(fromKey)) {
      val state = status.potentialStates(fromKey)
      val timeDiff = event.timestamp.getTime - state.pageCreationTimestamp.getOrElse(new Timestamp(Long.MinValue)).getTime
      val newEvent = {
        if (timeDiff >= 0 && timeDiff <= 2000) {
          event.copy(timestamp = state.pageCreationTimestamp.getOrElse(new Timestamp(0L)))
        } else {
          event
        }
      }
      val newKnownState = state.copy(
        startTimestamp = Some(newEvent.timestamp),
        pageCreationTimestamp = Some(newEvent.timestamp),
        causedByEventType = "create",
        inferredFrom = Some("move-conflict")
      )
      (
        status.copy(
            potentialStates = status.potentialStates - fromKey,
            knownStates = status.knownStates :+ newKnownState),
        newEvent
      )
    } else (status, event)

    val toKey = event1.toKey
    if (status1.restoredStates.contains(toKey)) {
      // Move happening after a restore
      // Flush move event and make restoredState a potential one
      statsAccumulator.add((s"${event1.wikiDb}.$METRIC_EVENTS_MATCHING_OK", 1))
      val state = status1.restoredStates(toKey)
      val newPotentialState = state.copy(
        titleHistorical = event1.oldTitle,
        namespaceHistorical = event1.oldNamespace,
        namespaceIsContentHistorical = event1.oldNamespaceIsContent,
        endTimestamp = Some(event1.timestamp)
      )
      val newKnownState = state.copy(
        startTimestamp = Some(event1.timestamp),
        causedByEventType = event1.eventType,
        causedByUserId = event1.causedByUserId,
        inferredFrom = None
      )
      status1.copy(
        potentialStates = status1.potentialStates + (event1.fromKey -> newPotentialState),
        restoredStates = status1.restoredStates - toKey,
        knownStates = status1.knownStates :+ newKnownState
      )
    } else if (status1.potentialStates.contains(toKey)) {
      // Event-state match, move potential to known state
      // and create new potentialState with old values
      statsAccumulator.add((s"${event1.wikiDb}.$METRIC_EVENTS_MATCHING_OK", 1))
      val state = status1.potentialStates(toKey)
      val newPotentialState = state.copy(
          titleHistorical = event1.oldTitle,
          namespaceHistorical = event1.oldNamespace,
          namespaceIsContentHistorical = event1.oldNamespaceIsContent,
          endTimestamp = Some(event1.timestamp)
      )
      val newKnownState = state.copy(
          startTimestamp = Some(event1.timestamp),
          causedByEventType = event1.eventType,
          causedByUserId = event1.causedByUserId,
          inferredFrom = None
      )
      status1.copy(
        potentialStates = status1.potentialStates - toKey + (event1.fromKey -> newPotentialState),
        knownStates = status1.knownStates :+ newKnownState
      )
    } else {
      // No event match - updating errors
      statsAccumulator.add((s"${event1.wikiDb}.$METRIC_EVENTS_MATCHING_KO", 1))
      status1.copy(unmatchedEvents = status1.unmatchedEvents :+ event)
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
    val (status1, event1) = if (status.restoredStates.contains(fromKey)) {
      // Delete comes just before a restore - add create at restore time
      // Notice: Not possible to have a current potential state - by construction
      // Move restored state to known state updating it to create with restore start timestamp
      // Uses original pageCreationTimestamp if it happens 2 seconds or less before restore start timestamp
      val state = status.restoredStates(fromKey)
      val newKnownState = state.copy(
        startTimestamp = state.endTimestamp,
        pageCreationTimestamp = state.endTimestamp,
        inferredFrom = Some("restore")
      )
      (
        status.copy(
          restoredStates = status.restoredStates - fromKey,
          knownStates = status.knownStates :+ newKnownState),
        event
        )
    } else if (status.potentialStates.contains(fromKey)) {
      // Flush potential state conflicting with the event (event.fromKey = state.key)
      // Uses original pageCreationTimestamp if it happens 2 seconds or less before delete event timestamp
      val state = status.potentialStates(fromKey)
      val timeDiff = event.timestamp.getTime - state.pageCreationTimestamp.getOrElse(new Timestamp(Long.MinValue)).getTime
      val newEvent = {
        if (timeDiff >= 0 && timeDiff <= 2000) {
          event.copy(timestamp = state.pageCreationTimestamp.getOrElse(new Timestamp(0L)))
        } else {
          event
        }
      }
      val newKnownState = state.copy(
        startTimestamp = Some(newEvent.timestamp),
        pageCreationTimestamp = Some(newEvent.timestamp),
        causedByUserId = None,
        inferredFrom = Some("delete-conflict")
      )
      (
        status.copy(
          potentialStates = status.potentialStates - fromKey,
          knownStates = status.knownStates :+ newKnownState),
        newEvent
      )
    } else (status, event)

    // Since the deleted page has no restore, we can't retrieve it's real id
    // We therefore assign a new fake Id to its lineage
    val fakeId = randomUUID.toString

    // Create a new known delete state and new potential create one
    val newPotentialCreateState = new PageState(
      wikiDb = event1.wikiDb,
      pageId = event1.pageId,
      pageIdArtificial = Some(fakeId),
      titleHistorical = event1.oldTitle,
      title = event1.oldTitle,
      namespaceHistorical = event1.oldNamespace,
      namespaceIsContentHistorical = event1.oldNamespaceIsContent,
      namespace = event1.oldNamespace,
      namespaceIsContent = event1.oldNamespaceIsContent,
      endTimestamp = Some(event1.timestamp),
      causedByEventType = "create",
      inferredFrom = Some("delete")
    )
    val newKnownDeleteState = new PageState(
      wikiDb = event1.wikiDb,
      pageId = event1.pageId,
      pageIdArtificial = Some(fakeId),
      titleHistorical = event1.oldTitle,
      title = event1.oldTitle,
      namespaceHistorical = event1.oldNamespace,
      namespaceIsContentHistorical = event1.oldNamespaceIsContent,
      namespace = event1.oldNamespace,
      namespaceIsContent = event1.oldNamespaceIsContent,
      startTimestamp = Some(event1.timestamp),
      endTimestamp = Some(event1.timestamp),
      causedByEventType = "delete",
      causedByUserId = event1.causedByUserId
    )
    status1.copy(
      potentialStates = status1.potentialStates + (event1.fromKey -> newPotentialCreateState),
      knownStates = status1.knownStates :+ newKnownDeleteState
    )
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
      statsAccumulator.add((s"${event.wikiDb}.$METRIC_EVENTS_MATCHING_OK", 1))
      status.copy(
        restoredStates = status.restoredStates - toKey + (toKey -> conflictingRestoredState.copy(
          endTimestamp = Some(event.timestamp)
        )),
        knownStates = status.knownStates :+ conflictingRestoredState.copy(
          startTimestamp = Some(event.timestamp),
          causedByEventType = "restore",
          causedByUserId = event.causedByUserId,
          inferredFrom = Some("restore-conflict")
        )
      )
    } else if (status.potentialStates.contains(toKey)) {
      // Event-state match, move state to known state,
      // remove it from potential states,
      // and create new restoredState
      statsAccumulator.add((s"${event.wikiDb}.$METRIC_EVENTS_MATCHING_OK", 1))
      val state = status.potentialStates(toKey)
      val newRestoredState = state.copy(
        endTimestamp = Some(event.timestamp)
      )
      val newKnownState = state.copy(
        startTimestamp = Some(event.timestamp),
        causedByEventType = event.eventType,
        causedByUserId = event.causedByUserId
      )
      status.copy(
        potentialStates = status.potentialStates - toKey,
        restoredStates = status.restoredStates + (toKey -> newRestoredState),
        knownStates = status.knownStates :+ newKnownState
      )
    } else {
      // No event match - updating errors
      statsAccumulator.add((s"${event.wikiDb}.$METRIC_EVENTS_MATCHING_KO", 1))
      status.copy(unmatchedEvents = status.unmatchedEvents :+ event)
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
        status.potentialStates(toKey).pageCreationTimestamp.isDefined &&
        event.timestamp.before(status.potentialStates(toKey).pageCreationTimestamp.get)
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
      case _ =>
        statsAccumulator.add((s"${event.wikiDb}.$METRIC_EVENTS_MATCHING_KO", 1))
        status1.copy(unmatchedEvents = status1.unmatchedEvents :+ event)
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
          val sortedStates = pageStates.toList.sortBy(state =>
            (state.startTimestamp.getOrElse(new Timestamp(Long.MinValue)).getTime,
              state.endTimestamp.getOrElse(new Timestamp(Long.MaxValue)).getTime))
          val pageCreationTimestamp = sortedStates.head.pageCreationTimestamp
          sortedStates.map(s => s.copy(pageCreationTimestamp = pageCreationTimestamp))
      }
      .toSeq
  }

  /**
    * Function updating startTimestamp to firstEditTimestamp for create events of page with real ids.
    * this allows to make sure we'll link all revisions related to page_id
    *
    * @param states The states sequence to update create events startTimestamps for
    * @return The state sequence with updated crete events startTimestamp
    */
  def updateCreateStartTimestamp(states: Seq[PageState]): Seq[PageState] = {
    states
      .groupBy(s => (s.pageId, s.pageIdArtificial))
      .flatMap {
        case (pageIds, pageStates) =>
          if (pageIds._1.getOrElse(0L) > 0L) {
              pageStates.map(state => {
                // If no first edit timestamp use max timestamp => don't update
                val firstEditTimestamp = state.pageFirstEditTimestamp.getOrElse(new Timestamp(Long.MaxValue))
                // If no startTimestamp use min timestamp => don't update
                val startTimestamp = state.startTimestamp.getOrElse(new Timestamp(Long.MinValue))
                if (state.causedByEventType == "create" && firstEditTimestamp.before(startTimestamp)) {
                  state.copy(startTimestamp = state.pageFirstEditTimestamp)
                } else {
                  state
                }
              })
          } else {
            pageStates
          }
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
        // Sort move events to be processed first,
        // thus avoiding confusion around "move_redir" events.
        (
          a.timestamp.equals(b.timestamp) &&
          a.eventType == "move" &&
          b.eventType != "move"
        )
    }
    val (fStates: Seq[PageState], unmatchedEvents: Seq[PageEvent]) = {
      if (sortedEvents.isEmpty) {
        val finalStates = states.map(s => s.copy(startTimestamp = s.pageCreationTimestamp)).toSeq
        (finalStates, Seq.empty[PageEvent])
      } else {
        val initialStatus = new ProcessingStatus(
          potentialStates = states.map(s => s.key -> s).toMap,
          restoredStates = Map.empty[PageHistoryBuilder.KEY, PageState],
          knownStates = Seq.empty[PageState],
          unmatchedEvents = Seq.empty[PageEvent]
        )
        val finalStatus = sortedEvents.foldLeft(initialStatus)(processEvent)
        val finalStates = finalStatus.knownStates ++
          finalStatus.potentialStates.values.map(s => s.copy(startTimestamp = s.pageCreationTimestamp))
        (finalStates, finalStatus.unmatchedEvents)
      }
    }
    (updateCreateStartTimestamp(propagatePageCreation(fStates)), unmatchedEvents)

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
      Some(statsAccumulator)
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
  type STATS_GROUP = String

  val METRIC_SUBGRAPH_PARTITIONS = "pages.subgraphPartitions.count"

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
