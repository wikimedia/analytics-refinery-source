package org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized

import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.spark.utils.{StatsHelper, MapAccumulator}

/**
  * This file defines the functions for revisions-enrichment.
  *
  * The [[run]] method first updates archived revisions
  * with deleteTimestamp in [[populateDeleteTime()]],
  * then joins archived and live revisions. This joint data is then populated
  * with text bytes diff in [[populateByteDiff()]],
  * and then finally updated with revert information using the partition-and-sort-
  * within-partition/zip-partitions trick over reverts prepared in
  * [[prepareRevertsLists]], using
  * [[updateRevisionWithOptionalRevertsList]]
  * as joining function.
  *
  * It then returned the updated revisions RDD to the [[DenormalizedRunner]].
  */
class DenormalizedRevisionsBuilder(
                                    val spark: SparkSession,
                                    val statsAccumulator: Option[MapAccumulator[String, Long]],
                                    val numPartitions: Int
                                  ) extends StatsHelper with Serializable {

  import org.apache.log4j.Logger
  import org.apache.spark.rdd.RDD
  import java.sql.Timestamp
  // Implicit needed to sort by timestamps
  import org.wikimedia.analytics.refinery.core.TimestampHelpers
  import org.wikimedia.analytics.refinery.core.TimestampHelpers.orderedTimestamp
  import org.wikimedia.analytics.refinery.job.mediawikihistory.page.PageState
  import scala.annotation.tailrec


  @transient
  lazy val log: Logger = Logger.getLogger(this.getClass)

  val METRIC_ARCHIVE_DELETE_TS_EVENT_TS = "denormalize.archivedRevision.deleteTs.eventTs"
  val METRIC_ARCHIVE_DELETE_TS_MAX_ARCHIVE_TS = "denormalize.archivedRevision.deleteTs.maxArchiveTs"
  val METRIC_ARCHIVE_DELETE_TS_PAGE_DELETE_TS = "denormalize.archivedRevision.deleteTs.pageDeleteTs"
  val METRIC_BYTES_DIFF_OK = "denormalize.revision.bytesDiff.OK"
  val METRIC_BYTES_DIFF_KO = "denormalize.revision.bytesDiff.KO"
  val METRIC_REVERTS_LISTS_COUNT = "denormalize.revision.revertsList"
  val METRIC_NO_REVERT_COUNT = "denormalize.revision.revertInfo.noRevert"
  val METRIC_REVERT_COUNT = "denormalize.revision.revertInfo.revert"
  val METRIC_REVERTED_COUNT = "denormalize.revision.revertInfo.reverted"
  val METRIC_REVERT_REVERTED_COUNT = "denormalize.revision.revertInfo.revertAndReverted"

  /**
    * Return the first value from sortedTimestamps
    * that is bigger than refTs, None if not existing.
    *
    * Note: Could be improved using binary search, but sortedTimestamps
    * is to be mostly small, so it's not really needed.
    *
    * @param refTimestamp The reference timestamp
    * @param sortedTimestamps The ordered list of timestamps
    * @return the first timestamp from sortedTimestamps bigger than refTs, None if not existing.
    */
  @tailrec
  final def firstBigger(
                         refTimestamp: Long,
                         sortedTimestamps: Vector[Long]
                       ): Option[Long] = {
    sortedTimestamps match {
      case IndexedSeq() => None
      case timestamp +: restTimestamps =>
        if (timestamp > refTimestamp) Some(timestamp)
        else firstBigger(refTimestamp, restTimestamps)
    }
  }

  /**
    * Update delete timestamp in archived revisions using:
    *  - The first timestamp of the delete events for that page that is bigger than the event timestamp
    *  - The biggest timestamp of all archived revisions for that page if no matching delete event timestamp
    *  - The archived revision event timestamp if none of the two above
    *
    * @param archivedRevisions The archived revisions RDD
    * @param pageStates The page states built in PageHistory process (for delete timestamps)
    * @return The updated archived revisions RDD
    */
  def populateDeleteTime(
                          archivedRevisions: RDD[MediawikiEvent],
                          pageStates: RDD[PageState]
                        ): RDD[MediawikiEvent] = {
    val archivedRevisionsByPage = archivedRevisions
      .keyBy(r => DenormalizedKeysHelper.pageMediawikiEventKey(r).partitionKey)

    // Sorted list of delete events timestamps by page (when startTimestamp and pageId are defined)
    // RDD[((wikiDb, PageId), SortedVector[timestamp])]
    val pageDeletionTimestamps = pageStates
      .flatMap(p =>
        if (p.causedByEventType == "delete" && p.startTimestamp.isDefined && p.pageId.getOrElse(-1L) > 0L)
          Seq((DenormalizedKeysHelper.pageStateKey(p).partitionKey, p.startTimestamp.get))
        else Seq.empty)
      .groupByKey()
      // We assume there will not be so many deletion events per page for this sort to fail
      .mapValues(_.toVector.map(_.getTime).sorted)

    // Max archived revision timestamp by page (when pageId is defined)
    // RDD[((wikiDb, PageId), MaxTimestamp)]
    val archivedRevisionsMaxTimestampByPage = archivedRevisionsByPage
      .filter(_._1.id > 0L) // Filter out non valid pageIds
      .mapValues(_.eventTimestamp)
      .reduceByKey {
        case (None, ts2) => ts2
        case (ts1, None) => ts1
        case (ts1, ts2) => if (ts1.get.getTime >= ts2.get.getTime) ts1 else ts2
      }

    // (Max Rev Ts, sorted deletes Ts) by page
    // RDD[((wikiDb, PageId), (MaxTimestamp, Option[SortedVector[timestamp]])]
    val pageTimestamps = archivedRevisionsMaxTimestampByPage.leftOuterJoin(pageDeletionTimestamps)

    // Update archived revision
    archivedRevisionsByPage
      .leftOuterJoin(pageTimestamps) // keep all archived revisions
      .map {
        case (_, (r, None)) => // No delete nor max timestamp -- Use event one (should never happen)
          addOptionalStat(s"${r.wikiDb}.$METRIC_ARCHIVE_DELETE_TS_EVENT_TS", 1)
          r.isDeleted(r.eventTimestamp)
        case (_, (r, Some((maxTs, deleteTs)))) =>
          if (deleteTs.isEmpty) { // No delete timestamp -- Use max archived revision one
            addOptionalStat(s"${r.wikiDb}.$METRIC_ARCHIVE_DELETE_TS_MAX_ARCHIVE_TS", 1)
            r.isDeleted(maxTs)
          } else {
            // In case event timestamp is empty, firstBigger will return deleteTs first value
            val foundDeleteTs = firstBigger(r.eventTimestamp.map(_.getTime).getOrElse(-1L), deleteTs.get).map(new Timestamp(_))
            foundDeleteTs match {
              case None => // No delete timestamp -- Use max archived revision one
                addOptionalStat(s"${r.wikiDb}.$METRIC_ARCHIVE_DELETE_TS_MAX_ARCHIVE_TS", 1)
                r.isDeleted(maxTs)
              case _ => // Use delete timestamp
                addOptionalStat(s"${r.wikiDb}.$METRIC_ARCHIVE_DELETE_TS_PAGE_DELETE_TS", 1)
                r.isDeleted(foundDeleteTs)
            }
          }
      }
  }

  /**
    * Update text bytes diff in revisions by self-joining on revId = parentId.
    *
    * @param revisions the revisions RDD
    * @return the updated revisions RDD
    */
  def populateByteDiff(revisions: RDD[MediawikiEvent]): RDD[MediawikiEvent] = {

    // Extract parent Id and text bytes length by revId from revisions
    // when revId and texBytes are defined)
    // RDD[((wikiDB, revId), (textBytes, parentId))]
    val revisionsParentAndBytesById = revisions
      .flatMap(r => {
        if (r.revisionDetails.revId.getOrElse(-1L) > 0L && r.revisionDetails.revTextBytes.isDefined)
          Seq((DenormalizedKeysHelper.revisionMediawikiEventKey(r).partitionKey, // Key: (wikiDb, revId, "")
            (r.revisionDetails.revTextBytes.get, r.revisionDetails.revParentId))) // Value: (textBytes, parentId)
        else Seq.empty
      })

    // From previous, filter undefined parentIds and reformat to have
    // revId and textBytes by ParentId
    // RDD[((wikiDB, parent), (textBytes, revId))]
    val revisionsIdAndBytesByParent = revisionsParentAndBytesById
      .flatMap {
        case (revPartitionKey, (len, parentId)) =>
          if (parentId.getOrElse(-1L) > 0L)
            Seq((revPartitionKey.copy(id=parentId.get), (len, revPartitionKey.id)))
          else Seq.empty
      }

    // Join (by parentId) with (by revId) and compute text bytes diff
    // RDD[((wikiDB, revId), textBytesDiff)]
    val revisionsBytesDiffById = revisionsIdAndBytesByParent
      .join(revisionsParentAndBytesById)
      .map { case (parentPartitionKey, ((len, revId), (pLen, _))) =>
        (parentPartitionKey.copy(id=revId), len - pLen) }

    // Update revisions with computed values
    revisions
      .keyBy(r => DenormalizedKeysHelper.revisionMediawikiEventKey(r).partitionKey)
      .leftOuterJoin(revisionsBytesDiffById) // Keep all revisions
      .map {
        case (_, (r, None)) =>
          if (r.revisionDetails.revParentId.getOrElse(0L) > 0) {
            addOptionalStat(s"${r.wikiDb}.$METRIC_BYTES_DIFF_KO", 1)
            r.textBytesDiff(None) // set None instead of textBytes since parentId is not 0 but no link found
          } else {
            addOptionalStat(s"${r.wikiDb}.$METRIC_BYTES_DIFF_OK", 1)
            r // Keep already existing textBytes
          }
        case (_, (r, diff)) =>
          addOptionalStat(s"${r.wikiDb}.$METRIC_BYTES_DIFF_OK", 1)
          r.textBytesDiff(diff)
      }
  }


  /**
    * Generate a RDD of reverts-lists by (page, year, revert-base) to be partitioned-sorted
    * and then zipped with revisions RDD through [[updateRevisionAndReverts]].
    * What we call a reverts-list is a revertBase revision (the first occurrence of a sha1 for a
    * given page) and the sorted list of revisions occurring after that revertBase and having
    * the same sha1 for the given page (the reverts).
    *
    * @param revisions the revisions RDD
    * @return the reverts-lists RDD:
    *         RDD[(((wikiDb, pageId, year) baseRevisionTimestamp, baseRevisionId),
    *             SortedVector[revertRevisionTimestamp, revertRevisionId])]
    */
  def prepareRevertsLists(
                           revisions: RDD[MediawikiEvent]
                         ): RDD[(MediawikiEventKey, Vector[(Option[Timestamp], Option[Long])])] = {
    revisions
      .filter(_.pageDetails.pageId.getOrElse(-1L) > 0L) // remove invalid pageIds
      .map(r =>
          // Key: ((wikiDb, pageId), revSha1)
          (RevertKey(DenormalizedKeysHelper.pageMediawikiEventKey(r).partitionKey, r.revisionDetails.revTextSha1),
          // Value: (revTimestamp, revId)
          (r.eventTimestamp, r.revisionDetails.revId)))
      .groupByKey() // Same pageId and sha1
      .flatMap {
        // revInfoIterator contains all revisions (timestamp, id) for any given pageId sha1
        case (revertKey, revInfoIterator) =>
          // First revision with a given sha1 is the base, others are reverts to the base
          val (baseRevision, reverts) = {
            // We assume there will not be so many reverting events per page for this sort to fail
            val sortedSha1s = revInfoIterator.toVector.sorted // sort by timestamp, revId
            (sortedSha1s.head, sortedSha1s.tail)
          }
          if (reverts.isEmpty) Seq() // No reverts, no work
          else {
            // Generate one revert-list event by year to be zipped with revisions-by-year
            addOptionalStat(s"${revertKey.partitionKey.db}.$METRIC_REVERTS_LISTS_COUNT", 1L)
            Seq((MediawikiEventKey(revertKey.partitionKey, baseRevision._1, baseRevision._2), reverts))
          }
      }
  }

  /**
    * Update revision revert information using the reverts ordered list created in
    * [[updateRevisionWithOptionalRevertsList]] (part of innerState) and updating
    * it as needed (not to be mixed up with the result of [[prepareRevertsLists]]).
    *
    * @param revision The revision to update
    * @param reverts The ordered reverts as a sorted list of ((revertTimestamp, revertRevisionId), baseRevisionId)
    * @return The updated revision -- Side effects on reverts list
    */
  def updateRevisionAndReverts(
                                revision: MediawikiEvent,
                                reverts: DenormalizedRevisionsBuilder.MutableOrderedReverts
                              ): MediawikiEvent = {
    if (reverts.isEmpty) { // No revert after this revision, no update
      addOptionalStat(s"${revision.wikiDb}.$METRIC_NO_REVERT_COUNT", 1)
      revision
    } else if (reverts.head._1 == (revision.eventTimestamp, revision.revisionDetails.revId)) {
      // Worked revision is a reverting one (head of reverts)

      // keep revertBaseId for DIFFERENT wider revert check
      val revertingBaseId = reverts.head._2
      // Remove first revert from the list since reached
      reverts.remove(reverts.head)

      if (reverts.isEmpty || (reverts.head._2 == revertingBaseId)) {
        // Worked revision is not reverted as part of a different wider revert
        addOptionalStat(s"${revision.wikiDb}.$METRIC_REVERT_COUNT", 1)
        revision.isIdentityRevert
      } else {
        // Worked revision is reverting and also reverted as part of a different wider revert
        val revertingTimestamp = reverts.head._1._1
        val revertingRevisionId = reverts.head._1._2
        val revisionTimeToRevert = TimestampHelpers.getTimestampDifference(revertingTimestamp, revision.eventTimestamp)
        addOptionalStat(s"${revision.wikiDb}.$METRIC_REVERT_REVERTED_COUNT", 1)
        revision.isIdentityRevert.isIdentityReverted(revertingRevisionId, revisionTimeToRevert)
      }
    } else {
      // Worked revision is reverted
      val revertingTimestamp = reverts.head._1._1
      val revertingRevisionId = reverts.head._1._2
      val revisionTimeToRevert = TimestampHelpers.getTimestampDifference(revertingTimestamp, revision.eventTimestamp)
      addOptionalStat(s"${revision.wikiDb}.$METRIC_REVERTED_COUNT", 1)
      revision.isIdentityReverted(revertingRevisionId, revisionTimeToRevert)
    }
  }

  /**
    *
    * Update revision revert information from potential reverts list (from [[prepareRevertsLists]]).
    * Manages its state through a changing [[DenormalizedRevisionsBuilder.RevertsListsState]] (innerState).
    *
    *
    * @param innerState The current state of page and reverts being worked
    * @param keyAndRevision The revision to potentially update
    * @param optionalKeyAndRevertsList The optional revert list to use to update the revision
    * @return The potentially updated revision
    */
  def updateRevisionWithOptionalRevertsList(innerState: DenormalizedRevisionsBuilder.RevertsListsState)
                                           (
                                             keyAndRevision: (MediawikiEventKey, MediawikiEvent),
                                             optionalKeyAndRevertsList: Option[(MediawikiEventKey, Vector[(Option[Timestamp], Option[Long])])]
                                           ): MediawikiEvent = {

    val (revKey, revision) = keyAndRevision

    // In case we change page, reinitialise inner state
    if (innerState.currentPage.isEmpty || revKey.partitionKey.compare(innerState.currentPage.get) != 0) {
      innerState.currentPage = Some(revKey.partitionKey)
      innerState.currentReverts.clear()
    }

    optionalKeyAndRevertsList match {
      // No revertsList to work - only care about currentReverts
      case None => updateRevisionAndReverts(revision, innerState.currentReverts)

      // Current revision is revertBase from
      case Some((rKey, revertsList)) =>

        // Update current revision
        val updatedRevision = updateRevisionAndReverts(revision, innerState.currentReverts)

        // Pop next revertsList and add its reverts to innerState reverts sorted list
        for (endRev <- revertsList)
          innerState.currentReverts.add((endRev, rKey.sortingId))

        // Return the updated revision
        updatedRevision
    }
  }

  /**
    * Run revisions-related functions on archived and live revisions RDDs:
    *  - populateDeleteTime (for archived revisions only)
    *  - union live and archived
    *  - populateBytesDiff (self-join on revId = parentId to compute text bytes diff)
    *  - revert-information generation using repartitionAndSortWithinPartitions
    *    and zipPartitions trick (pushing heavy sorting into Spark machinery)
    *
    * @param liveRevisions The live revisions RDD
    * @param archivedRevisions The archived revisions RDD
    * @param pageStates The page states built in PageHistory process (for delete timestamps)
    * @param historyPartitioner The partitioner to use for the repartitionAndSortWithinPartitions trick
    * @return The enhanced revision RDD
    */
  def run(
           liveRevisions: RDD[MediawikiEvent],
           archivedRevisions: RDD[MediawikiEvent],
           pageStates: RDD[PageState],
           historyPartitioner: PartitionKeyPartitioner[MediawikiEventKey]
         ): RDD[MediawikiEvent] = {

    log.info(s"Denormalized revisions jobs starting")

    log.info(s"Populating delete times for archived denormalized revisions")
    val archivedRevisionsWithDeleteTime = populateDeleteTime(archivedRevisions, pageStates)

    log.info(s"Union-ing denormalized archived revisions and live revisions")
    val revisions = liveRevisions.union(archivedRevisionsWithDeleteTime).repartition(numPartitions)

    log.info(s"Populating text bytes diff for denormalized revisions")
    val revisionsWithDiff = populateByteDiff(revisions)

    // Compute reverts info
    log.info(s"Populating revert info for denormalized revisions")
    val revertsLists = prepareRevertsLists(revisionsWithDiff)
      .repartitionAndSortWithinPartitions(historyPartitioner)
    val zipper = DenormalizedKeysHelper.leftOuterZip(
      DenormalizedKeysHelper.compareMediawikiEventKeys,
      updateRevisionWithOptionalRevertsList(new DenormalizedRevisionsBuilder.RevertsListsState)) _
    val revisionsWithDiffAndPerUserAndPageMetricsAndRevert = revisionsWithDiff
      //.keyBy(r => DenormalizedKeysHelper.pageMediawikiEventKey(r))
      .keyBy(r => DenormalizedKeysHelper.pageMediawikiEventKey(r))
      .repartitionAndSortWithinPartitions(historyPartitioner)
      .zipPartitions(revertsLists)(
        (keysAndRevisions, keysAndRevertsLists) => zipper(keysAndRevisions, keysAndRevertsLists)
      )

    log.info(s"Denormalized revisions jobs done")
    revisionsWithDiffAndPerUserAndPageMetricsAndRevert
  }

}

object DenormalizedRevisionsBuilder {

  import java.sql.Timestamp
  import org.wikimedia.analytics.refinery.core.TimestampHelpers.orderedTimestamp

  // Mutable ordered set to manage reverts by timestamp
  type MutableOrderedReverts = scala.collection.mutable.TreeSet[((Option[Timestamp], Option[Long]), Option[Long])]

  /**
    * To be used as inner state for [[DenormalizedRevisionsBuilder.updateRevisionWithOptionalRevertsList]].
    *
    * @param currentPage The page currently updated for reverts
    * @param currentReverts The reverts currently in progress as a sorted list of
    *                       ((revertTimestamp, revertRevisionId), baseRevisionId)
    */
  case class RevertsListsState(
                                var currentPage: Option[PartitionKey] = None,
                                currentReverts: MutableOrderedReverts = new MutableOrderedReverts
                              )


}
