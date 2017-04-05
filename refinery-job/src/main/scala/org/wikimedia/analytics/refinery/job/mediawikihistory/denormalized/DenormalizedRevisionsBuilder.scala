package org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized


/**
  * This file defines the functions for revisions-enrichment.
  *
  * The [[DenormalizedRevisionsBuilder.run]] first updates archived revisions
  * with deleteTimestamp in [[DenormalizedRevisionsBuilder.populateDeleteTime()]],
  * then joins archived and live revisions. This joint data is then populated
  * with text bytes diff in [[DenormalizedRevisionsBuilder.populateByteDiff()]],
  * and then finally updated with revert information using the partition-and-sort-
  * within-partition/zip-partitions trick over reverts prepared in
  * [[DenormalizedRevisionsBuilder.prepareRevertsLists]], using
  * [[DenormalizedRevisionsBuilder.updateRevisionWithOptionalRevertsList]]
  * as joining function.
  *
  * It then returned the updated revisions RDD to the [[DenormalizedRunner]].
  */
object DenormalizedRevisionsBuilder extends Serializable {

  import com.github.nscala_time.time.Imports._
  import org.apache.log4j.Logger
  import org.apache.spark.rdd.RDD
  import org.wikimedia.analytics.refinery.job.mediawikihistory.page.PageState

  import scala.annotation.tailrec

  // Mutable ordered set to manage reverts by timestamp
  type MutableOrderedReverts = scala.collection.mutable.TreeSet[((Option[String], Option[Long]), Option[Long])]

  @transient
  lazy val log: Logger = Logger.getLogger(this.getClass)

  @transient
  lazy val timestampParser = DateTimeFormat.forPattern("YYYYMMddHHmmss")

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
                         refTimestamp: String,
                         sortedTimestamps: Vector[String]
                       ): Option[String] = {
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
      .keyBy(r => DenormalizedKeysHelper.pageMediawikiEventKeyNoYear(r).partitionKey)

    // Sorted list of delete events timestamps by page (when startTimestamp and pageId are defined)
    // RDD[((wikiDb, PageId), SortedVector[timestamp])]
    val pageDeletionTimestamps = pageStates
      .flatMap(p =>
        if (p.causedByEventType == "delete" && p.startTimestamp.isDefined && p.pageId.getOrElse(-1L) > 0L)
          Seq((DenormalizedKeysHelper.pageStateKeyNoYear(p).partitionKey, p.startTimestamp.get))
        else Seq.empty)
      .groupByKey()
      // We assume there will not be so many deletion events per page for this sort to fail
      .mapValues(_.toVector.sorted)


    // Max archived revision timestamp by page (when pageId is defined)
    // RDD[((wikiDb, PageId), MaxTimestamp)]
    val archivedRevisionsMaxTimestampByPage = archivedRevisionsByPage
      .filter(_._1.id > 0L) // Filter out non valid pageIds
      .mapValues(_.eventTimestamp)
      .reduceByKey {
        case (None, ts2) => ts2
        case (ts1, None) => ts1
        case (ts1, ts2) => if (ts1.get >= ts2.get) ts1 else ts2
      }

    // (Max Rev Ts, sorted deletes Ts) by page
    // RDD[((wikiDb, PageId), (MaxTimestamp, Option[SortedVector[timestamp]])]
    val pageTimestamps = archivedRevisionsMaxTimestampByPage.leftOuterJoin(pageDeletionTimestamps)

    // Update archived revision
    archivedRevisionsByPage
      .leftOuterJoin(pageTimestamps) // keep all archived revisions
      .map {
        case (_, (r, None)) => r.isDeleted(r.eventTimestamp) // No delete nor max timestamp -- Use event one
        case (_, (r, Some((maxTs, deleteTs)))) =>
          if (deleteTs.isEmpty) r.isDeleted(maxTs)  // No delete timestamp -- Use max archived revision one
          else {
            // In case event timestamp is empty, firstBigger will return deleteTs first value
            val foundDeleteTs = firstBigger(r.eventTimestamp.getOrElse(""), deleteTs.get)
            foundDeleteTs match {
              case None => r.isDeleted(maxTs) // No delete timestamp -- Use max archived revision one
              case _ => r.isDeleted(foundDeleteTs) // Use delete timestamp
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
          Seq((DenormalizedKeysHelper.revisionMediawikiEventKeyNoYear(r).partitionKey, // Key: (wikiDb, revId, "")
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
      .keyBy(r => DenormalizedKeysHelper.revisionMediawikiEventKeyNoYear(r).partitionKey)
      .leftOuterJoin(revisionsBytesDiffById) // Keep all revisions
      .map {
        case (_, (r, None)) =>
          if (r.revisionDetails.revParentId.getOrElse(0L) > 0)
            r.textBytesDiff(None) // set None instead of textBytes since parentId is not 0 but no link found
          else
            r // Keep already existing textBytes
        case (_, (r, diff)) =>  r.textBytesDiff(diff)
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
                         ): RDD[(MediawikiEventKey, Vector[(Option[String], Option[Long])])] = {
    revisions
      .filter(_.pageDetails.pageId.getOrElse(-1L) > 0L) // remove invalid pageIds
      .map(r =>
          // Key: ((wikiDb, pageId), revSha1)
          ((DenormalizedKeysHelper.pageMediawikiEventKeyNoYear(r).partitionKey, r.revisionDetails.revTextSha1),
          // Value: (revTimestamp, revId)
          (r.eventTimestamp, r.revisionDetails.revId)))
      .groupByKey() // Same pageId and sha1
      .flatMap {
        // revInfoIterator contains all revisions (timestamp, id) for any given pageId sha1
        case ((partitionKey, sha1), revInfoIterator) =>
          // First revision with a given sha1 is the base, others are reverts to the base
          val (baseRevision, reverts) = {
            // We assume there will not be so many reverting events per page for this sort to fail
            val sortedSha1s = revInfoIterator.toVector.sorted // sort by timestamp, revId
            (sortedSha1s.head, sortedSha1s.tail)
          }
          if (reverts.isEmpty) Seq() // No reverts, no work
          else {
            // Years spanned by the reverts-lists (from base to last revert)
            val yearsSpanned = DenormalizedKeysHelper.years(new TimeBoundaries {
              override def startTimestamp: Option[String] = baseRevision._1
              override def endTimestamp: Option[String] = reverts.last._1
            })
            // Generate one revert-list event by year to be zipped with revisions-by-year
            yearsSpanned.map(y => {
              (
                MediawikiEventKey(partitionKey.copy(year = y), baseRevision._1, baseRevision._2),
                reverts
                )
            })
          }
      }
  }

  /**
    * Returns the number of seconds elapsed between the revision's creation and
    * its revert.
  **/

  def getTimestampDifference(
                              revertTimestamp: Option[String],
                              revisionTimestamp: Option[String]
                            ): Option[Long] = (revertTimestamp, revisionTimestamp) match {
    case (Some(t1), Some(t2)) => {
      val revertTimestampMs = timestampParser.parseMillis(t1)
      val revisionTimestampMs = timestampParser.parseMillis(t2)
      Option((revertTimestampMs - revisionTimestampMs) / 1000)
    }
    case _ => None
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
  def updateRevisionAndReverts(revision: MediawikiEvent, reverts: MutableOrderedReverts): MediawikiEvent = {
    if (reverts.isEmpty) revision // No revert after this revision, no update
    else if (reverts.head._1 == (revision.eventTimestamp, revision.revisionDetails.revId)) {
      // Worked revision is a reverting one (head of reverts)

      // keep revertBaseId for DIFFERENT wider revert check
      val revertingBaseId = reverts.head._2
      // Remove first revert from the list since reached
      reverts.remove(reverts.head)

      if (reverts.isEmpty || (reverts.head._2 == revertingBaseId))
      // Worked revision is not reverted as part of a different wider revert
        revision.isIdentityRevert
      else {
        // Worked revision is reverting and also reverted as part of a different wider revert
        val revertingTimestamp = reverts.head._1._1
        val revertingRevisionId = reverts.head._1._2
        val revisionTimeToRevert = getTimestampDifference(revertingTimestamp, revision.eventTimestamp)
        revision.isIdentityRevert.isIdentityReverted(revertingRevisionId, revisionTimeToRevert)
      }
    } else {
      // Worked revision is reverted
      val revertingTimestamp = reverts.head._1._1
      val revertingRevisionId = reverts.head._1._2
      val revisionTimeToRevert = getTimestampDifference(revertingTimestamp, revision.eventTimestamp)
      revision.isIdentityReverted(revertingRevisionId, revisionTimeToRevert)
    }
  }

  /**
    * To be used as inner state for [[updateRevisionWithOptionalRevertsList]].
    *
    * @param currentPage The page currently updated for reverts
    * @param currentReverts The reverts currently in progress as a sorted list of
    *                       ((revertTimestamp, revertRevisionId), baseRevisionId)
    */
  case class RevertsListsState(
                                var currentPage: Option[PartitionKey] = None,
                                currentReverts: MutableOrderedReverts = new MutableOrderedReverts
                              )


  /**
    *
    * Update revision revert information from potential reverts list (from [[prepareRevertsLists]]).
    * Manages its state through a changing [[RevertsListsState]] (innerState).
    *
    *
    * @param innerState The current state of page and reverts being worked
    * @param keyAndRevision The revision to potentially update
    * @param optionalKeyAndRevertsList The optional revert list to use to update the revision
    * @return The potentially updated revision
    */
  def updateRevisionWithOptionalRevertsList(innerState: RevertsListsState)
                                           (
                                             keyAndRevision: (MediawikiEventKey, MediawikiEvent),
                                             optionalKeyAndRevertsList: Option[(MediawikiEventKey, Vector[(Option[String], Option[Long])])]
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
    log.info(s"Archived denormalized revisions with delete times: ${archivedRevisionsWithDeleteTime.count()}")

    log.info(s"Union-ing denormalized archived revisions and live revisions")
    val revisions = liveRevisions.union(archivedRevisionsWithDeleteTime)
    log.info(s"Union-ed denormalized revisions: ${revisions.count()}")

    log.info(s"Populating text bytes diff for denormalized revisions")
    val revisionsWithDiff = populateByteDiff(revisions)
    log.info(s"Revisions with text bytes diff: ${revisionsWithDiff.count()}")

    // Compute reverts info
    log.info(s"Populating revert info for denormalized revisions")
    val revertsLists = prepareRevertsLists(revisionsWithDiff)
      .repartitionAndSortWithinPartitions(historyPartitioner)
    val zipper = DenormalizedKeysHelper.leftOuterZip(
      DenormalizedKeysHelper.compareMediawikiEventKeys,
      updateRevisionWithOptionalRevertsList(new RevertsListsState)) _
    val revisionsWithDiffAndRevert = revisionsWithDiff
      .keyBy(r => DenormalizedKeysHelper.pageMediawikiEventKey(r))
      .repartitionAndSortWithinPartitions(historyPartitioner)
      .zipPartitions(revertsLists)(
        (keysAndRevisions, keysAndRevertsLists) => zipper(keysAndRevisions, keysAndRevertsLists)
      )
    log.info(s"Revisions with text bytes diff and revert info: ${revisionsWithDiffAndRevert.count()}")

    log.info(s"Denormalized revisions jobs done")
    revisionsWithDiffAndRevert
  }

}
