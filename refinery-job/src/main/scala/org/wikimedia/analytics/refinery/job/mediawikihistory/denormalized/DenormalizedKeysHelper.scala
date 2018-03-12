package org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized

import java.sql.Timestamp

import org.apache.spark.Partitioner

/**
  * Generalisation of an object having start and end timestamp.
  * Used to generate the set of years covered by such an object.
  */
trait TimeBoundaries {
  def startTimestamp: Option[Timestamp]
  def endTimestamp: Option[Timestamp]
}

/**
  * Generalisation of an object having a partition key.
  * Used to define generalised partitioner over partition keys.
  */
trait HasPartitionKey {
  def partitionKey: PartitionKey
}


/**
  * Key used for partitioning our data.
  *
  * This class overrides compare in order to be used in a Partitioner
  *
  * @param db The wiki db of the row (we don't mix wikis)
  * @param id The interesting id (can be userId, pageId, revId...)
  * @param year The year (because whole-time groups are too big)
  */
case class PartitionKey(db: String, id: Long, year: Int)
  extends Ordered[PartitionKey] {
  override def compare(that: PartitionKey): Int =
    implicitly[Ordering[(String, Long, Int)]].compare((this.db, this.id, this.year), (that.db, that.id, that.year))
}


/**
  * Key used to partition and sort states (users or pages)
  * Partition is done on the partition key and sorting
  * on the partition key AND start and end timestamps.
  *
  * This class overrides compare in order to be used in a Partitioner
  *
  * @param partitionKey The state partition key
  * @param startTimestamp The state startTimestamp
  * @param endTimestamp The state endTimestamp
  */
case class StateKey(partitionKey: PartitionKey,
                    startTimestamp: Option[Timestamp],
                    endTimestamp: Option[Timestamp])
  extends Ordered[StateKey]
  with HasPartitionKey {
  override def compare(that: StateKey): Int = {
    val partitionComp = this.partitionKey.compare(that.partitionKey)
    if (partitionComp == 0) {
      // No option comparator defined for Timestamp, so we use the Long one using getTime
      implicitly[Ordering[(Option[Long], Option[Long])]].compare(
        (this.startTimestamp.map(_.getTime), this.endTimestamp.map(_.getTime)),
        (that.startTimestamp.map(_.getTime), that.endTimestamp.map(_.getTime)))
    }
    else partitionComp
  }
}


/**
  * Key used to partition and sort MW Events (revisions, users or pages)
  * Partition is done on the partition key and sorting
  * on the partition key AND timestamp and sortingId (when
  * two revisions have the same timestamp... It happens).
  *
  * This class overrides compare in order to be used in a Partitioner
  *
  * @param partitionKey The MW Event partition key
  * @param timestamp The MW Event timestamp
  * @param sortingId The MW Event sorting id
  */
case class MediawikiEventKey(partitionKey: PartitionKey,
                             timestamp: Option[Timestamp],
                             sortingId: Option[Long])
  extends Ordered[MediawikiEventKey]
  with HasPartitionKey {
  override def compare(that: MediawikiEventKey): Int = {
    val partitionComp = this.partitionKey.compare(that.partitionKey)
    if (partitionComp == 0)
    // No option comparator defined for Timestamp, so we use the Long one using getTime
      implicitly[Ordering[(Option[Long], Option[Long])]].compare(
        (this.timestamp.map(_.getTime), this.sortingId),
        (that.timestamp.map(_.getTime), that.sortingId))
    else partitionComp
  }
}


/**
  * Key used to prepare reverts and group revisions by partition and sha1.
  *
  * This class overrides compare in order to be used with groupBy
  *
  * @param partitionKey The revision partition key
  * @param sha1 The revision sha1
  */
case class RevertKey(partitionKey: PartitionKey,
                     sha1: Option[String])
  extends Ordered[RevertKey]
  with HasPartitionKey {
  override def compare(that: RevertKey): Int = {
    val partitionComp = this.partitionKey.compare(that.partitionKey)
    if (partitionComp == 0)
      implicitly[Ordering[Option[String]]].compare(this.sha1, that.sha1)
    else partitionComp
  }
}



/**
  * Partitioner for keys implementing [[HasPartitionKey]].
  * Partitioning is done on the PartitionKey part of the key.
  *
  * @param numPartitions The number of partitions to partition over
  * @tparam K The type of the key
  */
class PartitionKeyPartitioner[K <: HasPartitionKey](val numPartitions: Int)
  extends Partitioner {
  require(numPartitions >= 0,
    s"Number of partitions ($numPartitions) cannot be negative.")
  override def getPartition(key: Any): Int = {
    math.abs(key.asInstanceOf[K].partitionKey.hashCode()) % numPartitions
  }
}


/**
  * Object providing functions helping building
  * keys used in the denormalized package.
  */
object DenormalizedKeysHelper extends Serializable {

  import com.github.nscala_time.time.Imports._
  import org.wikimedia.analytics.refinery.job.mediawikihistory.page.PageState
  import org.wikimedia.analytics.refinery.job.mediawikihistory.user.UserState

  /**
    * Extracts the year part of a timestamp.
    *
    * @param timestamp the timestamp to extract the year from
    * @param default The value to return in case of None timestamp
    * @return The year or the default value
    */
  def year(timestamp: Option[Timestamp], default: Integer): Int = timestamp match {
    case None => default
    case Some(validTimestamp) => new DateTime(validTimestamp.getTime).getYear
  }


  /**
    * 1999 to current-year year list used to generate years covered
    * by an object implementing [[TimeBoundaries]].
    */
  val yearList = 1999 to DateTime.now.getYear
  def years[B <: TimeBoundaries](boundaries: B): Seq[Int] =
    yearList.filter(y =>
      // default to a year smaller than any valid one
      y >= year(boundaries.startTimestamp, -1) &&
      // default to a year bigger than any valid one
      y <= year(boundaries.endTimestamp, 10000))


  /**
    * Returns the given id if valid (defined and strictly positive),
    * or a negative hash value as fake id if invalid.
    *
    * @param id the id to test
    * @param toHash The object to hash as a negative value
    * @return id if defined and strictly positive, a negative hash value otherwise
    */
  def idOrHashNegative(id: Option[Long], toHash: Any): Long = {
    if (id.getOrElse(-1L) > 0L) id.get
    else -math.abs(toHash.hashCode())
  }


  /**************************************************************
    * User helpers
    */

  /**
    * Generate a list of user-centered [[StateKey]] for
    * a given [[UserState]], having the same values except
    * for the year and using a fake value in place of
    * user id if invalid (see [[idOrHashNegative]]).
    *
    * @param userState The user state to generate keys for
    * @return The sequence of keys for userState
    */
  def userStateKeys(userState: UserState): Seq[StateKey] = {
    val userId: Long = idOrHashNegative(Some(userState.userId), userState)
    years(userState).map(y =>
      StateKey(PartitionKey(userState.wikiDb, userId, y),
        userState.startTimestamp, userState.endTimestamp))
  }


  /**
    * Generate the user-centered [[StateKey]] ((wikiDb, UserId, -1), start, end)
    * (-1 as year) using a fake value in place of user id
    * if invalid (see [[idOrHashNegative]]).
    *
    * @param userState The user state to generate key for
    * @return The key for the userState
    */
  def userStateKeyNoYear(userState: UserState): StateKey = {
    val userId = DenormalizedKeysHelper.idOrHashNegative(Some(userState.userId), userState)
    StateKey(PartitionKey(userState.wikiDb, userId, -1),
      userState.startTimestamp, userState.endTimestamp)
  }


  /**
    * Generate a user-centered [[MediawikiEventKey]] for a
    * given MW event using a fake value in place of user id
    * if invalid (see [[idOrHashNegative]]).
    *
    * Year value of the [[PartitionKey]] can be -1
    * if the useYear parameter is false (default true).
    *
    * @param mwEvent The MW Event to generate key for
    * @param useYear Flag for using MW Event year (true) or
    *                -1 (false) in [[PartitionKey]]
    * @return The MW Event key
    */
  def userMediawikiEventKey(mwEvent: MediawikiEvent, useYear: Boolean = true): MediawikiEventKey = {
    val userId: Long = idOrHashNegative(mwEvent.eventUserDetails.userId, mwEvent)
    MediawikiEventKey(PartitionKey(mwEvent.wikiDb, userId, if (useYear) year(mwEvent.eventTimestamp, 0) else -1),
      mwEvent.eventTimestamp, mwEvent.revisionDetails.revId)
  }


  /**
    * Generate a user-centered [[MediawikiEventKey]] for a
    * given MW Event with -1 as year using a fake
    * value in place of user id if invalid (see [[idOrHashNegative]]).
    *
    * @param mwEvent The MW Event to generate key for
    * @return The MW Event key
    */
  def userMediawikiEventKeyNoYear(mwEvent: MediawikiEvent): MediawikiEventKey = {
    userMediawikiEventKey(mwEvent, useYear = false)
  }


  /**************************************************************
    * Page helpers
    */

  /**
    * Generate a list of page-centered [[StateKey]] for
    * a given [[PageState]], having the same values except
    * for the year and using a fake value in place of
    * page id if invalid (see [[idOrHashNegative]]).
    *
    * @param pageState The page state to generate keys for
    * @return The sequence of keys for pageState
    */
  def pageStateKeys(pageState: PageState): Seq[StateKey] = {
    val pageId: Long = idOrHashNegative(pageState.pageId, pageState)
    years(pageState).map(y =>
      StateKey(PartitionKey(pageState.wikiDb, pageId, y),
        pageState.startTimestamp, pageState.endTimestamp))
  }


  /**
    * Generate the page-centered [[StateKey]] ((wikiDb, pageId, -1), start, end)
    * (-1 as year) using a fake value in place of page id
    * if invalid (see [[idOrHashNegative]]).
    *
    * @param pageState The page state to generate key for
    * @return The key for the pageState
    */
  def pageStateKeyNoYear(pageState: PageState): StateKey = {
    val pageId = DenormalizedKeysHelper.idOrHashNegative(pageState.pageId, pageState)
    StateKey(PartitionKey(pageState.wikiDb, pageId, -1),
      pageState.startTimestamp, pageState.endTimestamp)
  }


  /**
    * Generate a page-centered [[MediawikiEventKey]] for a
    * given [[MediawikiEvent]] using a fake value in place
    * of page id if invalid (see [[idOrHashNegative]]).
    *
    * Year value of the [[PartitionKey]] can be -1
    * if the useYear parameter is false (default true).
    *
    * @param mwEvent The MW Event to generate key for
    * @param useYear Flag for using MW Event year (true) or
    *                -1 (false) in [[PartitionKey]]
    * @return The MW Event key
    */
  def pageMediawikiEventKey(mwEvent: MediawikiEvent, useYear: Boolean = true): MediawikiEventKey = {
    val pageId: Long = idOrHashNegative(mwEvent.pageDetails.pageId, mwEvent)
    MediawikiEventKey(PartitionKey(mwEvent.wikiDb, pageId, if (useYear) year(mwEvent.eventTimestamp, 0) else -1),
      mwEvent.eventTimestamp, mwEvent.revisionDetails.revId)
  }

  /**
    * Generate a page-centered [[MediawikiEventKey]] for a
    * given MW Event with -1 as year using a fake
    * value in place of page id if invalid (see [[idOrHashNegative]]).
    *
    * @param mwEvent The MW Event to generate key for
    * @return The MW Event key
    */
  def pageMediawikiEventKeyNoYear(mwEvent: MediawikiEvent): MediawikiEventKey = {
    pageMediawikiEventKey(mwEvent, useYear = false)
  }


  /**************************************************************
    * Revision helpers
    */

  /**
    * Generate a revision-centered [[MediawikiEventKey]] for a
    * given MW Event with -1 as year using a fake
    * value in place of revision id if invalid (see [[idOrHashNegative]]).
    *
    * @param mwEvent The MW Event to generate key for
    * @return The MW Event key
    */
  def revisionMediawikiEventKeyNoYear(mwEvent: MediawikiEvent): MediawikiEventKey = {
    val revisionId: Long = idOrHashNegative(mwEvent.revisionDetails.revId, mwEvent)
    MediawikiEventKey(PartitionKey(mwEvent.wikiDb, revisionId, -1),
      mwEvent.eventTimestamp, mwEvent.revisionDetails.revId)
  }


  /**************************************************************
    * Generic helpers
    */

  /**
    * Compare a [[MediawikiEventKey]] with a [[StateKey]] by
    *  1 - [[PartitionKey]]
    *  2 - MW Event timestamp being between state start and end timestamps,
    *      possibly equal to start and strictly lower than end.
    *
    * @param mweKey The MW Event key
    * @param sKey The state key
    * @return 0 if equal, -1 if hKey =< sKey, 1 if hKey > sKey
    */
  def compareMediawikiEventAndStateKeys(mweKey: MediawikiEventKey, sKey: StateKey): Int = {
    val partitionKeyComp = mweKey.partitionKey.compare(sKey.partitionKey)
    if (partitionKeyComp != 0) partitionKeyComp
    else { // Same partition, check timestamps
      val hTimestamp = mweKey.timestamp.map(_.getTime).getOrElse(-1L)
      val sStartTimestamp = sKey.startTimestamp.map(_.getTime).getOrElse(-1L)
      if (hTimestamp < sStartTimestamp) -1
      else if (hTimestamp >= sStartTimestamp &&
        (hTimestamp < sKey.endTimestamp.map(_.getTime).getOrElse(Long.MaxValue))) 0
      else 1
    }
  }

  /**
    * Fully compare two MediawikiEventKeys
    */
  def compareMediawikiEventKeys(key1: MediawikiEventKey, key2: MediawikiEventKey): Integer = key1.compare(key2)

  /**
    * Compare the partition-keys of two MediawikiEventKeys
    */
  def compareMediawikiEventPartitionKeys(key1: MediawikiEventKey, key2: MediawikiEventKey): Integer = {
    key1.partitionKey.compare(key2.partitionKey)
  }


  /**
    * Map partition iterators providing each element with its previous element with same key or None.
    * To be used with repartitionAndSortWithinPartition.
    *
    * WARNING: This functions assumes the given partition to be
    * strictly sorted.
    *
    * @param iterator The iterator to map (all of its items are yielded)
    * @param map The map function for each item and their optional previous item
    * @param partitionKeyComparator The partition key comparator used to compare item and its previous
    * @tparam K The key type
    * @tparam V The value type
    * @tparam Z The returned iterator type
    */
  def mapWithPreviouslyComputed[K, V, Z](
                                partitionKeyComparator: (K, K) => Integer,
                                map: ((K, V), Option[(K, Z)]) => Z
                              )
                              (
                                iterator: Iterator[(K, V)]
                              ): Iterator[Z] = {

    // Mutable variable for previous element
    var optionalPrevious = None.asInstanceOf[Option[(K, Z)]]

    // Yielding loop over iterator
    for (keyValue <- iterator) yield {
      val mappedValue = optionalPrevious match {
        // Only map with previous value when the partition-keys are equal
        case Some(previous) if partitionKeyComparator(previous._1, keyValue._1) == 0 => map(keyValue, optionalPrevious)
        case _ => map(keyValue, None)
      }
      optionalPrevious = Some(keyValue._1, mappedValue)
      mappedValue
    }
  }

  /**
    * Zip two partitions iterators yielding every left item joint with
    * either right item (on key equality) or None. To be used within the
    * repartitionAndSortWithinPartition and zipPartitions trick.
    *
    * WARNING: This functions assumes the given partitions to be
    * strictly sorted, as in if a left item is joint with a right one,
    * the next right one cannot be joint with the same left one.
    *
    * @param left The left iterator to zip (all of its items are yielded)
    * @param right The right iterator to zip (some of its items can be discarded)
    * @param join The joining function for left and optional right items
    * @param keyComparator The key comparator used to compare left and right items
    * @tparam LK The left key type
    * @tparam LV The left value type
    * @tparam RK The right key type
    * @tparam RV The right value type
    * @tparam Z The returned iterator type
    */
  def leftOuterZip[LK, LV, RK, RV, Z](
                                       keyComparator: (LK, RK) => Integer,
                                       join: ((LK, LV), Option[(RK, RV)]) => Z

                                     )
                                     (
                                       left: Iterator[(LK, LV)],
                                       right: Iterator[(RK, RV)]
                                     ): Iterator[Z] = {
    // Buffering right iterator to be able to peak into next without popping it
    val buffRight: BufferedIterator[(RK, RV)] = right.buffered

    // Yielding loop over left, since we keep all of them
    for (leftItem <- left) yield {

      // Store key comparison (if exists) preventing re-computing it
      var keyComparison = if (buffRight.hasNext) Some(keyComparator(leftItem._1, buffRight.head._1)) else None

      // Iterate through right while left key is greater than next right key
      while (keyComparison.isDefined && keyComparison.get > 0) {
        buffRight.next()
        keyComparison = if (buffRight.hasNext) Some(keyComparator(leftItem._1, buffRight.head._1)) else None
      }

      // Check next right item
      if (keyComparison.isDefined) {
        // Both left and right have values -- use keyComparison to choose join
        if (keyComparison.get == 0)
          join(leftItem, Some(buffRight.head)) // Key equality - Join !
        else if (keyComparison.get < 0)
          join(leftItem, None) // left item smaller than next right item - Nothing to join with
        else
          // We have just exhausted smaller right side -- Shouldn't happen
          throw new IllegalStateException("Incoherent keyComparator state (left bigger than right)")
      }
      else
        join(leftItem, None) // No more right items to join with
    }
  }

}
