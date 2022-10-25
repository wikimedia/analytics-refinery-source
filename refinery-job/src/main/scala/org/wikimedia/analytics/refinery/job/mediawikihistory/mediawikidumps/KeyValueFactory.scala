package org.wikimedia.analytics.refinery.job.mediawikihistory.mediawikidumps

import org.apache.hadoop.io.{Writable, WritableComparable}

/**
 * Trait defining a Key/Value factory generating the keys and values
 * out of mediawiki-revision objects.
 * It also provides a filtering function to allows for filtering out
 * revisions from the read dataset.
 *
 * @tparam K The type of the key to be generated
 * @tparam V The type of the value to be generated
 * @tparam MwObjectsFactory The [[MediawikiObjectsFactory]] implementation
 *                          from which given revisions have been created
 */
trait KeyValueFactory[
  K <: WritableComparable[K],
  V <: Writable,
  MwObjectsFactory <: MediawikiObjectsFactory
] {

  def newKey(): K
  def setKey(key: K, pos: Long, rev: MwObjectsFactory#MwRev): Unit

  def newValue(): V
  def setValue(value: V, rev: MwObjectsFactory#MwRev): Unit

  /**
   * By default don't filter out any revision. Override this method
   * to filter out some revisions from the parsed dataset.
   * @param rev The revision to check
   * @return True is the revision is to be filtered out
   */
  def filterOut(rev: MwObjectsFactory#MwRev): Boolean = false

}
