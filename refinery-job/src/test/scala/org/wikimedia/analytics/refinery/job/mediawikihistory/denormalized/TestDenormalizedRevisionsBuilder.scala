package org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{BeforeAndAfterEach, Matchers, FlatSpec}
import TestHistoryEventHelpers._
import org.wikimedia.analytics.refinery.job.mediawikihistory.page.TestPageHistoryHelpers._

class TestDenormalizedRevisionsBuilder extends FlatSpec
with Matchers
with BeforeAndAfterEach
with SharedSparkContext {

  /**
    * Tests for FirstBigger function
    */
  "firstBigger" should "return the first element bigger than ref in vector" in {

    val ref = "b"
    val vec = Vector("a", "b", "c", "d")

    val result = DenormalizedRevisionsBuilder.firstBigger(ref, vec)
    result should equal(Some("c"))

  }

  it should "return the first element bigger than empty in vector" in {

    val ref = ""
    val vec = Vector("", "a", "b", "c", "d")

    val result = DenormalizedRevisionsBuilder.firstBigger(ref, vec)
    result should equal(Some("a"))

  }

  it should "return None if no element is bigger than ref in vector" in {

    val ref = "e"
    val vec = Vector("", "a", "b", "c", "d")

    val result = DenormalizedRevisionsBuilder.firstBigger(ref, vec)
    result should equal(None)

  }

  /**
    * Tests for populateDeleteTime function
    */
  "populateDeleteTime" should "put max rev ts when no page state match" in {

    val revs = sc.parallelize(
      revisionMwEventSet()(
        "time revId pageId  isDeleted",
        "01    1      1        true",
        "02    2      1        true"
      ))

    val pageStates = sc.parallelize(
      pageStateSet()(
        "id",
        "2"
      ))

    val expectedResults = revisionMwEventSet()(
      "time  revId   pageId  isDeleted  deleteTime",
      "01      1       1       true         02",
      "02      2       1       true         02"
    )

    val results = DenormalizedRevisionsBuilder
      .populateDeleteTime(revs, pageStates)
      .collect
      .sortBy(_.revisionDetails.revId)

    results should equal(expectedResults)
  }

  it should "put the first page ts when no revision ts" in {

    val revs = sc.parallelize(
      revisionMwEventSet()(
        "time revId pageId  isDeleted",
        "None    1      2        true"
      ))

    val pageStates = sc.parallelize(
      pageStateSet()(
        "id   type     start",
        "2   delete      01",
        "2   delete      04"
      ))

    val expectedResults = revisionMwEventSet()(
      "time  revId   pageId  isDeleted  deleteTime",
      "None    1       2       true         01"
    )

    val results = DenormalizedRevisionsBuilder
      .populateDeleteTime(revs, pageStates)
      .collect
      .sortBy(_.revisionDetails.revId)

    results should equal(expectedResults)
  }

  it should "put correct page ts when state match" in {

    val revs = sc.parallelize(
      revisionMwEventSet()(
        "time revId pageId  isDeleted",
        "02    1      2        true",
        "03    2      2        true"
      ))

    val pageStates = sc.parallelize(
      pageStateSet()(
        "id   type     start",
        "2   delete      01",
        "2   delete      04"
      ))

    val expectedResults = revisionMwEventSet()(
      "time  revId   pageId  isDeleted  deleteTime",
      "02      1       2       true         04",
      "03      2       2       true         04"
    )

    val results = DenormalizedRevisionsBuilder
      .populateDeleteTime(revs, pageStates)
      .collect
      .sortBy(_.revisionDetails.revId)

    results should equal(expectedResults)
  }

  it should "correctly handle multiple pages" in {

    val revs = sc.parallelize(
      revisionMwEventSet()(
        "time revId pageId  isDeleted",
        "02    1      1        true",
        "03    2      2        true",
        "04    3      1        true",
        "06    4      2        true"
      ))

    val pageStates = sc.parallelize(
      pageStateSet()(
        "id   type     start",
        "2   delete      05",
        "2   delete      07"
      ))

    val expectedResults = revisionMwEventSet()(
      "time  revId   pageId  isDeleted  deleteTime",
      "02      1      1        true        04",
      "03      2      2        true        05",
      "04      3      1        true        04",
      "06      4      2        true        07"
    )

    val results = DenormalizedRevisionsBuilder
      .populateDeleteTime(revs, pageStates)
      .collect
      .sortBy(_.revisionDetails.revId)

    results should equal(expectedResults)
  }

  /**
    * Tests for populateByteDiff function
    */
  "populateByteDiff" should "set None bytes diff if no match" in {

    val revs = sc.parallelize(
      revisionMwEventSet()(
        "time revId parentId bytes bytesDiff",
        "01    1      0        90    90",
        "02    3      2        80    80"
      ))

    val expectedResults = revisionMwEventSet()(
      "time  revId   parentId bytes bytesDiff",
      "01      1        0      90      90",
      "02      3        2      80      None"
    )

    val results = DenormalizedRevisionsBuilder
      .populateByteDiff(revs)
      .collect
      .sortBy(_.revisionDetails.revId)

    results should equal(expectedResults)
  }

  it should "compute bytes diff if match" in {

    val revs = sc.parallelize(
      revisionMwEventSet()(
        "time revId parentId bytes bytesDiff",
        "01    1      0        90    90",
        "02    2      1        80    80"
      ))

    val expectedResults = revisionMwEventSet()(
      "time  revId   parentId bytes bytesDiff",
      "01      1        0      90      90",
      "02      2        1      80      -10"
    )

    val results = DenormalizedRevisionsBuilder
      .populateByteDiff(revs)
      .collect
      .sortBy(_.revisionDetails.revId)

    results should equal(expectedResults)
  }

  /**
    * Tests for prepareReverts function
    */
  "prepareReverts" should "properly prepare reverts" in {
    val revs = sc.parallelize(
      revisionMwEventSet()(
        "db       time       revId pageId sha1",
        "w1  20100101000000    1      1     s1",
        "w1  20100102000000    2      1     s2",
        "w1  20100103000000    3      1     s3",
        "w1  20100104000000    4      1     s2",
        "w1  20100105000000    5      1     s5",
        "w1  20100106000000    6      1     s1",
        "w1  20100107000000    7      1     s7",
        "w1  20100108000000    8      1     s5",
        "w1  20100109000000    12     1     s1",
        "w1  20100110000000    14     1     s14",
        "w1  20100103000000    9      2     s9",
        "w1  20100104000000    10     2     s10",
        "w1  20110101000000    11     2     s9",
        "w2  20100101000000    1      1     s1",
        "w2  20100102000000    2      1     s2",
        "w2  20100103000000    3      1     s1"
      ))

    val results = DenormalizedRevisionsBuilder.prepareRevertsLists(revs).collect.sortBy(_._1)

    val partw1p1 = PartitionKey("w1", 1L, "2010")
    val partw1p2_2010 = PartitionKey("w1", 2L, "2010")
    val partw1p2_2011 = PartitionKey("w1", 2L, "2011")
    val partw2p1 = PartitionKey("w2", 1L, "2010")
    val expectedResults = Seq(
      (MediawikiEventKey(partw1p1, Some("20100101000000"), Some(1L)),
        Vector((Some("20100106000000"), Some(6L)), (Some("20100109000000"), Some(12L)))),
      (MediawikiEventKey(partw1p1, Some("20100102000000"), Some(2L)),
        Vector((Some("20100104000000"), Some(4L)))),
      (MediawikiEventKey(partw1p1, Some("20100105000000"), Some(5L)),
        Vector((Some("20100108000000"), Some(8L)))),
      (MediawikiEventKey(partw1p2_2010, Some("20100103000000"), Some(9L)),
        Vector((Some("20110101000000"), Some(11L)))),
      (MediawikiEventKey(partw1p2_2011, Some("20100103000000"), Some(9L)),
        Vector((Some("20110101000000"), Some(11L)))),
      (MediawikiEventKey(partw2p1, Some("20100101000000"), Some(1L)),
        Vector((Some("20100103000000"), Some(3L))))
    )

    results should equal(expectedResults)
  }

  /**
    * Tests for updateRevisionAndReverts function
    */

  "updateRevisionAndReverts" should "not update MW Event if no endRevert" in {
    val revs = revisionMwEventSet()(
      "db        time       revId pageId sha1 revert reverted",
      "w1   19700101000000    1      1     s1  false   false"
    )

    val endReverts = new scala.collection.mutable.TreeSet[((Option[String], Option[Long]), Option[Long])]

    revs.foreach(r => {
      val res = DenormalizedRevisionsBuilder.updateRevisionAndReverts(r, endReverts)
      res.revisionDetails.revIsIdentityRevert should equal(Some(false))
      res.revisionDetails.revIsIdentityReverted should equal(Some(false))
      res.revisionDetails.revFirstIdentityRevertingRevisionId should equal(None)
      res.revisionDetails.revSecondsToIdentityRevert should equal(None)
    })
  }

  it should "update MW Event and not endReverts - isReverted case" in {
    val revs = revisionMwEventSet()(
      "db       time        revId pageId sha1 revert reverted",
      "w1  19700101000000      1      1     s1  false   false"
    )

    val endReverts = new scala.collection.mutable.TreeSet[((Option[String], Option[Long]), Option[Long])]
    endReverts.add((Some("19710101000000"), Some(2L)), None)

    revs.foreach(r => {
      val res = DenormalizedRevisionsBuilder.updateRevisionAndReverts(r, endReverts)
      res.revisionDetails.revIsIdentityRevert should equal(Some(false))
      res.revisionDetails.revIsIdentityReverted should equal(Some(true))
      res.revisionDetails.revFirstIdentityRevertingRevisionId should equal(Some(2L))
      res.revisionDetails.revSecondsToIdentityRevert should equal(Some(31536000))
    })
    endReverts.size should equal(1)
  }

  it should "update MW Event and endReverts - isRevert case (not isReverted, no other revert)" in {
    val revs = revisionMwEventSet()(
      "db         time     revId pageId sha1 revert reverted",
      "w1   19700101000000    1      1     s1  false   false"
    )

    val endReverts = new scala.collection.mutable.TreeSet[((Option[String], Option[Long]), Option[Long])]
    endReverts.add((Some("19700101000000"), Some(1L)), None)

    revs.foreach(r => {
      val res = DenormalizedRevisionsBuilder.updateRevisionAndReverts(r, endReverts)
      res.revisionDetails.revIsIdentityRevert should equal(Some(true))
      res.revisionDetails.revIsIdentityReverted should equal(Some(false))
      res.revisionDetails.revFirstIdentityRevertingRevisionId should equal(None)
      res.revisionDetails.revSecondsToIdentityRevert should equal(None)
    })
    endReverts.size should equal(0)
  }

  it should "update MW Event and endReverts - isRevert case (not isReverted, same wider revert)" in {
    val revs = revisionMwEventSet()(
      "db       time        revId pageId sha1 revert reverted",
      "w1   19700101000000    2      1     s1  false   false"
    )

    val endReverts = new scala.collection.mutable.TreeSet[((Option[String], Option[Long]), Option[Long])]
    endReverts.add((Some("19700101000000"), Some(2L)), Some(1L))
    endReverts.add((Some("19700102000000"), Some(3L)), Some(1L))

    revs.foreach(r => {
      val res = DenormalizedRevisionsBuilder.updateRevisionAndReverts(r, endReverts)
      res.revisionDetails.revIsIdentityRevert should equal(Some(true))
      res.revisionDetails.revIsIdentityReverted should equal(Some(false))
      res.revisionDetails.revFirstIdentityRevertingRevisionId should equal(None)
      res.revisionDetails.revSecondsToIdentityRevert should equal(None)
    })
    endReverts.size should equal(1)
  }


  it should "update MW Event and endReverts - isRevert and isReverted case (different wider revert)" in {
    val revs = revisionMwEventSet()(
      "db        time       revId pageId sha1 revert reverted",
      "w1   19700101000000    3      1     s1  false   false"
    )

    val endReverts = new scala.collection.mutable.TreeSet[((Option[String], Option[Long]), Option[Long])]
    endReverts.add((Some("19700101000000"), Some(3L)), Some(2L))
    endReverts.add((Some("19700101100000"), Some(4L)), Some(1L))

    revs.foreach(r => {
      val res = DenormalizedRevisionsBuilder.updateRevisionAndReverts(r, endReverts)
      res.revisionDetails.revIsIdentityRevert should equal(Some(true))
      res.revisionDetails.revIsIdentityReverted should equal(Some(true))
      res.revisionDetails.revFirstIdentityRevertingRevisionId should equal(Some(4L))
      res.revisionDetails.revSecondsToIdentityRevert should equal(Some(36000))
    })
    endReverts.size should equal(1)
  }



  /**
    * Tests for iterateSortedRevisionsAndRevertsLists function
    *
    * Tests [[DenormalizedKeysHelper.leftOuterZip]] in conjunction with revertsList update function
    *
    * Remark: Tests originally written before code refactor, function
    * [[iterateSortedRevisionsAndRevertsLists]] is now defined in this test file
    * using refactored code.
    *
    */

  def iterateSortedRevisionsAndRevertsLists(
                                             keysAndRevisions: Iterator[(MediawikiEventKey, MediawikiEvent)],
                                             keysAndRevertsLists: Iterator[(MediawikiEventKey, Vector[(Option[String], Option[Long])])]
                                           ): Iterator[MediawikiEvent] = {



    DenormalizedKeysHelper.leftOuterZip(
      DenormalizedKeysHelper.compareMediawikiEventKeys,
      DenormalizedRevisionsBuilder.updateRevisionWithOptionalRevertsList(new DenormalizedRevisionsBuilder.RevertsListsState)
    )(
      keysAndRevisions,
      keysAndRevertsLists
    )

  }



  "iterateSortedRevisionsAndRevertsLists" should "not update revision if no revert is present" in {
    val revs = revisionMwEventSet()(
      "db      time        revId pageId sha1 revert reverted",
      "w1  19700101000000    1      1     s1  false   false"
    )

    val results = iterateSortedRevisionsAndRevertsLists(
      revs.map(r => (DenormalizedKeysHelper.pageMediawikiEventKey(r), r)).iterator,
      Seq.empty[(MediawikiEventKey, Vector[(Option[String], Option[Long])])].iterator
    )

    results.foreach(r => {
      r.revisionDetails.revIsIdentityRevert should equal(Some(false))
      r.revisionDetails.revIsIdentityReverted should equal(Some(false))
      r.revisionDetails.revFirstIdentityRevertingRevisionId should equal(None)
      r.revisionDetails.revSecondsToIdentityRevert should equal(None)
    })
  }

  it should "correctly update revision if a revert is present" in {
    val revs = revisionMwEventSet()(
      "db        time       revId pageId sha1 revert reverted",
      "w0   19700101000000    1      1     s1  false   false",
      "w1   19700101000000    1      1     s1  false   false",
      "w1   19700102000000    2      1     s2  false   false",
      "w1   19700103000000    3      1     s1  false   false"
    )

    val reverts = Seq(
      (MediawikiEventKey(PartitionKey("w1", 1L, "1970"), Some("19700101000000"), Some(1L)),
        Vector((Some("19700103000000"), Some(3L))))
    )

    val expectedResults = revisionMwEventSet()(
      "db       time        revId pageId sha1 revert reverted revertId    secondsToRevert",
      "w0   19700101000000    1      1     s1  false   false   None         None ",
      "w1   19700101000000    1      1     s1  false   false   None         None ",
      "w1   19700102000000    2      1     s2  false   true     3           86400  ",
      "w1   19700103000000    3      1     s1  true   false    None         None "
    )

    val results = iterateSortedRevisionsAndRevertsLists(
      revs.map(r => (DenormalizedKeysHelper.pageMediawikiEventKey(r), r)).iterator,
      reverts.iterator
    ).toVector

    results should equal(expectedResults)
  }

  it should "correctly change pages even if end of revert not matched" in {
    val revs = revisionMwEventSet()(
      "db        time       revId pageId sha1 revert reverted",
      "w1   19700101000000    1      1     s1  false   false",
      "w1   19700102000000    2      1     s2  false   false",
      "w2   19700103000000    3      1     s1  false   false"
    )

    val reverts = Seq(
      (MediawikiEventKey(PartitionKey("w1", 1L, "1970"), Some("19700101000000"), Some(1L)),
        Vector((Some("19700103000000"), Some(3L))))
    )

    val expectedResults = revisionMwEventSet()(
      "db        time       revId pageId sha1 revert reverted revertId     secondsToRevert",
      "w1   19700101000000    1      1     s1  false   false   None          None",
      "w1   19700102000000    2      1     s2  false   true     3            86400",
      "w2   19700103000000    3      1     s1  false   false    None         None"
    )

    val results = iterateSortedRevisionsAndRevertsLists(
      revs.map(r => (DenormalizedKeysHelper.pageMediawikiEventKey(r), r)).iterator,
      reverts.iterator
    ).toVector

    results should equal(expectedResults)
  }

  it should "correctly drop reverts until revision matches" in {
    val revs = revisionMwEventSet()(
      "db        time       revId pageId sha1 revert reverted",
      "w1   19700101000000    1      1     s1  false   false",
      "w1   19700102000000    2      1     s2  false   false",
      "w1   19700103000000    3      1     s1  false   false"
    )

    val reverts = Seq(
      (MediawikiEventKey(PartitionKey("w0", 1L, "1970"), Some("19700101000000"), Some(1L)),
        Vector((Some("19700103000000"), Some(3L)))),
      (MediawikiEventKey(PartitionKey("w0", 2L, "1970"), Some("19700101000000"), Some(1L)),
        Vector((Some("19700103000000"), Some(3L)))),
      (MediawikiEventKey(PartitionKey("w1", 1L, "1970"), Some("19700101000000"), Some(1L)),
        Vector((Some("19700103000000"), Some(3L))))
    )

    val expectedResults = revisionMwEventSet()(
      "db        time       revId pageId sha1 revert reverted revertId     secondsToRevert",
      "w1   19700101000000    1      1     s1  false   false   None          None",
      "w1   19700102000000    2      1     s2  false   true     3        86400",
      "w1   19700103000000    3      1     s1  true   false    None         None"
    )

    val results = iterateSortedRevisionsAndRevertsLists(
      revs.map(r => (DenormalizedKeysHelper.pageMediawikiEventKey(r), r)).iterator,
      reverts.iterator
    ).toVector

    results should equal(expectedResults)
  }

  it should "correctly update revision in case of nested reverts" in {
    val revs = revisionMwEventSet()(
      "db        time       revId pageId sha1 revert reverted",
      "w0   19700101000000    1      1     s1  false   false",

      "w1   19700101000000    1      1     s1  false   false",
      "w1   19700102000000    2      1     s2  false   false",
      "w1   19700103000000    3      1     s3  false   false",
      "w1   19700104000000    4      1     s2  false   false",
      "w1   19700105000000    5      1     s5  false   false",
      "w1   19700106000000    6      1     s1  false   false",
      "w1   19700107000000    7      1     s7  false   false",
      "w1   19700108000000    8      1     s3  false   false",
      "w1   19700109000000    9      1     s9  false   false",

      "w2   19700102000000    1      1     s1  false   false"
    )

    val reverts = Seq(
      (MediawikiEventKey(PartitionKey("w1", 1L, "1970"), Some("19700101000000"), Some(1L)),
        Vector((Some("19700106000000"), Some(6L)))),
      (MediawikiEventKey(PartitionKey("w1", 1L, "1970"), Some("19700102000000"), Some(2L)),
        Vector((Some("19700104000000"), Some(4L)))),
      (MediawikiEventKey(PartitionKey("w1", 1L, "1970"), Some("19700103000000"), Some(3L)),
        Vector((Some("19700108000000"), Some(8L))))
    )

    val expectedResults = revisionMwEventSet()(
      "db        time       revId pageId sha1 revert reverted revertId     secondsToRevert",
      "w0   19700101000000    1      1     s1  false   false   None          None",

      "w1   19700101000000    1      1     s1  false   false   None          None",
      "w1   19700102000000    2      1     s2  false  true      6       345600",
      "w1   19700103000000    3      1     s3  false  true      4       86400",
      "w1   19700104000000    4      1     s2  true   true      6       172800",
      "w1   19700105000000    5      1     s5  false  true      6       86400",
      "w1   19700106000000    6      1     s1  true   true      8       172800",
      "w1   19700107000000    7      1     s7  false  true      8       86400",
      "w1   19700108000000    8      1     s3  true   false    None         None",
      "w1   19700109000000    9      1     s9  false  false    None         None",

      "w2   19700102000000    1      1     s1  false   false   None         None"
    )

    val results = iterateSortedRevisionsAndRevertsLists(
      revs.map(r => (DenormalizedKeysHelper.pageMediawikiEventKey(r), r)).iterator,
      reverts.iterator
    ).toVector

    results should equal(expectedResults)
  }

}
