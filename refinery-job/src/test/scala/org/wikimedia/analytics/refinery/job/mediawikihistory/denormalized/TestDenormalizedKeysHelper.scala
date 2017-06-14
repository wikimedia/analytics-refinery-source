package org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized

import java.sql.Timestamp

import org.scalatest.{Matchers, FlatSpec}
import org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized.TestHistoryEventHelpers._
import org.wikimedia.analytics.refinery.job.mediawikihistory.user.UserState

class TestDenormalizedKeysHelper extends FlatSpec with Matchers {

  // Reusable fake timestamps
  val t0 = Some(new Timestamp(0L))
  val t1 = Some(new Timestamp(1L))
  val t2 = Some(new Timestamp(2L))
  val t3 = Some(new Timestamp(3L))
  val t4 = Some(new Timestamp(4L))

  /**
    * Tests for compareHistAndStateKeys function
    */

  "compareHistAndStateKeys" should "return -1 if first is smaller than second" in {

    val histKeys: Seq[MediawikiEventKey] =
      Seq(MediawikiEventKey(PartitionKey("wiki1", -1L, -1), None, Some(1L)),
        MediawikiEventKey(PartitionKey("wiki1", 1L, -1), None, None),
        MediawikiEventKey(PartitionKey("wiki1", 1L, -1), t1, None),
        MediawikiEventKey(PartitionKey("wiki1", 1L, -1), t1, Some(1L)),
        MediawikiEventKey(PartitionKey("wiki2", 1L, -1), t1, Some(1L)),
        MediawikiEventKey(PartitionKey("wiki2", 2L, -1), t1, Some(1L)),
        MediawikiEventKey(PartitionKey("wiki2", 2L, -1), t2, Some(1L)))

    val stateKeys: Seq[StateKey] =
      Seq(StateKey(PartitionKey("wiki1", -1L, -1), t2, t2),
        StateKey(PartitionKey("wiki1", 2L, -1), None, None),
        StateKey(PartitionKey("wiki1", 1L, -1), t2, t3),
        StateKey(PartitionKey("wiki1", 1L, -1), t2, t3),
        StateKey(PartitionKey("wiki3", 1L, -1), t1, None),
        StateKey(PartitionKey("wiki2", 3L, -1), t1, None),
        StateKey(PartitionKey("wiki2", 2L, -1), t3, None))

    histKeys.zip(stateKeys).foreach {
      case (hk, sk) =>
        DenormalizedKeysHelper.compareMediawikiEventAndStateKeys(hk, sk) should equal(-1)
    }
  }

  it should "return 1 if first is bigger than second" in {
    val histKeys: Seq[MediawikiEventKey] = Seq(
      MediawikiEventKey(PartitionKey("wiki1", 2L, -1), None, None),
      MediawikiEventKey(PartitionKey("wiki1", 1L, -1), t2, None),
      MediawikiEventKey(PartitionKey("wiki3", 1L, -1), t1, Some(1L)),
      MediawikiEventKey(PartitionKey("wiki2", 3L, -1), t1, Some(1L)),
      MediawikiEventKey(PartitionKey("wiki2", 2L, -1), t3, Some(1L))
    )

    val stateKeys: Seq[StateKey] = Seq(
      StateKey(PartitionKey("wiki1", 1L, -1), None, None),
      StateKey(PartitionKey("wiki1", 1L, -1), None, t1),
      StateKey(PartitionKey("wiki2", 1L, -1), t1, None),
      StateKey(PartitionKey("wiki2", 2L, -1), t1, None),
      StateKey(PartitionKey("wiki2", 2L, -1), t2, t2)
    )

    histKeys.zip(stateKeys).foreach {
      case (hk, sk) =>
        DenormalizedKeysHelper.compareMediawikiEventAndStateKeys(hk, sk) should equal(1)
    }
  }

  it should "return 0 if first is enclosed in second" in {
    val histKeys: Seq[MediawikiEventKey] =
      Seq(
        MediawikiEventKey(PartitionKey("wiki1", -1L, -1), t2, Some(1L)),
        MediawikiEventKey(PartitionKey("wiki1", 2L, -1), t1, None),
        MediawikiEventKey(PartitionKey("wiki1", 1L, -1), t1, None))

    val stateKeys: Seq[StateKey] =
      Seq(
        StateKey(PartitionKey("wiki1", -1L, -1), t2, t3),
        StateKey(PartitionKey("wiki1", 2L, -1), None, None),
        StateKey(PartitionKey("wiki1", 1L, -1), None, t3))

    histKeys.zip(stateKeys).foreach {
      case (hk, sk) =>
        DenormalizedKeysHelper.compareMediawikiEventAndStateKeys(hk, sk) should equal(0)
    }
  }



  /**
    * Tests for leftOuterZip function
    */

  "leftOuterZip" should "group ME Events and states by key equality" in {
    val mwKeys =
      Seq(MediawikiEventKey(PartitionKey("wiki1", -1L, -1), None, Some(1L)),
        MediawikiEventKey(PartitionKey("wiki1", 1L, -1), None, None),
        MediawikiEventKey(PartitionKey("wiki1", 1L, -1), t1, Some(1L)),
        MediawikiEventKey(PartitionKey("wiki1", 1L, -1), t2, Some(2L)),
        MediawikiEventKey(PartitionKey("wiki2", 1L, -1), t1, Some(1L)),
        MediawikiEventKey(PartitionKey("wiki2", 2L, -1), t1, Some(1L)),
        MediawikiEventKey(PartitionKey("wiki2", 2L, -1), t4, Some(2L)))
        .map(k => (k, "")).iterator

    val stateKeys =
      Seq(StateKey(PartitionKey("wiki1", -1L, -1), t2, t2),
        StateKey(PartitionKey("wiki1", 1L, -1), t2, t3),
        StateKey(PartitionKey("wiki1", 1L, -1), t3, t4),
        StateKey(PartitionKey("wiki1", 2L, -1), None, None),
        StateKey(PartitionKey("wiki1", 3L, -1), None, None),
        StateKey(PartitionKey("wiki2", 2L, -1), t1, t3),
        StateKey(PartitionKey("wiki2", 2L, -1), t3, None),
        StateKey(PartitionKey("wiki3", 1L, -1), t1, None))
        .map(k => (k, "")).iterator

    val zipper = DenormalizedKeysHelper
      .leftOuterZip[MediawikiEventKey, String, StateKey, String, (MediawikiEventKey, Option[StateKey])](
        DenormalizedKeysHelper.compareMediawikiEventAndStateKeys,
        (mwe, s) => (mwe._1, s.map(_._1)))(mwKeys, stateKeys)

    val result = zipper.toVector

    val expectedResult = Seq(
      (MediawikiEventKey(PartitionKey("wiki1", -1L, -1), None, Some(1L)), None),

      (MediawikiEventKey(PartitionKey("wiki1", 1L, -1), None, None), None),
      (MediawikiEventKey(PartitionKey("wiki1", 1L, -1), t1, Some(1L)), None),
      (MediawikiEventKey(PartitionKey("wiki1", 1L, -1), t2, Some(2L)), Some(StateKey(PartitionKey("wiki1", 1L, -1), t2, t3))),

      (MediawikiEventKey(PartitionKey("wiki2", 1L, -1), t1, Some(1L)), None),
      (MediawikiEventKey(PartitionKey("wiki2", 2L, -1), t1, Some(1L)), Some(StateKey(PartitionKey("wiki2", 2L, -1), t1, t3))),
      (MediawikiEventKey(PartitionKey("wiki2", 2L, -1), t4, Some(2L)), Some(StateKey(PartitionKey("wiki2", 2L, -1), t3, None)))
    )

    result should contain theSameElementsInOrderAs expectedResult

  }

  /**
    * Tests leftOuterZip in conjunction with user and page update functions
    *
    * Remark: Tests originally written before code refactor, function
    * [[iterateSortedMwEventsAndStates]] is now defined in this test file
    * using refactored code.
    *
    */
  def iterateSortedMwEventsAndStates[S](
                                         updateMwEventFromState: (MediawikiEvent, S) => MediawikiEvent,
                                         stateName: String
                                        )(
                                         keysAndMwEvents: Iterator[(MediawikiEventKey, MediawikiEvent)],
                                         keysAndStates: Iterator[(StateKey, S)]
                                        ): Iterator[MediawikiEvent] = {
    val optJoiner = MediawikiEvent.updateWithOptionalState(updateMwEventFromState, stateName)_
    DenormalizedKeysHelper.leftOuterZip(DenormalizedKeysHelper.compareMediawikiEventAndStateKeys, optJoiner)(keysAndMwEvents, keysAndStates)
  }

  "iterateSortedMwEventsAndStates" should "not update MW Event having user or page ids undefined" in {
    val mwKeys: Seq[MediawikiEventKey] =
      Seq(MediawikiEventKey(PartitionKey("wiki1", -1L, -1), t1, None),
        MediawikiEventKey(PartitionKey("wiki1", -1L, -1), t2, None),
        MediawikiEventKey(PartitionKey("wiki1", 0L, -1), t3, None))

    val result = iterateSortedMwEventsAndStates(MediawikiEvent.updateWithUserState, "testState")(
      mwKeys.map(k => (k, emptyMwEvent)).iterator,
      Seq.empty[(StateKey, UserState)].iterator)
    result.foreach(mwe => {
      mwe.eventErrors.size should equal(1)
      mwe.eventErrors.head should equal(
          "Negative MW Event id for potential testState update")
    })
  }

  it should "not update MW Event if no state is available in state iterator" in {
    val histKeys: Seq[MediawikiEventKey] =
      Seq(MediawikiEventKey(PartitionKey("wiki1", 1L, -1), t1, None))

    val result = iterateSortedMwEventsAndStates(MediawikiEvent.updateWithUserState, "testState")(
      histKeys.map(k => (k, emptyMwEvent)).iterator,
      Seq.empty[(StateKey, UserState)].iterator)

    result.foreach(mwe => {
      mwe.eventErrors.size should equal(1)
      mwe.eventErrors.head should equal(
          "No testState match for this MW Event")
    })
  }

  it should "drop states smaller than worked MW Event" in {
    val histKeys: Seq[MediawikiEventKey] =
      Seq(MediawikiEventKey(PartitionKey("wiki1", 1L, -1), t1, None),
        MediawikiEventKey(PartitionKey("wiki1", 1L, -1), t2, None))

    val stateKeys: Seq[StateKey] = Seq(
      StateKey(PartitionKey("wiki1", 0L, -1), t1, t1),
      StateKey(PartitionKey("wiki1", 1L, -1), None, t0),
      StateKey(PartitionKey("wiki1", 1L, -1), t0, t0)
    )

    val result = iterateSortedMwEventsAndStates(MediawikiEvent.updateWithUserState, "testState")(
        histKeys.map(k => (k, emptyMwEvent)).iterator,
        stateKeys.map(k => (k, fakeUserState(-1L))).iterator)
      .toVector

    result.size should equal(2)
    result(0).eventErrors.size should equal(1)
    result(0).eventErrors.head should equal(
        "No testState match for this MW Event")
    result(1).eventErrors.size should equal(1)
    result(1).eventErrors.head should equal(
        "No testState match for this MW Event")
  }

  it should "not update MW Event smaller than worked state" in {
    val histKeys: Seq[MediawikiEventKey] =
      Seq(MediawikiEventKey(PartitionKey("wiki1", 1L, -1), t1, None),
        MediawikiEventKey(PartitionKey("wiki1", 1L, -1), t2, None))

    val stateKeys: Seq[StateKey] = Seq(
      StateKey(PartitionKey("wiki1", 2L, -1), t1, t1)
    )

    val result = iterateSortedMwEventsAndStates(MediawikiEvent.updateWithUserState, "testState")(
      histKeys.map(k => (k, emptyMwEvent)).iterator,
      stateKeys.map(k => (k, fakeUserState(-1L))).iterator)

    result.foreach(h => {
      h.eventErrors.size should equal(1)
      h.eventErrors.head should equal(
          "No testState match for this MW Event")
    })
  }

  it should "update MW Event with empty timestamp overlapping worked state being first in its partition" in {
    val keysAndHistories: Seq[(MediawikiEventKey, MediawikiEvent)] = Seq(
      (MediawikiEventKey(PartitionKey("wiki1", 1L, -1), None, None),
        emptyMwEvent)
    )

    val keysAndStates: Seq[(StateKey, UserState)] = Seq(
      (StateKey(PartitionKey("wiki1", 0L, -1), t1, t1), fakeUserState(-1L)),
      (StateKey(PartitionKey("wiki1", 1L, -1), None, t1), fakeUserState(userId = 101L)),
      (StateKey(PartitionKey("wiki1", 1L, -1), t2, t2), fakeUserState(-1L))
    )

    val result = iterateSortedMwEventsAndStates(MediawikiEvent.updateWithUserState, "testState")(
        keysAndHistories.iterator,
        keysAndStates.iterator)
      .toVector

    result.size should equal(1)

    result(0).eventErrors.size should equal(0)
    result(0).eventUserDetails.userId should equal(Some(101L))

  }

  it should "update MW Event with some timestamp overlapping worked state with no start and end being alone in its partition" in {
    val keysAndHistories: Seq[(MediawikiEventKey, MediawikiEvent)] = Seq(
      (MediawikiEventKey(PartitionKey("wiki1", 1L, -1), t1, None),
        emptyMwEvent)
    )

    val keysAndStates: Seq[(StateKey, UserState)] = Seq(
      (StateKey(PartitionKey("wiki1", 0L, -1), t1, t1), fakeUserState(-1L)),
      (StateKey(PartitionKey("wiki1", 1L, -1), None, None), fakeUserState(101L)),
      (StateKey(PartitionKey("wiki1", 2L, -1), t2, t2), fakeUserState(-1L))
    )

    val result = iterateSortedMwEventsAndStates(MediawikiEvent.updateWithUserState, "testState")(
        keysAndHistories.iterator,
        keysAndStates.iterator)
      .toVector

    result.size should equal(1)

    result(0).eventErrors.size should equal(0)
    result(0).eventUserDetails.userId should equal(Some(101L))
  }

  it should "update MW Event with some timestamp overlapping worked state with start and/or end" in {
    val keysAndHistories: Seq[(MediawikiEventKey, MediawikiEvent)] = Seq(
      (MediawikiEventKey(PartitionKey("wiki1", 1L, -1), t1, None), emptyMwEvent),
      (MediawikiEventKey(PartitionKey("wiki1", 2L, -1), t1, None), emptyMwEvent),
      (MediawikiEventKey(PartitionKey("wiki1", 3L, -1), t2, None), emptyMwEvent)
    )

    val keysAndStates: Seq[(StateKey, UserState)] = Seq(
      (StateKey(PartitionKey("wiki1", 1L, -1), None, t2), fakeUserState(userId = 101L)),
      (StateKey(PartitionKey("wiki1", 2L, -1), t1, None), fakeUserState(userId = 102L)),
      (StateKey(PartitionKey("wiki1", 3L, -1), t1, t3), fakeUserState(userId = 103L))
    )

    val result = iterateSortedMwEventsAndStates(MediawikiEvent.updateWithUserState, "testState")(
        keysAndHistories.iterator,
        keysAndStates.iterator)
      .toVector

    result.size should equal(3)

    result(0).eventErrors.size should equal(0)
    result(0).eventUserDetails.userId should equal(Some(101L))
    result(1).eventErrors.size should equal(0)
    result(1).eventUserDetails.userId should equal(Some(102L))
    result(2).eventErrors.size should equal(0)
    result(2).eventUserDetails.userId should equal(Some(103L))
  }

  /**
    * Tests for mapWithPrevious function
    */

  "mapWithPrevious" should "map ME Events with their previous by partition-key equality" in {
    val mwKeys =
      Seq(MediawikiEventKey(PartitionKey("wiki1", -1L, -1), None, Some(1L)),
        MediawikiEventKey(PartitionKey("wiki1", 1L, -1), None, None),
        MediawikiEventKey(PartitionKey("wiki1", 1L, -1), t1, Some(1L)),
        MediawikiEventKey(PartitionKey("wiki1", 1L, -1), t2, Some(2L)),
        MediawikiEventKey(PartitionKey("wiki2", 1L, -1), t1, Some(1L)),
        MediawikiEventKey(PartitionKey("wiki2", 2L, -1), t1, Some(1L)),
        MediawikiEventKey(PartitionKey("wiki2", 2L, -1), t4, Some(2L)))
        .map(k => (k, "")).iterator

    val zipper = DenormalizedKeysHelper
      .mapWithPreviouslyComputed[MediawikiEventKey, String, (MediawikiEventKey, Option[MediawikiEventKey])](
      DenormalizedKeysHelper.compareMediawikiEventPartitionKeys,
      (mwe, pmwe) => (mwe._1, pmwe.map(_._1)))(mwKeys)

    val result = zipper.toVector

    val expectedResult = Seq(
      (MediawikiEventKey(PartitionKey("wiki1", -1L, -1), None, Some(1L)), None),

      (MediawikiEventKey(PartitionKey("wiki1", 1L, -1), None, None), None),
      (MediawikiEventKey(PartitionKey("wiki1", 1L, -1), t1, Some(1L)), Some(MediawikiEventKey(PartitionKey("wiki1", 1L, -1), None, None))),
      (MediawikiEventKey(PartitionKey("wiki1", 1L, -1), t2, Some(2L)), Some(MediawikiEventKey(PartitionKey("wiki1", 1L, -1), t1, Some(1L)))),

      (MediawikiEventKey(PartitionKey("wiki2", 1L, -1), t1, Some(1L)), None),

      (MediawikiEventKey(PartitionKey("wiki2", 2L, -1), t1, Some(1L)), None),
      (MediawikiEventKey(PartitionKey("wiki2", 2L, -1), t4, Some(2L)), Some(MediawikiEventKey(PartitionKey("wiki2", 2L, -1), t1, Some(1L))))
    )

    result should contain theSameElementsInOrderAs expectedResult

  }


}