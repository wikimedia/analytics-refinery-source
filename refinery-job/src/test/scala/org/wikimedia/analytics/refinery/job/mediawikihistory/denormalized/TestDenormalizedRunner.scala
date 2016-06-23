package org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import TestHistoryEventHelpers._
import org.wikimedia.analytics.refinery.job.mediawikihistory.user.UserState


class TestDenormalizedRunner
    extends FlatSpec
    with Matchers
    with BeforeAndAfterEach
    with SharedSparkContext {

  var denormalizedRunner = null.asInstanceOf[DenormalizedRunner]

  override def beforeEach(): Unit = {
    denormalizedRunner = new DenormalizedRunner(new SQLContext(sc))
  }

  /**
    * Tests for filterStates function
    */

  "filterStates" should "filter non-single whole-history state from partition" in {
    val userStates = sc.parallelize(Seq(
      fakeUserState(1L).copy(startTimestamp = None, endTimestamp = None),
      fakeUserState(1L).copy(startTimestamp = None, endTimestamp = Some("1")),
      fakeUserState(2L).copy(startTimestamp = Some("2"), endTimestamp = None)
    ))

    val result = denormalizedRunner
      .filterStates[UserState](userStates, DenormalizedKeysHelper.userStateKeyNoYear)
      .collect()

    val expectedResults = Seq(
      fakeUserState(1L).copy(startTimestamp = None, endTimestamp = Some("1")),
      fakeUserState(2L).copy(startTimestamp = Some("2"), endTimestamp = None))

    result.length should be(2)
    result should contain theSameElementsAs expectedResults
  }

  it should "filter 2 whole-history states from partition" in {
    val userStates = sc.parallelize(Seq(
      fakeUserState(1L).copy(startTimestamp = None, endTimestamp = None),
      fakeUserState(1L).copy(startTimestamp = None, endTimestamp = None)
    ))

    val result = denormalizedRunner
      .filterStates[UserState](userStates, DenormalizedKeysHelper.userStateKeyNoYear)
      .collect()

    result.length should be(0)
  }

  it should "not filter single whole-history states from partition" in {
    val userStates = Seq(
      fakeUserState(1L).copy(startTimestamp = None, endTimestamp = None),
      fakeUserState(2L).copy(startTimestamp = None, endTimestamp = Some("1")),
      fakeUserState(2L).copy(startTimestamp = Some("2"), endTimestamp = None))

    val userStatesRdd = sc.parallelize(userStates)

    val result = denormalizedRunner
      .filterStates[UserState](userStatesRdd, DenormalizedKeysHelper.userStateKeyNoYear)
      .collect()

    result.length should be(3)
    result should contain theSameElementsAs userStates
  }

}
