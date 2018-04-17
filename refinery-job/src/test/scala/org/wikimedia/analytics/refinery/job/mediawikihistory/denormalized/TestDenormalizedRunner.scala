package org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized

import java.sql.Timestamp

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized.TestHistoryEventHelpers._
import org.wikimedia.analytics.refinery.job.mediawikihistory.user.UserState
import org.wikimedia.analytics.refinery.spark.utils.MapAccumulator


class TestDenormalizedRunner
    extends FlatSpec
    with Matchers
    with BeforeAndAfterEach
    with DataFrameSuiteBase {

  implicit val sumLongs = (a: Long, b: Long) => a + b
  var statsAccumulator = None.asInstanceOf[Option[MapAccumulator[String, Long]]]
  var denormalizedRunner = null.asInstanceOf[DenormalizedRunner]

  override def beforeEach(): Unit = {
    statsAccumulator = Some(new MapAccumulator[String, Long])
    denormalizedRunner = new DenormalizedRunner(spark, statsAccumulator, 1)
    statsAccumulator.foreach(statsAcc => spark.sparkContext.register(statsAcc))
  }

  /**
    * Tests for filterStates function
    */

  "filterStates" should "filter non-single whole-history state from partition" in {
    val userStates = sc.parallelize(Seq(
      fakeUserState(1L).copy(startTimestamp = None, endTimestamp = None),
      fakeUserState(1L).copy(startTimestamp = None, endTimestamp = Some(new Timestamp(1L))),
      fakeUserState(2L).copy(startTimestamp = Some(new Timestamp(2L)), endTimestamp = None)
    ))

    val result = denormalizedRunner
      .filterStates[UserState](userStates, DenormalizedKeysHelper.userStateKey, "test_user")
      .collect()

    val expectedResults = Seq(
      fakeUserState(1L).copy(startTimestamp = None, endTimestamp = Some(new Timestamp(1L))),
      fakeUserState(2L).copy(startTimestamp = Some(new Timestamp(2L)), endTimestamp = None))

    result.length should be(2)
    result should contain theSameElementsAs expectedResults
  }

  it should "filter 2 whole-history states from partition" in {
    val userStates = sc.parallelize(Seq(
      fakeUserState(1L).copy(startTimestamp = None, endTimestamp = None),
      fakeUserState(1L).copy(startTimestamp = None, endTimestamp = None)
    ))

    val result = denormalizedRunner
      .filterStates[UserState](userStates, DenormalizedKeysHelper.userStateKey, "test_user")
      .collect()

    result.length should be(0)
  }

  it should "not filter single whole-history states from partition" in {
    val userStates = Seq(
      fakeUserState(1L).copy(startTimestamp = None, endTimestamp = None),
      fakeUserState(2L).copy(startTimestamp = None, endTimestamp = Some(new Timestamp(1L))),
      fakeUserState(2L).copy(startTimestamp = Some(new Timestamp(2L)), endTimestamp = None))

    val userStatesRdd = sc.parallelize(userStates)

    val result = denormalizedRunner
      .filterStates[UserState](userStatesRdd, DenormalizedKeysHelper.userStateKey, "test_user")
      .collect()

    result.length should be(3)
    result should contain theSameElementsAs userStates
  }

}
