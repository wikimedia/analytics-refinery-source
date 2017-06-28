package org.wikimedia.analytics.refinery.job.mediawikihistory.page

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.job.mediawikihistory.utils.TimestampHelpers

class TestPageEventBuilder extends FlatSpec with Matchers {

  "normalizeTitle" should "transform all spaces in underscores" in {
    val testData = Seq(
        ("nothing_to_change", "nothing_to_change"),
        ("one space_to_change", "one_space_to_change"),
        ("two spaces to_change", "two_spaces_to_change"),
        ("three spaces to change", "three_spaces_to_change"),
        (" trimming test  ", "trimming_test"),
        ("", "")
    )

    testData.foreach {
      case (test, expectedResult) =>
        PageEventBuilder.normalizeTitle(test) should equal(
            expectedResult)
    }
  }

  it should "raise an NullPointerException in case of null title" in {
    an[NullPointerException] should be thrownBy PageEventBuilder
      .normalizeTitle(null)
  }

  "getOldAndNewTitles" should "work correctly with php dictionary" in {
    val testLogTitle = "Johannes Chrysostomus Wolfgangus Theophilus Mozart"
    val testLogParam = "a:2:{s:9:\"4::target\";s:23:\"Wolfgang Amadeus Mozart\";s:10:\"5::noredir\";s:1:\"0\";}"

    val expectedOldTitle = "Johannes_Chrysostomus_Wolfgangus_Theophilus_Mozart"
    val expectedNewTitle = "Wolfgang_Amadeus_Mozart"

    PageEventBuilder
      .getOldAndNewTitles(testLogTitle, testLogParam) should equal((expectedOldTitle, expectedNewTitle))
  }

  it should "work correctly with a non php dictionary" in {
    val testLogTitle = "test old title"
    val testLogParam = "test new title"

    val expectedOldTitle = "test_old_title"
    val expectedNewTitle = "test_new_title"

    PageEventBuilder
      .getOldAndNewTitles(testLogTitle, testLogParam) should equal((expectedOldTitle, expectedNewTitle))
  }

  it should "return an event with error in case of null title or params" in {
    an[NullPointerException] should be thrownBy PageEventBuilder
      .getOldAndNewTitles(null, "")
    an[NullPointerException] should be thrownBy PageEventBuilder
      .getOldAndNewTitles("", null)
  }

  val wikiDb = "fakewiki"
  val eventType = "move"
  val namespaces =
    Seq((0, "", "", 1), (1, "User", "Localized User", 0))
  val canonicalNamespaceMap = namespaces
    .map(t =>
      (wikiDb, PageEventBuilder.normalizeTitle(t._2)) -> t._1)
    .toMap
  val localizedNamespaceMap = namespaces
    .map(t =>
      (wikiDb, PageEventBuilder.normalizeTitle(t._3)) -> t._1)
    .toMap
  val isContentNamespaceMap = namespaces
    .map(t => (wikiDb, t._1) -> (t._4 == 1))
    .toMap
  val pageMoveParser = {
    PageEventBuilder
      .buildMovePageEvent(canonicalNamespaceMap, localizedNamespaceMap, isContentNamespaceMap) _
  }

  "parsePageLog" should "work for namespace 0 event" in {

    val testRow = Row.fromTuple((eventType,
      "20130202200839",
      220966L,
      "The_Night_Watch",
      "a:2:{s:9:\"4::target\";s:14:\"The Nightwatch\";s:10:\"5::noredir\";s:1:\"0\";}",
      0,
      wikiDb))
    val expectedEvent = new PageEvent(
      wikiDb = wikiDb,
      oldTitle = "The_Night_Watch",
      newTitle = "The_Nightwatch",
      newTitlePrefix = "",
      newTitleWithoutPrefix = "The_Nightwatch",
      oldNamespace = 0,
      oldNamespaceIsContent = true,
      newNamespace = 0,
      newNamespaceIsContent = true,
      timestamp = TimestampHelpers.makeMediawikiTimestamp("20130202200839").get,
      eventType = eventType,
      causedByUserId = Some(220966L)
    )

    pageMoveParser(testRow) should equal(expectedEvent)

  }

  it should "work for canonical namespace non-0 event" in {

    val testRow = Row.fromTuple((eventType,
      "20130202200839",
      220966L,
      "test_user",
      "a:2:{s:9:\"4::target\";s:14:\"User:test user\";s:10:\"5::noredir\";s:1:\"0\";}",
      0,
      wikiDb))
    val expectedEvent = new PageEvent(
      wikiDb = wikiDb,
      oldTitle = "test_user",
      newTitle = "User:test_user",
      newTitlePrefix = "User",
      newTitleWithoutPrefix = "test_user",
      oldNamespace = 0,
      oldNamespaceIsContent = true,
      newNamespace = 1,
      newNamespaceIsContent = false,
      timestamp = TimestampHelpers.makeMediawikiTimestamp("20130202200839").get,
      eventType = eventType,
      causedByUserId = Some(220966L)
    )

    pageMoveParser(testRow) should equal(expectedEvent)

  }

  it should "work for localized namespace non-0 event" in {

    val testRow = Row.fromTuple((eventType,
      "20130202200839",
      220966L,
      "test_user",
      "a:2:{s:9:\"4::target\";s:24:\"Localized User:test user\";s:10:\"5::noredir\";s:1:\"0\";}",
      1,
      wikiDb))
    val expectedEvent = new PageEvent(
      wikiDb = wikiDb,
      oldTitle = "test_user",
      newTitle = "Localized_User:test_user",
      newTitlePrefix = "Localized_User",
      newTitleWithoutPrefix = "test_user",
      oldNamespace = 1,
      oldNamespaceIsContent = false,
      newNamespace = 1,
      newNamespaceIsContent = false,
      timestamp = TimestampHelpers.makeMediawikiTimestamp("20130202200839").get,
      eventType = eventType,
      causedByUserId = Some(220966L)
    )

    pageMoveParser(testRow) should equal(expectedEvent)

  }

  it should "return an event with error if null logTitle" in {

    val testRow = Row.fromTuple((eventType,
      "20130202200839",
      220966L,
      null,
      "a:2:{s:9:\"4::target\";s:24:\"Localized User:test user\";s:10:\"5::noredir\";s:1:\"0\";}",
      1,
      wikiDb))
    val expectedEvent = new PageEvent(
      wikiDb = wikiDb,
      timestamp = TimestampHelpers.makeMediawikiTimestamp("20130202200839").get,
      eventType = eventType,
      oldTitle = "",
      newTitle = "",
      newTitlePrefix = "",
      newTitleWithoutPrefix = "",
      oldNamespace = Integer.MIN_VALUE,
      oldNamespaceIsContent = false,
      newNamespace = Integer.MIN_VALUE,
      newNamespaceIsContent = false,
      parsingErrors = Seq("Could not parse old and new titles from null logTitle or logParams")
    )

    pageMoveParser(testRow) should equal(expectedEvent)

  }

  it should "return an event with error if null logParam" in {

    val testRow = Row.fromTuple((eventType,
      "20130202200839",
      220966L,
      "test_user",
      null,
      1,
      wikiDb))
    val expectedEvent = new PageEvent(
      wikiDb = wikiDb,
      timestamp = TimestampHelpers.makeMediawikiTimestamp("20130202200839").get,
      eventType = eventType,
      oldTitle = "",
      newTitle = "",
      newTitlePrefix = "",
      newTitleWithoutPrefix = "",
      oldNamespace = Integer.MIN_VALUE,
      oldNamespaceIsContent = false,
      newNamespace = Integer.MIN_VALUE,
      newNamespaceIsContent = false,
      parsingErrors = Seq("Could not parse old and new titles from null logTitle or logParams")
    )

    pageMoveParser(testRow) should equal(expectedEvent)

  }

  "makePageEvent" should "make a pageEvent without error from a regular row" in {

    val testRow = Row.fromTuple((
      1L,
      "The_Nightwatch",
      0,
      "20130202200839",
      220966L,
      wikiDb,
      "test"))
    val expectedEvent = new PageEvent(
      wikiDb = wikiDb,
      pageId = Some(1L),
      oldTitle = "The_Nightwatch",
      newTitle = "The_Nightwatch",
      newTitlePrefix = "",
      newTitleWithoutPrefix = "The_Nightwatch",
      oldNamespace = 0,
      oldNamespaceIsContent = true,
      newNamespace = 0,
      newNamespaceIsContent = true,
      timestamp = TimestampHelpers.makeMediawikiTimestamp("20130202200839").get,
      eventType = "test",
      causedByUserId = Some(220966L)
    )

    PageEventBuilder.buildSimplePageEvent(isContentNamespaceMap)(testRow) should equal(expectedEvent)

  }

  it should "make a page event with error if null title" in {

    val testRow = Row.fromTuple((
      1L,
      null,
      0,
      "20130202200839",
      220966L,
      wikiDb,
      "test"))
    val expectedEvent = new PageEvent(
      wikiDb = wikiDb,
      pageId = Some(1L),
      oldTitle = null,
      newTitle = null,
      newTitlePrefix = "",
      newTitleWithoutPrefix = null,
      oldNamespace = 0,
      oldNamespaceIsContent = true,
      newNamespace = 0,
      newNamespaceIsContent = true,
      timestamp = TimestampHelpers.makeMediawikiTimestamp("20130202200839").get,
      eventType = "test",
      causedByUserId = Some(220966L),
      parsingErrors = Seq("Could not get title from null logTitle")
    )

    PageEventBuilder.buildSimplePageEvent(isContentNamespaceMap)(testRow) should equal(expectedEvent)

  }

  it should "make a page event with error if incorrect timestamp" in {

    val testRow = Row.fromTuple((
      1L,
      "The_Nightwatch",
      0,
      "201302022008",
      220966L,
      wikiDb,
      "test"))
    val expectedEvent = new PageEvent(
      wikiDb = wikiDb,
      pageId = Some(1L),
      oldTitle = "The_Nightwatch",
      newTitle = "The_Nightwatch",
      newTitlePrefix = "",
      newTitleWithoutPrefix = "The_Nightwatch",
      oldNamespace = 0,
      oldNamespaceIsContent = true,
      newNamespace = 0,
      newNamespaceIsContent = true,
      timestamp = new Timestamp(0L),
      eventType = "test",
      causedByUserId = Some(220966L),
      parsingErrors = Seq("Could not parse timestamp")
    )

    PageEventBuilder.buildSimplePageEvent(isContentNamespaceMap)(testRow) should equal(expectedEvent)

  }

}
