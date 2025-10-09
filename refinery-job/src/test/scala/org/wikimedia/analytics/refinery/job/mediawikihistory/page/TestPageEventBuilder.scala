package org.wikimedia.analytics.refinery.job.mediawikihistory.page

import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.core.{PhpUnserializer, TimestampHelpers}

class TestPageEventBuilder extends FlatSpec with Matchers {

  val wikiDb = "fakewiki"
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
  val pageEventBuilder = new PageEventBuilder(canonicalNamespaceMap, localizedNamespaceMap, isContentNamespaceMap)


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
    val testLogParam = PhpUnserializer.tryUnserializeMap(
      "a:2:{s:9:\"4::target\";s:23:\"Wolfgang Amadeus Mozart\";s:10:\"5::noredir\";s:1:\"0\";}"
    )

    val expectedOldTitle = "Johannes_Chrysostomus_Wolfgangus_Theophilus_Mozart"
    val expectedNewTitle = "Wolfgang_Amadeus_Mozart"

    pageEventBuilder
      .getOldAndNewTitles(testLogTitle, testLogParam) should equal((expectedOldTitle, expectedNewTitle))
  }

  it should "work correctly with a non php dictionary" in {
    val testLogTitle = "test old title"
    val testLogParam = PhpUnserializer.tryUnserializeMap("test new title")

    val expectedOldTitle = "test_old_title"
    val expectedNewTitle = "test_new_title"

    pageEventBuilder
      .getOldAndNewTitles(testLogTitle, testLogParam) should equal((expectedOldTitle, expectedNewTitle))
  }

  it should "return an event with error in case of null title or params" in {
    an[NullPointerException] should be thrownBy pageEventBuilder
      .getOldAndNewTitles(null, PhpUnserializer.tryUnserializeMap(""))
    an[NullPointerException] should be thrownBy pageEventBuilder
      .getOldAndNewTitles("", null)
  }


  val eventTypeMove = "move"
  val eventActionMove = "move"
  "parsePageLog" should "set no page_id if null" in {

    val testRow = Row.fromTuple((
      eventTypeMove,
      eventActionMove,
      null,
      "20130202200839",
      220966L,
      331077L,
      "fakeUser",
      false,
      null,
      "The_Night_Watch",
      "a:2:{s:9:\"4::target\";s:14:\"The Nightwatch\";s:10:\"5::noredir\";s:1:\"0\";}",
      0,
      wikiDb,
      1L,
      "comment"))
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
      timestamp = TimestampHelpers.makeMediawikiTimestampOption("20130202200839").get,
      eventType = eventTypeMove,
      causedByUserId = Some(220966L),
      causedByUserCentralId = Some(331077L),
      causedByUserText = Some("fakeUser"),
      causedByAnonymousUser = Some(false),
      causedByTemporaryUser = None,
      causedByPermanentUser = None,
      pageId = None,
      sourceLogId = 1L,
      sourceLogComment = "comment",
      sourceLogParams = Map("4::target" -> "The Nightwatch", "5::noredir" -> "0")
    )

    pageEventBuilder.buildMovePageEvent(testRow) should equal(expectedEvent)

  }

  it should "set a page_id if greater than 0" in {

    val testRow = Row.fromTuple((
      eventTypeMove,
      eventActionMove,
      1L,
      "20130202200839",
      220966L,
      331077L,
      "fakeUser",
      false,
      true,
      "The_Night_Watch",
      "a:2:{s:9:\"4::target\";s:14:\"The Nightwatch\";s:10:\"5::noredir\";s:1:\"0\";}",
      0,
      wikiDb,
      1L,
      "comment"))
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
      timestamp = TimestampHelpers.makeMediawikiTimestampOption("20130202200839").get,
      eventType = eventTypeMove,
      causedByUserId = Some(220966L),
      causedByUserCentralId = Some(331077L),
      causedByUserText = Some("fakeUser"),
      causedByAnonymousUser = Some(false),
      causedByTemporaryUser = Some(true),
      causedByPermanentUser = Some(false),
      pageId = Some(1L),
      sourceLogId = 1L,
      sourceLogComment = "comment",
      sourceLogParams = Map("4::target" -> "The Nightwatch", "5::noredir" -> "0")
    )

    pageEventBuilder.buildMovePageEvent(testRow) should equal(expectedEvent)

  }

  it should "set None causedByUserId if null" in {

    val testRow = Row.fromTuple((
      eventTypeMove,
      eventActionMove,
      1L,
      "20130202200839",
      null,
      null,
      "127.0.0.1",
      true,
      null,
      "The_Night_Watch",
      "a:2:{s:9:\"4::target\";s:14:\"The Nightwatch\";s:10:\"5::noredir\";s:1:\"0\";}",
      0,
      wikiDb,
      1L,
      "comment"))
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
      timestamp = TimestampHelpers.makeMediawikiTimestampOption("20130202200839").get,
      eventType = eventTypeMove,
      causedByUserId = None,
      causedByUserCentralId = None,
      causedByUserText = Some("127.0.0.1"),
      causedByAnonymousUser = Some(true),
      causedByTemporaryUser = Some(false),
      causedByPermanentUser = Some(false),
      pageId = Some(1L),
      sourceLogId = 1L,
      sourceLogComment = "comment",
      sourceLogParams = Map("4::target" -> "The Nightwatch", "5::noredir" -> "0")
    )

    pageEventBuilder.buildMovePageEvent(testRow) should equal(expectedEvent)

  }

  it should "work for namespace 0 event" in {

    val testRow = Row.fromTuple((
      eventTypeMove,
      eventActionMove,
      null,
      "20130202200839",
      220966L,
      331077L,
      "fakeUser",
      false,
      false,
      "The_Night_Watch",
      "a:2:{s:9:\"4::target\";s:14:\"The Nightwatch\";s:10:\"5::noredir\";s:1:\"0\";}",
      0,
      wikiDb,
      2L,
      "comment"))
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
      timestamp = TimestampHelpers.makeMediawikiTimestampOption("20130202200839").get,
      eventType = eventTypeMove,
      causedByUserId = Some(220966L),
      causedByUserCentralId = Some(331077L),
      causedByUserText = Some("fakeUser"),
      causedByAnonymousUser = Some(false),
      causedByTemporaryUser = Some(false),
      causedByPermanentUser = Some(true),
      pageId = None,
      sourceLogId = 2L,
      sourceLogComment = "comment",
      sourceLogParams = Map("4::target" -> "The Nightwatch", "5::noredir" -> "0")
    )

    pageEventBuilder.buildMovePageEvent(testRow) should equal(expectedEvent)

  }

  it should "work for canonical namespace non-0 event" in {

    val testRow = Row.fromTuple((
      eventTypeMove,
      eventActionMove,
      1L,
      "20130202200839",
      220966L,
      331077L,
      "test_user",
      false,
      null,
      "test_user",
      "a:2:{s:9:\"4::target\";s:14:\"User:test user\";s:10:\"5::noredir\";s:1:\"0\";}",
      0,
      wikiDb,
      1L,
      "comment"))
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
      timestamp = TimestampHelpers.makeMediawikiTimestampOption("20130202200839").get,
      eventType = eventTypeMove,
      causedByUserId = Some(220966L),
      causedByUserCentralId = Some(331077L),
      causedByUserText = Some("test_user"),
      causedByAnonymousUser = Some(false),
      causedByTemporaryUser = None,
      causedByPermanentUser = None,
      pageId = Some(1L),
      sourceLogId = 1L,
      sourceLogComment = "comment",
      sourceLogParams = Map("4::target" -> "User:test user", "5::noredir" -> "0")
    )

    pageEventBuilder.buildMovePageEvent(testRow) should equal(expectedEvent)

  }

  it should "work for localized namespace non-0 event" in {

    val testRow = Row.fromTuple((
      eventTypeMove,
      eventActionMove,
      null,
      "20130202200839",
      220966L,
      331077L,
      "test_user",
      false,
      null,
      "test_user",
      "a:2:{s:9:\"4::target\";s:24:\"Localized User:test user\";s:10:\"5::noredir\";s:1:\"0\";}",
      1,
      wikiDb,
      3L,
      "comment"))
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
      timestamp = TimestampHelpers.makeMediawikiTimestampOption("20130202200839").get,
      eventType = eventTypeMove,
      causedByUserId = Some(220966L),
      causedByUserCentralId = Some(331077L),
      causedByUserText = Some("test_user"),
      causedByAnonymousUser = Some(false),
      causedByTemporaryUser = None,
      causedByPermanentUser = None,
      pageId = None,
      sourceLogId = 3L,
      sourceLogComment = "comment",
      sourceLogParams = Map("4::target" -> "Localized User:test user", "5::noredir" -> "0")
    )

    pageEventBuilder.buildMovePageEvent(testRow) should equal(expectedEvent)

  }

  it should "return an event with error for non-reference namespace event" in {

    val testRow = Row.fromTuple((
      eventTypeMove,
      eventActionMove,
      0L,
      "20130202200839",
      220966L,
      331077L,
      "test_user",
      false,
      true,
      "test_user",
      "a:2:{s:9:\"4::target\";s:25:\"Wrong namespace:test user\";s:10:\"5::noredir\";s:1:\"0\";}",
      1,
      wikiDb,
      1L,
      "comment"))
    val expectedEvent = new PageEvent(
      wikiDb = wikiDb,
      oldTitle = "test_user",
      newTitle = "Wrong_namespace:test_user",
      newTitlePrefix = "Wrong_namespace",
      newTitleWithoutPrefix = "test_user",
      oldNamespace = 1,
      oldNamespaceIsContent = false,
      newNamespace = Int.MinValue,
      newNamespaceIsContent = false,
      timestamp = TimestampHelpers.makeMediawikiTimestampOption("20130202200839").get,
      eventType = eventTypeMove,
      causedByUserId = Some(220966L),
      causedByUserCentralId = Some(331077L),
      causedByUserText = Some("test_user"),
      causedByAnonymousUser = Some(false),
      causedByTemporaryUser = Some(true),
      causedByPermanentUser = Some(false),
      pageId = Some(0L),
      sourceLogId = 1L,
      sourceLogComment = "comment",
      sourceLogParams = Map("4::target" -> "Wrong namespace:test user", "5::noredir" -> "0"),
      parsingErrors = Seq("Could not find new-namespace value 'Wrong_namespace' in namespace maps")
    )

    pageEventBuilder.buildMovePageEvent(testRow) should equal(expectedEvent)

  }

  it should "return an event with error if null logTitle" in {

    val testRow = Row.fromTuple((
      eventTypeMove,
      eventActionMove,
      1L,
      "20130202200839",
      220966L,
      331077L,
      "fakeUser",
      false,
      false,
      null,
      "a:2:{s:9:\"4::target\";s:24:\"Localized User:test user\";s:10:\"5::noredir\";s:1:\"0\";}",
      1,
      wikiDb,
      1L,
      "comment"))
    val expectedEvent = new PageEvent(
      wikiDb = wikiDb,
      timestamp = TimestampHelpers.makeMediawikiTimestampOption("20130202200839").get,
      eventType = eventTypeMove,
      oldTitle = "",
      newTitle = "",
      newTitlePrefix = "",
      newTitleWithoutPrefix = "",
      oldNamespace = Integer.MIN_VALUE,
      oldNamespaceIsContent = false,
      newNamespace = Integer.MIN_VALUE,
      newNamespaceIsContent = false,
      causedByUserId = Some(220966L),
      causedByUserCentralId = Some(331077L),
      causedByUserText = Some("fakeUser"),
      causedByAnonymousUser = Some(false),
      causedByTemporaryUser = Some(false),
      causedByPermanentUser = Some(true),
      pageId = Some(1L),
      sourceLogId = 1L,
      sourceLogComment = "comment",
      sourceLogParams = Map("4::target" -> "Localized User:test user", "5::noredir" -> "0"),
      parsingErrors = Seq("Could not parse old and new titles from null logTitle or logParams")
    )

    pageEventBuilder.buildMovePageEvent(testRow) should equal(expectedEvent)

  }

  it should "return an event with error if null logParam" in {

    val testRow = Row.fromTuple((
      eventTypeMove,
      eventActionMove,
      null,
      "20130202200839",
      220966L,
      331077L,
      "test_user",
      false,
      null,
      "test_user",
      null,
      1,
      wikiDb,
      1L,
      "comment"))
    val expectedEvent = new PageEvent(
      wikiDb = wikiDb,
      timestamp = TimestampHelpers.makeMediawikiTimestampOption("20130202200839").get,
      eventType = eventTypeMove,
      oldTitle = "",
      newTitle = "",
      newTitlePrefix = "",
      newTitleWithoutPrefix = "",
      oldNamespace = Integer.MIN_VALUE,
      oldNamespaceIsContent = false,
      newNamespace = Integer.MIN_VALUE,
      newNamespaceIsContent = false,
      causedByUserId = Some(220966L),
      causedByUserCentralId = Some(331077L),
      causedByUserText = Some("test_user"),
      causedByAnonymousUser = Some(false),
      causedByTemporaryUser = None,
      causedByPermanentUser = None,
      pageId = None,
      sourceLogId = 1L,
      sourceLogComment = "comment",
      sourceLogParams = Map.empty,
      parsingErrors = Seq("Could not parse old and new titles from null logTitle or logParams")
    )

    pageEventBuilder.buildMovePageEvent(testRow) should equal(expectedEvent)

  }

  it should "store the unparsed map parameter in event" in {

    val testRow = Row.fromTuple((
      eventTypeMove,
      eventActionMove,
      null,
      "20130202200839",
      220966L,
      331077L,
      "test_user",
      false,
      null,
      "test_user",
      "new_user",
      1,
      wikiDb,
      1L,
      "no-comment"))
    val expectedEvent = new PageEvent(
      wikiDb = wikiDb,
      timestamp = TimestampHelpers.makeMediawikiTimestampOption("20130202200839").get,
      eventType = eventTypeMove,
      oldTitle = "test_user",
      newTitle = "new_user",
      newTitlePrefix = "",
      newTitleWithoutPrefix = "new_user",
      oldNamespace = 1,
      oldNamespaceIsContent = false,
      newNamespace = 0,
      newNamespaceIsContent = true,
      causedByUserId = Some(220966L),
      causedByUserCentralId = Some(331077L),
      causedByUserText = Some("test_user"),
      causedByAnonymousUser = Some(false),
      causedByTemporaryUser = None,
      causedByPermanentUser = None,
      pageId = None,
      sourceLogId = 1L,
      sourceLogComment = "no-comment",
      sourceLogParams = Map("unparsed" -> "new_user")
    )

    pageEventBuilder.buildMovePageEvent(testRow) should equal(expectedEvent)

  }

  val eventTypeSimple = "simpleType"
  val eventActionSimple = "simpleAction"

  "buildSimplePageEvent" should "make a pageEvent without error from a regular row" in {

    val testRow = Row.fromTuple((
      eventTypeSimple,
      eventActionSimple,
      1L,
      "20130202200839",
      220966L,
      331077L,
      "fakeUser",
      false,
      null,
      "The_Nightwatch",
      null,
      0,
      wikiDb,
      1L,
      "comment"))
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
      timestamp = TimestampHelpers.makeMediawikiTimestampOption("20130202200839").get,
      eventType = eventActionSimple,
      causedByUserId = Some(220966L),
      causedByUserCentralId = Some(331077L),
      causedByUserText = Some("fakeUser"),
      causedByAnonymousUser = Some(false),
      causedByTemporaryUser = None,
      causedByPermanentUser = None,
      sourceLogId = 1L,
      sourceLogComment = "comment",
      sourceLogParams = Map.empty
    )

    pageEventBuilder.buildSimplePageEvent(testRow) should equal(expectedEvent)

  }

  it should "make a pageEvent with delete type when logAction is delete_redir" in {

    val testRow = Row.fromTuple((
      "delete",
      "delete_redir",
      1L,
      "20130202200839",
      220966L,
      331077L,
      "fakeUser",
      false,
      null,
      "The_Nightwatch",
      null,
      0,
      wikiDb,
      1L,
      "comment"))
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
      timestamp = TimestampHelpers.makeMediawikiTimestampOption("20130202200839").get,
      eventType = "delete",
      causedByUserId = Some(220966L),
      causedByUserCentralId = Some(331077L),
      causedByAnonymousUser = Some(false),
      causedByUserText = Some("fakeUser"),
      causedByTemporaryUser = None,
      causedByPermanentUser = None,
      sourceLogId = 1L,
      sourceLogComment = "comment",
      sourceLogParams = Map.empty
    )

    pageEventBuilder.buildSimplePageEvent(testRow) should equal(expectedEvent)

  }

  it should "make a page event with error if null title" in {

    val testRow = Row.fromTuple((
      eventTypeSimple,
      eventActionSimple,
      1L,
      "20130202200839",
      220966L,
      331077L,
      "fakeUser",
      false,
      true,
      null,
      null,
      0,
      wikiDb,
      1L,
      "no-comment"))
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
      timestamp = TimestampHelpers.makeMediawikiTimestampOption("20130202200839").get,
      eventType = eventActionSimple,
      causedByUserId = Some(220966L),
      causedByUserCentralId = Some(331077L),
      causedByUserText = Some("fakeUser"),
      causedByAnonymousUser = Some(false),
      causedByTemporaryUser = Some(true),
      causedByPermanentUser = Some(false),
      sourceLogId = 1L,
      sourceLogComment = "no-comment",
      sourceLogParams = Map.empty,
      parsingErrors = Seq("Could not get title from null logTitle")
    )

    pageEventBuilder.buildSimplePageEvent(testRow) should equal(expectedEvent)

  }

  it should "ignore user id if user text is null" in {

    val testRow = Row.fromTuple((
      eventTypeMove,
      eventActionMove,
      1L,
      "20130202200839",
      220966L,
      331077L,
      null,
      false,
      false,
      null,
      null,
      1,
      wikiDb,
      1L,
      "comment"))
    val expectedEvent = new PageEvent(
      wikiDb = wikiDb,
      timestamp = TimestampHelpers.makeMediawikiTimestampOption("20130202200839").get,
      eventType = eventTypeMove,
      oldTitle = "",
      newTitle = "",
      newTitlePrefix = "",
      newTitleWithoutPrefix = "",
      oldNamespace = Integer.MIN_VALUE,
      oldNamespaceIsContent = false,
      newNamespace = Integer.MIN_VALUE,
      newNamespaceIsContent = false,
      causedByUserId = None,
      causedByUserCentralId = None,
      causedByUserText = None,
      causedByAnonymousUser = Some(false),
      causedByTemporaryUser = Some(false),
      causedByPermanentUser = Some(true),
      pageId = Some(1L),
      sourceLogId = 1L,
      sourceLogComment = "comment",
      sourceLogParams = Map.empty,
      parsingErrors = Seq("Could not parse old and new titles from null logTitle or logParams")
    )

    pageEventBuilder.buildMovePageEvent(testRow) should equal(expectedEvent)

  }

}
