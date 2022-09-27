package org.wikimedia.analytics.refinery.job.mediawikihistory.user

import org.scalatest.{FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.core.TimestampHelpers
import org.wikimedia.analytics.refinery.job.mediawikihistory.user.UserEventBuilder._

class TestUserEventBuilder extends FlatSpec with Matchers {

  import org.apache.spark.sql.Row

  "isBotBy" should "correctly identify name bots" in {
    val emptyGroups = Seq.empty[String]
    Seq(
      "easyBot",
      "easyBot2",
      "easyBot3WithEnd",
      "noncapbot",
      "noncapbot2",
      "noncapbot3withend"
    ).foreach(bot => isBotBy(Option(bot), emptyGroups) should equal(Seq("name")))
  }

  it should "correctly identify no name bots" in {
    val emptyGroups = Seq.empty[String]
    Seq(
      "nonbotname",
      "nonBotnameeither",
      "notEvenTricky",
      null
    ).foreach(nonBot => isBotBy(Option(nonBot), emptyGroups) should equal(Seq.empty[String]))
  }

  it should "correctly identify group-bots" in {
    Seq(
      Seq("bot", "other_group"),
      Seq("bot")
    ).foreach(groups => isBotBy(Some(""), groups) should equal(Seq("group")))
  }

  it should "correctly identify non-group-bots" in {
    Seq(
      Seq("other_group"),
      Seq.empty
    ).foreach(groups => isBotBy(Some(""), groups) should equal(Seq.empty))
  }

  it should "correctly identify bot name and group bots" in {
    val groups = Seq("bot", "other")
    Seq(
      "easyBot",
      "easyBot2",
      "easyBot3WithEnd",
      "noncapbot",
      "noncapbot2",
      "noncapbot3withend"
    ).foreach(bot => isBotBy(Some(bot), groups) should equal(Seq("name", "group")))
  }

  "getOldAndNewUserTexts" should "parse userTexts from php blob" in {
    getOldAndNewUserTexts(
        Left(Map("4::olduser" -> "Claaser", "5::newuser" -> "ClaasA", "6::edits" -> 0)),
        "",
        ""
    ) should be ("Claaser", "ClaasA", None)
  }

  it should "parse userTexts from comment" in {
    getOldAndNewUserTexts(
        Right(""),
        """Renamed the user "[[User:Yurik|Yurik]]" to "[[User:YurikBot|YurikBot]]" """,
        ""
    ) should be ("Yurik", "YurikBot", None)
  }

  it should "read userTexts from title and params" in {
    getOldAndNewUserTexts(
        Right("New Name"), // log_params is not normalized
        "",
        "Old_Name" // log_title is normalized
    ) should be ("Old Name", "New Name", None) // result is not normalized
  }

  it should "return error reading from title if null title " in {
    getOldAndNewUserTexts(
      Right(""),
      "",
      null
    ) should be (null, null, Some("Could not get old userText from null logTitle or logParams"))
  }

  it should "return error reading from title if null params " in {
    getOldAndNewUserTexts(
      Right(null),
      "",
      ""
    ) should be (null, null, Some("Could not get old userText from null logTitle or logParams"))
  }

  "getCreationUserTexts" should "parse userTexts from comment" in {
    getCreationUserTexts(
        "Created the user [[User:Fiveless|Fiveless]] [[User talk:Fiveless|Talk]]",
        ""
    ) should be ("Fiveless", "Fiveless", None)
  }

  it should "read userText from title" in {
    getCreationUserTexts(
        "",
        "User_Name" // log_title is normalized
    ) should be ("User Name", "User Name", None) // result is not normalized
  }

  it should "return error reading from title if title is null" in {
    getCreationUserTexts(
      "",
      null
    ) should be (null, null, Some("Could not get creation userTexts from null logtitle"))
  }

  "getOldAndNewUserGroups" should "parse groups from php blob" in {
    getOldAndNewUserGroups(
      Left(Map("4::oldgroups" -> Map(0 -> "sysop"), "5::newgroups" -> Map(0 ->"sysop", 1 -> "flood"))),
      ""
    ) should be (List("sysop"), List("sysop", "flood"), None)
  }

  it should "parse groups from params" in {
    getOldAndNewUserGroups(
      Right("""flood, sysop
sysop"""), // note the line break
      ""
    ) should be (List("flood", "sysop"), List("sysop"), None)
  }

  it should "parse groups from comment" in {
    getOldAndNewUserGroups(Right(""), "=sysop,flood") should be (
      List(),
      List("sysop", "flood"),
      None
    )
    getOldAndNewUserGroups(Right(""), "+sysop +flood") should be (
      List(),
      List("sysop", "flood"),
      None
    )
  }

  it should "fail when params is invalid" in {
    getOldAndNewUserGroups(Right("Invalid params"), "") should be (
      List(),
      List(),
      Some("Could not parse groups from: Right(Invalid params)")
    )
  }

  it should "fail when comment is invalid" in {
    getOldAndNewUserGroups(Right(""), "Invalid comment") should be (
      List(),
      List(),
      Some("Could not parse groups from: Invalid comment")
    )
  }

  "applyDurationToTimestamp" should "return indefinite when appropriate" in {
    List("indefinite", "infinite", "never", "").foreach(duration =>
      applyDurationToTimestamp("", duration) should be ("indefinite")
    )
  }

  // TODO: Fix timezone execution issue
  it should "apply simple duration string" in {
    // Note that a month is considered to be always 30 days and a year 356 days.
    List(
      ("1 second", "20160101000001"),
      ("2 minute", "20160101000200"),
      ("3 hour", "20160101030000"),
      ("4 day", "20160105000000"),
      ("5 week", "20160205000000"),
      ("6 month", "20160629000000"),
      ("7 year", "20221230000000")
    ).foreach{case (duration, expected) =>
      applyDurationToTimestamp("20160101000000", duration) should be (expected)
      applyDurationToTimestamp("20160101000000", duration + "s") should be (expected)
    }
  }

  // TODO: Fix timezone execution issue
  it should "apply composed duration string" in {
    applyDurationToTimestamp(
      "20160101000000",
      "3 months 10 days 1 hour"
    ) should be ("20160410010000")
  }

  it should "throw exception with invalid duration string" in {
    an [Exception] should be thrownBy applyDurationToTimestamp(
      "20160101000000",
      "2017-01-01T00:00:00"
    )
  }

  "getNewUserBlocksAndBlockExpiration" should "parse info from php blob" in {
    getNewUserBlocksAndBlockExpiration(
      Left(Map("5::duration" -> "indefinite", "6::flags" -> "nocreate")),
      "20160101000000"
    ) should be (List("nocreate"), Some("indefinite"), None)
  }

  it should "parse info from one-liner params" in {
    getNewUserBlocksAndBlockExpiration(
      Right("24 hours"),
      "20160101000000"
    ) should be (List(), Some("20160102000000"), None)
  }

  it should "parse info from two-liner params" in {
    getNewUserBlocksAndBlockExpiration(
      Right("""24 hours
nocreate, noemail"""),
      "20160101000000"
    ) should be (List("nocreate", "noemail"), Some("20160102000000"), None)
  }

  it should "fail with multi-liner params" in {
    getNewUserBlocksAndBlockExpiration(
      Right("""invalid
multiline
string"""),
      "20160101000000"
    ) should be (List(), None, Some("""Could not parse blocks from: invalid
multiline
string"""))
  }

  it should "parse timestamp yyyy-MM-ddTHH:mm:ss" in {
    getNewUserBlocksAndBlockExpiration(
      Right("""2017-01-01T00:00:00
nocreate"""),
      "20160101000000"
    ) should be (List("nocreate"), Some("20170101000000"), None)
  }

  // TODO: Fix timezone execution issue
  it should "parse timestamp yyyy-MM-ddTHH:mm:ssZ" in {
    getNewUserBlocksAndBlockExpiration(
      Right("""2017-01-01T00:00:00Z
nocreate"""),
      "20160101000000"
    ) should be (List("nocreate"), Some("20170101000000"), None)
  }

  it should "parse timestamp E, dd MMM yyyy HH:mm:ss GMT" in {
    getNewUserBlocksAndBlockExpiration(
      Right("""Sun, 01 Jan 2017 00:00:00 GMT
nocreate"""),
      "20160101000000"
    ) should be (List("nocreate"), Some("20170101000000"), None)
  }

  it should "parse timestamp dd MMM yyyy HH:mm:ss GMT" in {
    getNewUserBlocksAndBlockExpiration(
      Right("""01 Jan 2017 00:00:00 GMT
nocreate"""),
      "20160101000000"
    ) should be (List("nocreate"), Some("20170101000000"), None)
  }

  it should "parse timestamp dd MMM yyyy" in {
    getNewUserBlocksAndBlockExpiration(
      Right("""01 Jan 2017
nocreate"""),
      "20160101000000"
    ) should be (List("nocreate"), Some("20170101000000"), None)
  }

  it should "parse timestamp yyyyMMddHH:mm" in {
    getNewUserBlocksAndBlockExpiration(
      Right("""21:35
nocreate"""),
      "20160101000000"
    ) should be (List("nocreate"), Some("20160101213500"), None)
  }

  it should "fail invalid timestamp" in {
    getNewUserBlocksAndBlockExpiration(
      Right("""Invalid timestamp
nocreate"""),
      "20160101000000"
    ) should be (List(), None, Some("""Could not parse blocks from: Right(Invalid timestamp
nocreate)"""))
  }

  "isValidLogTitle" should "fail with ip-like user names" in {
    isValidLogTitle("123.45.67.890") should be (false)
  }

  it should "fail with mac-like user names" in {
    isValidLogTitle("AD54:28F3:E44F:070B:AD54:28F3:E44F:070B") should be (false)
  }

  it should "fail with #number-like user names" in {
    isValidLogTitle("#1234567890") should be (false)
  }

  it should "pass with valid user names" in {
    isValidLogTitle("Valid Username") should be (true)
  }

  "parseUserLog" should "Set no user_id if null" in {
    val row = Row(
      "renameuser",
      "renameuser",
      "20160101000000",
      null,
      "user",
      true,
      "OldUserText",
      "Some comment",
      "NewUserText",
      "testwiki",
      1L
    )
    buildUserEvent(row) should be (new UserEvent(
      wikiDb = "testwiki",
      timestamp =  TimestampHelpers.makeMediawikiTimestampOption("20160101000000").get,
      eventType = "rename",
      causedByUserId = None,
      causedByAnonymousUser = Some(true),
      causedByUserText = Some("user"),
      oldUserText = "OldUserText",
      newUserText = "NewUserText",
      oldUserGroups = Seq.empty,
      newUserGroups = Seq.empty,
      blockExpiration = None,
      sourceLogId = 1L,
      sourceLogComment = "Some comment",
      sourceLogParams = Map("unparsed"-> "NewUserText")
    ))
  }


  "parseUserLog" should "parse renameuser log" in {
    val row = Row(
      "renameuser",
      "renameuser",
      "20160101000000",
      12345L,
      "user",
      false,
      "OldUserText",
      "Some comment",
      "NewUserText",
      "testwiki",
      1L
    )
    buildUserEvent(row) should be (new UserEvent(
      wikiDb = "testwiki",
      timestamp =  TimestampHelpers.makeMediawikiTimestampOption("20160101000000").get,
      eventType = "rename",
      causedByUserId = Some(12345L),
      causedByAnonymousUser = Some(false),
      causedByUserText = Some("user"),
      oldUserText = "OldUserText",
      newUserText = "NewUserText",
      oldUserGroups = Seq.empty,
      newUserGroups = Seq.empty,
      blockExpiration = None,
      sourceLogId = 1L,
      sourceLogComment = "Some comment",
      sourceLogParams = Map("unparsed"-> "NewUserText")
    ))
  }

  it should "parse rights log" in {
    val row = Row(
      "rights",
      "rights",
      "20160101000000",
      null,
      "user",
      true,
      "UserText",
      "Some comment",
      "sysop\nsysop,flood", // mind the \n
      "testwiki",
      1L
    )
    buildUserEvent(row) should be (new UserEvent(
      wikiDb = "testwiki",
      timestamp = TimestampHelpers.makeMediawikiTimestampOption("20160101000000").get,
      eventType = "altergroups",
      causedByUserId = None,
      causedByAnonymousUser = Some(true),
      causedByUserText = Some("user"),
      oldUserText = "UserText",
      newUserText = "UserText",
      oldUserGroups = Seq("sysop"),
      newUserGroups = Seq("sysop", "flood"),
      blockExpiration = None,
      sourceLogId = 1L,
      sourceLogComment = "Some comment",
      sourceLogParams = Map("unparsed"-> "sysop\nsysop,flood")
    ))
  }

  it should "parse block log" in {
    val row = Row(
      "block",
      "block",
      "20160101000000",
      12345L,
      "user",
      false,
      "UserText",
      "Some comment",
      "indefinite\nnocreate,noemail", // mind the \n
      "testwiki",
      1L
    )
    buildUserEvent(row) should be (new UserEvent(
      wikiDb = "testwiki",
      timestamp = TimestampHelpers.makeMediawikiTimestampOption("20160101000000").get,
      eventType = "alterblocks",
      causedByUserId = Some(12345L),
      causedByAnonymousUser = Some(false),
      causedByUserText = Some("user"),
      oldUserText = "UserText",
      newUserText = "UserText",
      oldUserGroups = Seq.empty,
      newUserGroups = Seq.empty,
      newUserBlocks = Seq("nocreate", "noemail"),
      blockExpiration = Some("indefinite"),
      sourceLogId = 1L,
      sourceLogComment = "Some comment",
      sourceLogParams = Map("unparsed"-> "indefinite\nnocreate,noemail")
    ))
  }

  it should "parse newusers-autocreate log" in {
    val row = Row(
      "newusers",
      "autocreate",
      "20160101000000",
      12345L,
      "user",
      false,
      "UserText",
      "Some comment",
      "Some params",
      "testwiki",
      1L
    )
    buildUserEvent(row) should be (new UserEvent(
      wikiDb = "testwiki",
      timestamp = TimestampHelpers.makeMediawikiTimestampOption("20160101000000").get,
      eventType = "create",
      causedByUserId = Some(12345L),
      causedByAnonymousUser = Some(false),
      causedByUserText = Some("user"),
      oldUserText = "UserText",
      newUserText = "UserText",
      oldUserGroups = Seq.empty,
      newUserGroups = Seq.empty,
      createdBySystem = true,
      blockExpiration = None,
      sourceLogId = 1L,
      sourceLogComment = "Some comment",
      sourceLogParams = Map("unparsed"-> "Some params")
    ))
  }

  it should "parse newusers-self log" in {
    val row = Row(
      "newusers",
      "create",
      "20160101000000",
      12345L,
      "user",
      false,
      "UserText",
      "Some comment",
      "Some params",
      "testwiki",
      1L
    )

    buildUserEvent(row) should be (new UserEvent(
      wikiDb = "testwiki",
      timestamp = TimestampHelpers.makeMediawikiTimestampOption("20160101000000").get,
      eventType = "create",
      causedByUserId = Some(12345L),
      causedByAnonymousUser = Some(false),
      causedByUserText = Some("user"),
      oldUserText = "UserText",
      newUserText = "UserText",
      oldUserGroups = Seq.empty,
      newUserGroups = Seq.empty,
      createdBySelf = true,
      blockExpiration = None,
      sourceLogId = 1L,
      sourceLogComment = "Some comment",
      sourceLogParams = Map("unparsed"-> "Some params")
    ))
  }

  it should "parse newusers-peer log" in {
    val row = Row(
      "newusers",
      "byemail",
      "20160101000000",
      12345L,
      "fakeUser",
      false,
      "UserText",
      "Some comment",
      "Some params",
      "testwiki",
      1L
    )

    buildUserEvent(row) should be (new UserEvent(
      wikiDb = "testwiki",
      timestamp = TimestampHelpers.makeMediawikiTimestampOption("20160101000000").get,
      eventType = "create",
      causedByUserId = Some(12345L),
      causedByAnonymousUser = Some(false),
      causedByUserText = Some("fakeUser"),
      oldUserText = "UserText",
      newUserText = "UserText",
      oldUserGroups = Seq.empty,
      newUserGroups = Seq.empty,
      createdByPeer = true,
      blockExpiration = None,
      sourceLogId = 1L,
      sourceLogComment = "Some comment",
      sourceLogParams = Map("unparsed"-> "Some params")
    ))
  }

  it should "populate block parsing errors" in {
    val row = Row(
      "block",
      "block",
      "20160101000000",
      12345L,
      "user",
      false,
      "UserText",
      "Some comment",
      "Invalid params",
      "testwiki",
      2L
    )
    buildUserEvent(row) should be (new UserEvent(
      wikiDb = "testwiki",
      timestamp = TimestampHelpers.makeMediawikiTimestampOption("20160101000000").get,
      eventType = "alterblocks",
      causedByUserId = Some(12345L),
      causedByAnonymousUser = Some(false),
      causedByUserText = Some("user"),
      oldUserText = "UserText",
      newUserText = "UserText",
      oldUserGroups = Seq.empty,
      newUserGroups = Seq.empty,
      newUserBlocks = Seq.empty,
      blockExpiration = None,
      sourceLogId = 2L,
      sourceLogComment = "Some comment",
      sourceLogParams = Map("unparsed"-> "Invalid params"),
      parsingErrors = Seq("Could not parse blocks from: Right(Invalid params)")
    ))
  }

  it should "populate names errors" in {
    val row = Row(
      "block",
      "block",
      "20160101000000",
      12345L,
      "user",
      false,
      null,
      "Some comment",
      "Invalid params",
      "testwiki",
      1L
    )
    buildUserEvent(row) should be (new UserEvent(
      wikiDb = "testwiki",
      timestamp = TimestampHelpers.makeMediawikiTimestampOption("20160101000000").get,
      eventType = "alterblocks",
      causedByUserId = Some(12345L),
      causedByAnonymousUser = Some(false),
      causedByUserText = Some("user"),
      oldUserText = null,
      newUserText = null,
      oldUserGroups = Seq.empty,
      newUserGroups = Seq.empty,
      newUserBlocks = Seq.empty,
      blockExpiration = None,
      sourceLogId = 1L,
      sourceLogComment = "Some comment",
      sourceLogParams = Map("unparsed"-> "Invalid params"),
      parsingErrors = Seq("Could not parse blocks from: Right(Invalid params)",
        "Could not get userTexts from null logtitle")
    ))
  }

  it should "ignore user id if user text is null" in {
    val row = Row(
      "newusers",
      "byemail",
      "20160101000000",
      12345L,
      null,
      false,
      "UserText",
      "Some comment",
      null,
      "testwiki",
      1L
    )

    buildUserEvent(row) should be (new UserEvent(
      wikiDb = "testwiki",
      timestamp = TimestampHelpers.makeMediawikiTimestampOption("20160101000000").get,
      eventType = "create",
      causedByUserId = None,
      causedByAnonymousUser = Some(false),
      causedByUserText = None,
      oldUserText = "UserText",
      newUserText = "UserText",
      oldUserGroups = Seq.empty,
      newUserGroups = Seq.empty,
      createdByPeer = true,
      blockExpiration = None,
      sourceLogId = 1L,
      sourceLogComment = "Some comment",
      sourceLogParams = Map.empty
    ))
  }

}
