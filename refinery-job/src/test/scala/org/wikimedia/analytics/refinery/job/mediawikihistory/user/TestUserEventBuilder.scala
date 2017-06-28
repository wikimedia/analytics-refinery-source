package org.wikimedia.analytics.refinery.job.mediawikihistory.user

import java.sql.Timestamp

import org.scalatest.{FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.job.mediawikihistory.utils.TimestampFormats

class TestUserEventBuilder extends FlatSpec with Matchers {

  import org.wikimedia.analytics.refinery.job.mediawikihistory.user.UserEventBuilder._
  import org.apache.spark.sql.Row

  "isBotByName" should "correctly identify bots" in {
    Seq(
      "easyBot",
      "easyBot2",
      "easyBot3WithEnd",
      "noncapbot",
      "noncapbot2",
      "noncapbot3withend"
    ).foreach(bot => isBotByName(bot) should equal(true))
  }

  it should "correctly not identify no-bots" in {
    Seq(
      "nonbotname",
      "nonBotnameeither",
      "notEvenTricky",
      null
    ).foreach(bot => isBotByName(bot) should equal(false))
  }

  "getOldAndNewUserNames" should "parse users from php blob" in {
    getOldAndNewUserNames(
        """a:3:{s:10:"4::olduser";s:7:"Claaser";s:10:"5::newuser";s:6:"ClaasA";s:8:"6::edits";i:0;}""",
        "",
        ""
    ) should be ("Claaser", "ClaasA", None)
  }

  it should "parse users from comment" in {
    getOldAndNewUserNames(
        "",
        """Renamed the user "[[User:Yurik|Yurik]]" to "[[User:YurikBot|YurikBot]]" """,
        ""
    ) should be ("Yurik", "YurikBot", None)
  }

  it should "read users from title and params" in {
    getOldAndNewUserNames(
        "New Name", // log_params is not normalized
        "",
        "Old_Name" // log_title is normalized
    ) should be ("Old Name", "New Name", None) // result is not normalized
  }

  it should "return error reading from title if null title " in {
    getOldAndNewUserNames(
      "",
      "",
      null
    ) should be (null, null, Some("Could not get old username from null logTitle or logParams"))
  }

  it should "return error reading from title if null params " in {
    getOldAndNewUserNames(
      null,
      "",
      ""
    ) should be (null, null, Some("Could not get old username from null logTitle or logParams"))
  }

  "getCreationNames" should "parse user from comment" in {
    getCreationNames(
        "Created the user [[User:Fiveless|Fiveless]] [[User talk:Fiveless|Talk]]",
        ""
    ) should be ("Fiveless", "Fiveless", None)
  }

  it should "read user from title" in {
    getCreationNames(
        "",
        "User_Name" // log_title is normalized
    ) should be ("User Name", "User Name", None) // result is not normalized
  }

  it should "return error reading from title if title is null" in {
    getCreationNames(
      "",
      null
    ) should be (null, null, Some("Could not get creation names from null logtitle"))
  }

  "getOldAndNewUserGroups" should "parse groups from php blob" in {
    getOldAndNewUserGroups(
      """a:2:{s:12:"4::oldgroups";a:1:{i:0;s:5:"sysop";}s:12:"5::newgroups";a:2:{i:0;s:5:"sysop";i:1;s:5:"flood";}}""",
      ""
    ) should be (List("sysop"), List("sysop", "flood"), None)
  }

  it should "parse groups from params" in {
    getOldAndNewUserGroups(
      """flood, sysop
sysop""", // note the line break
      ""
    ) should be (List("flood", "sysop"), List("sysop"), None)
  }

  it should "parse groups from comment" in {
    getOldAndNewUserGroups("", "=sysop,flood") should be (
      List(),
      List("sysop", "flood"),
      None
    )
    getOldAndNewUserGroups("", "+sysop +flood") should be (
      List(),
      List("sysop", "flood"),
      None
    )
  }

  it should "fail when params is invalid" in {
    getOldAndNewUserGroups("Invalid params", "") should be (
      List(),
      List(),
      Some("Could not parse groups from: Invalid params")
    )
  }

  it should "fail when comment is invalid" in {
    getOldAndNewUserGroups("", "Invalid comment") should be (
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
      """a:2:{s:11:"5::duration";s:10:"indefinite";s:8:"6::flags";s:8:"nocreate";}""",
      "20160101000000"
    ) should be (List("nocreate"), Some("indefinite"), None)
  }

  it should "parse info from one-liner params" in {
    getNewUserBlocksAndBlockExpiration(
      "24 hours",
      "20160101000000"
    ) should be (List(), Some("20160102000000"), None)
  }

  it should "parse info from two-liner params" in {
    getNewUserBlocksAndBlockExpiration(
      """24 hours
nocreate, noemail""",
      "20160101000000"
    ) should be (List("nocreate", "noemail"), Some("20160102000000"), None)
  }

  it should "fail with multi-liner params" in {
    getNewUserBlocksAndBlockExpiration(
      """invalid
multiline
string""",
      "20160101000000"
    ) should be (List(), None, Some("""Could not parse blocks from: invalid
multiline
string"""))
  }

  it should "parse timestamp yyyy-MM-ddTHH:mm:ss" in {
    getNewUserBlocksAndBlockExpiration(
      """2017-01-01T00:00:00
nocreate""",
      "20160101000000"
    ) should be (List("nocreate"), Some("20170101000000"), None)
  }

  // TODO: Fix timezone execution issue
  it should "parse timestamp yyyy-MM-ddTHH:mm:ssZ" in {
    getNewUserBlocksAndBlockExpiration(
      """2017-01-01T00:00:00Z
nocreate""",
      "20160101000000"
    ) should be (List("nocreate"), Some("20170101000000"), None)
  }

  it should "parse timestamp E, dd MMM yyyy HH:mm:ss GMT" in {
    getNewUserBlocksAndBlockExpiration(
      """Sun, 01 Jan 2017 00:00:00 GMT
nocreate""",
      "20160101000000"
    ) should be (List("nocreate"), Some("20170101000000"), None)
  }

  it should "parse timestamp dd MMM yyyy HH:mm:ss GMT" in {
    getNewUserBlocksAndBlockExpiration(
      """01 Jan 2017 00:00:00 GMT
nocreate""",
      "20160101000000"
    ) should be (List("nocreate"), Some("20170101000000"), None)
  }

  it should "parse timestamp dd MMM yyyy" in {
    getNewUserBlocksAndBlockExpiration(
      """01 Jan 2017
nocreate""",
      "20160101000000"
    ) should be (List("nocreate"), Some("20170101000000"), None)
  }

  it should "parse timestamp yyyyMMddHH:mm" in {
    getNewUserBlocksAndBlockExpiration(
      """21:35
nocreate""",
      "20160101000000"
    ) should be (List("nocreate"), Some("20160101213500"), None)
  }

  it should "fail invalid timestamp" in {
    getNewUserBlocksAndBlockExpiration(
      """Invalid timestamp
nocreate""",
      "20160101000000"
    ) should be (List(), None, Some("""Could not parse blocks from: Invalid timestamp
nocreate"""))
  }

  "isValidLog" should "fail with ip-like user names" in {
    val row = Row(List(), None, None, None, "123.45.67.890")
    isValidLog(row) should be (false)
  }

  it should "fail with mac-like user names" in {
    val row = Row(List(), None, None, None, "AD54:28F3:E44F:070B:AD54:28F3:E44F:070B")
    isValidLog(row) should be (false)
  }

  it should "fail with #number-like user names" in {
    val row = Row(List(), None, None, None, "#1234567890")
    isValidLog(row) should be (false)
  }

  it should "pass with valid user names" in {
    val row = Row(List(), None, None, None, "Valid Username")
    isValidLog(row) should be (true)
  }

  "parseUserLog" should "parse renameuser log" in {
    val row = Row(
      "renameuser",
      "renameuser",
      "20160101000000",
      12345L,
      "OldUserName",
      "Some comment",
      "NewUserName",
      "testwiki"
    )
    buildUserEvent(row) should be (new UserEvent(
      wikiDb = "testwiki",
      timestamp =  TimestampFormats.makeMediawikiTimestamp("20160101000000").get,
      eventType = "rename",
      causedByUserId = Some(12345L),
      oldUserName = "OldUserName",
      newUserName = "NewUserName",
      oldUserGroups = Seq.empty,
      newUserGroups = Seq.empty
    ))
  }

  it should "parse rights log" in {
    val row = Row(
      "rights",
      "rights",
      "20160101000000",
      12345L,
      "UserName",
      "Some comment",
      """sysop
sysop,flood""",
      "testwiki"
    )
    buildUserEvent(row) should be (new UserEvent(
      wikiDb = "testwiki",
      timestamp = TimestampFormats.makeMediawikiTimestamp("20160101000000").get,
      eventType = "altergroups",
      causedByUserId = Some(12345L),
      oldUserName = "UserName",
      newUserName = "UserName",
      oldUserGroups = Seq("sysop"),
      newUserGroups = Seq("sysop", "flood")
    ))
  }

  it should "parse block log" in {
    val row = Row(
      "block",
      "block",
      "20160101000000",
      12345L,
      "UserName",
      "Some comment",
      """indefinite
nocreate,noemail""",
      "testwiki"
    )
    buildUserEvent(row) should be (new UserEvent(
      wikiDb = "testwiki",
      timestamp = TimestampFormats.makeMediawikiTimestamp("20160101000000").get,
      eventType = "alterblocks",
      causedByUserId = Some(12345L),
      oldUserName = "UserName",
      newUserName = "UserName",
      oldUserGroups = Seq.empty,
      newUserGroups = Seq.empty,
      newUserBlocks = Seq("nocreate", "noemail"),
      blockExpiration = Some("indefinite")
    ))
  }

  it should "parse newusers-autocreate log" in {
    val row = Row(
      "newusers",
      "autocreate",
      "20160101000000",
      12345L,
      "UserName",
      "Some comment",
      "Some params",
      "testwiki"
    )
    buildUserEvent(row) should be (new UserEvent(
      wikiDb = "testwiki",
      timestamp = TimestampFormats.makeMediawikiTimestamp("20160101000000").get,
      eventType = "create",
      causedByUserId = Some(12345L),
      oldUserName = "UserName",
      newUserName = "UserName",
      oldUserGroups = Seq.empty,
      newUserGroups = Seq.empty,
      createdBySystem = true
    ))
  }

  it should "parse newusers-self log" in {
    val row = Row(
      "newusers",
      "create",
      "20160101000000",
      12345L,
      "UserName",
      "Some comment",
      "Some params",
      "testwiki"
    )

    buildUserEvent(row) should be (new UserEvent(
      wikiDb = "testwiki",
      timestamp = TimestampFormats.makeMediawikiTimestamp("20160101000000").get,
      eventType = "create",
      causedByUserId = Some(12345L),
      oldUserName = "UserName",
      newUserName = "UserName",
      oldUserGroups = Seq.empty,
      newUserGroups = Seq.empty,
      createdBySelf = true
    ))
  }

  it should "parse newusers-peer log" in {
    val row = Row(
      "newusers",
      "byemail",
      "20160101000000",
      12345L,
      "UserName",
      "Some comment",
      "Some params",
      "testwiki"
    )

    buildUserEvent(row) should be (new UserEvent(
      wikiDb = "testwiki",
      timestamp = TimestampFormats.makeMediawikiTimestamp("20160101000000").get,
      eventType = "create",
      causedByUserId = Some(12345L),
      oldUserName = "UserName",
      newUserName = "UserName",
      oldUserGroups = Seq.empty,
      newUserGroups = Seq.empty,
      createdByPeer = true
    ))
  }

  it should "populate block parsing errors" in {
    val row = Row(
      "block",
      "block",
      "20160101000000",
      12345L,
      "UserName",
      "Some comment",
      "Invalid params",
      "testwiki"
    )
    buildUserEvent(row) should be (new UserEvent(
      wikiDb = "testwiki",
      timestamp = TimestampFormats.makeMediawikiTimestamp("20160101000000").get,
      eventType = "alterblocks",
      causedByUserId = Some(12345L),
      oldUserName = "UserName",
      newUserName = "UserName",
      oldUserGroups = Seq.empty,
      newUserGroups = Seq.empty,
      newUserBlocks = Seq.empty,
      blockExpiration = None,
      parsingErrors = Seq("Could not parse blocks from: Invalid params")
    ))
  }

  it should "populate names errors" in {
    val row = Row(
      "block",
      "block",
      "20160101000000",
      12345L,
      null,
      "Some comment",
      "Invalid params",
      "testwiki"
    )
    buildUserEvent(row) should be (new UserEvent(
      wikiDb = "testwiki",
      timestamp = TimestampFormats.makeMediawikiTimestamp("20160101000000").get,
      eventType = "alterblocks",
      causedByUserId = Some(12345L),
      oldUserName = null,
      newUserName = null,
      oldUserGroups = Seq.empty,
      newUserGroups = Seq.empty,
      newUserBlocks = Seq.empty,
      blockExpiration = None,
      parsingErrors = Seq("Could not parse blocks from: Invalid params",
        "Could not get names from null logtitle")
    ))
  }

  it should "populate timestamp errors" in {
    val row = Row(
      "block",
      "block",
      "20160101000",
      12345L,
      "UserName",
      "Some comment",
      "Invalid params",
      "testwiki"
    )
    buildUserEvent(row) should be (new UserEvent(
      wikiDb = "testwiki",
      timestamp = new Timestamp(0L),
      eventType = "alterblocks",
      causedByUserId = Some(12345L),
      oldUserName = "UserName",
      newUserName = "UserName",
      oldUserGroups = Seq.empty,
      newUserGroups = Seq.empty,
      newUserBlocks = Seq.empty,
      blockExpiration = None,
      parsingErrors = Seq("Could not parse blocks from: Invalid params",
        "Could not parse timestamp")
    ))
  }
}
