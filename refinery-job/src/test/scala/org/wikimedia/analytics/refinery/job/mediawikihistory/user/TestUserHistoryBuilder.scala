package org.wikimedia.analytics.refinery.job.mediawikihistory.user

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.BeforeAndAfterEach


class TestUserHistoryBuilder extends FlatSpec with Matchers with BeforeAndAfterEach with DataFrameSuiteBase {

  import java.sql.Timestamp
  import org.wikimedia.analytics.refinery.core.TimestampHelpers
  import org.wikimedia.analytics.refinery.spark.utils.MapAccumulator
  import org.wikimedia.analytics.refinery.job.mediawikihistory.user.TestUserHistoryHelpers._

  // Implicit needed to sort by timestamps
  import org.wikimedia.analytics.refinery.core.TimestampHelpers.orderedTimestamp

  implicit def sumLongs = (a: Long, b: Long) => a + b
  var statsAccumulator = None.asInstanceOf[Option[MapAccumulator[String, Long]]]
  var userHistoryBuilder = null.asInstanceOf[UserHistoryBuilder]

  override def beforeEach(): Unit = {
    statsAccumulator = Some(new MapAccumulator[String, Long])
    statsAccumulator.foreach(statsAcc => spark.sparkContext.register(statsAcc))
    userHistoryBuilder = new UserHistoryBuilder(spark, statsAccumulator)
  }

  /**
   * Helper to execute processSubgraph with the given input
   * and organize the output so that it can be asserted easily.
   */
  def process(e: Iterable[UserEvent], s: Iterable[UserState]): Seq[Seq[UserState]] = {
    userHistoryBuilder.processSubgraph(e, s)._1.groupBy(_.userId).toSeq.sortBy(_._1).map{
      case (userId, userStates) => userStates.sortBy(_.startTimestamp.getOrElse(new Timestamp(0L)))
    }
  }

  "UserHistoryBuilder" should "historify userText of create, altergroups and alterblocks events" in {
    val events = userEventSet()(
      "time  eventType    oldText  newText  oldGroups  newGroups",
      "01    create       New      New      ()         ()", // Non-historical names!
      "02    altergroups  New      New      ()         (sysop)", // Non-historical names!
      "03    rename       Old      New      ()         ()" // Historical names.
    )
    val states = userStateSet()(
      "textH  id  creation  groups",
      "New    1   01        (sysop)"
    )
    val expectedResults = userStateSet(userId = Some(1L), userCreation = Some(new Timestamp(1L)))(
      "start  end   textH text adminAnon  groupsH  groups   eventType",
      "01     02    Old   New  false      ()       (sysop)  create",
      "02     03    Old   New  false      (sysop)  (sysop)  altergroups",
      "03     None  New   New  false      (sysop)  (sysop)  rename"
    )
    process(events, states) should be (Seq(expectedResults))
  }

  it should "flush states with user creation after event timestamp" in {
    val events = userEventSet()(
      "time  eventType    oldGroups  newGroups",
      "01    altergroups  ()         (sysop)"
    )
    val states = userStateSet()(
      "textH  id  creation",
      "User   1   02"
    )
    val expectedResults = userStateSet()(
      "start  end   textH id  creation  groupsH  eventType  adminId  adminText  adminAnon  adminTemp  adminPerm  inferred",
      "02     None  User  1   02        ()       create     None     None       None       None       None       unclosed"
    )
    process(events, states) should be (Seq(expectedResults))
  }

  it should "flush states that are left without create event" in {
    val events = Seq()
    val states = userStateSet()(
      "textH id  creation eventType  adminId  adminText  adminAnon",
      "User  1   01       create     None     None       None"
    )
    val expectedResults = userStateSet()(
      "start  end   textH id  creation  eventType  adminId  adminText  adminAnon  inferred",
      "01     None  User  1   01        create     None     None       None       unclosed"
    )
    process(events, states) should be (Seq(expectedResults))
  }

  it should "flush states that conflict with the upcoming event" in {
    val events = userEventSet()(
      // Note that this event would violate the uniqueness of userTexts
      // in the database given a point in time.
      "time  eventType  oldText  newText",
      "02    rename     User     Other"
    )
    val states = userStateSet()(
      "textH id  creation",
      "User  1   01"
    )
    val expectedResults = userStateSet()(
      "start  end   eventType  textH  id  creation  adminId  adminText  adminAnon  adminTemp  adminPerm  inferred",
      "02     None  create     User   1   02        None     None       None       None       None       conflict"
    )
    process(events, states) should be (Seq(expectedResults))
  }

  it should "update start-timestamp of create event based on registration-timestamp" in {
    val events = userEventSet()(
      "logId  time  eventType  oldText  newText",
      "1      10000    create     Name1    Name1"
    )
    val states = userStateSet()(
      "textH  id  registration creation",
      "Name1  1   1            None"
    )
    val expectedResults = userStateSet()(
      "start  end   creation  registration  textH  text   eventType  logId",
      "1      None  10000     1             Name1  Name1  create     1"
    )
    val actualResults = process(events, states)
    actualResults should be (Seq(expectedResults))
  }

  it should "Set null start-timestamp of create event if no registration/creation/firstEdit timestamp" in {
    val states = userStateSet()(
      "textH  id  registration  firstEdit  creation  eventType",
      "Name1  1   None          None       None      create"
    )
    val expectedResults = userStateSet()(
      "start  end   creation  registration  firstEdit  textH  text   adminAnon  eventType inferred",
      "None   None  None      None          None       Name1  Name1  false      create    unclosed"
    )
    val actualResults = process(Seq.empty, states)
    actualResults should be (Seq(expectedResults))
  }

  it should "update registration-timestamp of create event based on event-timestamp" in {
    val events = userEventSet()(
      "logId  time  eventType  oldText  newText",
      "1      01    create     Name1    Name1"
    )
    val states = userStateSet()(
      "textH  id  creation",
      "Name1  1   None"
    )
    val expectedResults = userStateSet()(
      "start  end   creation  textH  text   adminAnon  eventType  logId",
      "01     None  01        Name1  Name1  false      create     1"
    )
    val actualResults = process(events, states)
    actualResults should be (Seq(expectedResults))
  }

  it should "process rename chain properly" in {
    val events = userEventSet()(
      "logId  time  eventType  oldText  newText",
      "1      01    create     Name3    Name3", // Non-historical names!
      "2      02    rename     Name1    Name2",
      "3      03    rename     Name2    Name3"
    )
    val states = userStateSet()(
      "textH  id  creation",
      "Name3  1   01"
    )
    val expectedResults = userStateSet(userId = Some(1L), userCreation = Some(new Timestamp(1L)))(
      "start  end   textH  text eventType  logId",
      "01     02    Name1  Name3  create   1",
      "02     03    Name2  Name3  rename   2",
      "03     None  Name3  Name3  rename   3"
    )
    val actualResults = process(events, states)
    actualResults should be (Seq(expectedResults))
  }

  it should "process altergroups chain properly" in {
    val events = userEventSet()(
      "time  eventType    oldGroups  newGroups",
      "01    create       ()         ()",
      "02    altergroups  ()         (sysop)",
      "03    altergroups  (sysop)    (sysop,flood)"
    )
    val states = userStateSet()(
      "textH id  creation  groups",
      "User  1   01        (sysop,flood)"
    )
    val expectedResults = userStateSet(
      userText = Some("User"), userId = Some(1L), userCreation = Some(new Timestamp(1L))
    )(
      "start  end   groupsH         groups        eventType",
      "01     02    ()             (sysop,flood)  create",
      "02     03    (sysop)        (sysop,flood)  altergroups",
      "03     None  (sysop,flood)  (sysop,flood)  altergroups"
    )
    process(events, states) should be (Seq(expectedResults))
  }

  it should "process alterblocks chain properly" in {
    val events = userEventSet()(
      "time  eventType    newBlocks           expiration",
      "01    create       ()                  None",
      "02    alterblocks  (nocreate)          indefinite",
      "03    alterblocks  (nocreate,noemail)  indefinite"
    )
    val states = userStateSet()(
      "text  id  creation",
      "User  1   01"    )
    val expectedResults = userStateSet(
      userText = Some("User"), userId = Some(1L), userCreation = Some(new Timestamp(1L))
    )(
      "start  end   blocksH              blocks             expiration       eventType",
      "01     02    ()                  (nocreate,noemail)  None             create",
      "02     03    (nocreate)          (nocreate,noemail)  indefinite       alterblocks",
      "03     None  (nocreate,noemail)  (nocreate,noemail)  indefinite       alterblocks"
    )
    process(events, states) should be (Seq(expectedResults))
  }

  it should "process colliding rename AFTER create when at same time" in {
    val events = userEventSet()(
      "logId  time  eventType  oldText  newText",
      "1      01    create     Name2    Name2",
      "2      02    create     Name2    Name2",
      "3      02    rename     Name2    Name3"
    )
    val states = userStateSet()(
      "textH  id  creation",
      "Name3  1   01",
      "Name2  2   02"
    )
    val expectedResults1 = userStateSet(userId = Some(1L), userCreation = Some(new Timestamp(1L)))(
      "start  end   textH  text eventType  logId",
      "01     02    Name2  Name3  create   1",
      "02     None  Name3  Name3  rename   3"
    )
    val expectedResults2 = userStateSet(userId = Some(2L), userCreation = Some(new Timestamp(2L)))(
      "start  end   textH  text eventType  logId",
      "02     None  Name2  Name2  create   2"
    )
    val actualResults = process(events, states)
    actualResults should be (Seq(expectedResults1, expectedResults2))
  }

  it should "populate states with caused by fields" in {
    val events = userEventSet()(
      "time  eventType    oldText  newText  oldGroups  newGroups  newBlocks   expiration  adminId",
      "01    create       New      New      ()         ()         ()          ()          1", // Non-historical names!
      "02    rename       Old      New      ()         ()         ()          ()          2",
      "03    altergroups  New      New      ()         (sysop)    ()          ()          3",
      "04    alterblocks  New      New      (sysop)    (sysop)    (nocreate)  indefinite  4"
    )
    val states = userStateSet()(
      "textH  id  creation  groups",
      "New   10  01         (sysop)"
    )
    val expectedResults = userStateSet(userId = Some(10L), userCreation = Some(new Timestamp(1L)))(
      "start  end   textH text groupsH   groups   blocksH     blocks     expiration  adminId  eventType",
      "01     02    Old   New    ()       (sysop)  ()          (nocreate)  ()          1        create",
      "02     03    New   New    ()       (sysop)  ()          (nocreate)  ()          2        rename",
      "03     04    New   New    (sysop)  (sysop)  ()          (nocreate)  ()          3        altergroups",
      "04     None  New   New    (sysop)  (sysop)  (nocreate)  (nocreate)  indefinite  4        alterblocks"
    )
    process(events, states) should be (Seq(expectedResults))
  }

  it should "propagate autoCreate, userGroups and userBlocks" in {
    val events = userEventSet()(
      "time  eventType    oldText  newText  oldGroups  newGroups  newBlocks   expiration  autoCreate",
      "01    create       UserA    UserA    ()         ()         ()          None        true",
      "02    altergroups  UserA    UserA    ()         (sysop)    ()          None        false",
      "03    alterblocks  UserA    UserA    ()         ()         (nocreate)  indefinite  false",
      "04    rename       UserA    UserB    ()         ()         ()          None        false",
      "05    rename       UserB    UserC    ()         ()         ()          None        false",
      "06    rename       UserC    UserA    ()         ()         ()          None        false"
    )
    val states = userStateSet()(
      "textH  id  creation groups",
      "UserA  1   01       (sysop)"
    )
    val expectedResults = userStateSet(userId = Some(1L), userCreation = Some(new Timestamp(1L)))(
      "start  end   textH   text groupsH   groups  blocksH      blocks     expiration  autoCreate  eventType",
      "01     02    UserA  UserA  ()       (sysop)  ()          (nocreate)  None        true        create",
      "02     03    UserA  UserA  (sysop)  (sysop)  ()          (nocreate)  None        true        altergroups",
      "03     04    UserA  UserA  (sysop)  (sysop)  (nocreate)  (nocreate)  indefinite  true        alterblocks",
      "04     05    UserB  UserA  (sysop)  (sysop)  (nocreate)  (nocreate)  None        true        rename",
      "05     06    UserC  UserA  (sysop)  (sysop)  (nocreate)  (nocreate)  None        true        rename",
      "06     None  UserA  UserA  (sysop)  (sysop)  (nocreate)  (nocreate)  None        true        rename"
    )
    process(events, states) should be (Seq(expectedResults))
  }

  it should "insert states at each block expiration if not explicit" in {
    // NOTE:
    // This test uses the propagation function which uses the Mediawiki format timestamp parser
    // We therefore need to use mediawiki formatted timestamps for this test (YYYYMMDDHHMMSS).

    val events = userEventSet()(
      "time              eventType    oldGroups  newGroups  newBlocks   expiration",
      "20010101000001    create       ()         ()         ()          None",
      "20010101000002    alterblocks  ()         ()         (nocreate)  20010101000004",   // Blocked until 04.
      "20010101000003    altergroups  ()         (sysop)    ()          None",             // Altergroups in the middle.
      // Here at 04 an explicit unblock is missing.
      "20010101000005    alterblocks  ()         ()         (noemail)   20010101000009",   // Blocked until 09.
      "20010101000006    altergroups  (sysop)    (flood)    ()          None",             // Altergroups in the middle.
      "20010101000007    alterblocks  ()         ()         ()          None",             // Explicit unblock before 09.
      "20010101000008    alterblocks  ()         ()         (nocreate)  20010101000010"    // Blocked until 10.
    )
    val states = userStateSet()(
      "textH id  creation         groups",
      "User  1   20010101000001   (flood)"
    )
    val expectedResults = userStateSet(
      userText = Some("User"), userId = Some(1L), userCreation = TimestampHelpers.makeMediawikiTimestampOption("20010101000001")
    )(
      "    start                end         groupsH  groups  blocksH      blocks  expiration      eventType     adminId  adminText  adminAnon  adminTemp  adminPerm  inferred",
      "20010101000001     20010101000002    ()       (flood)  ()          ()       None            create       1        User       false      false      true       None",
      "20010101000002     20010101000003    ()       (flood)  (nocreate)  ()       20010101000004  alterblocks  1        User       false      false      true       None",
      "20010101000003     20010101000004    (sysop)  (flood)  (nocreate)  ()       None            altergroups  1        User       false      false      true       None",
      "20010101000004     20010101000005    (sysop)  (flood)  ()          ()       None            alterblocks  None     None       None       None       None       unblock", // Inserted.
      "20010101000005     20010101000006    (sysop)  (flood)  (noemail)   ()       20010101000009  alterblocks  1        User       false      false      true       None",
      "20010101000006     20010101000007    (flood)  (flood)  (noemail)   ()       None            altergroups  1        User       false      false      true       None",
      "20010101000007     20010101000008    (flood)  (flood)  ()          ()       None            alterblocks  1        User       false      false      true       None",    // Explicit.
      "20010101000008     20010101000010    (flood)  (flood)  (nocreate)  ()       20010101000010  alterblocks  1        User       false      false      true       None",
      "20010101000010     None              (flood)  (flood)  ()          ()       None            alterblocks  None     None       None       None       None       unblock"  // Inserted.
    )
    process(events, states) should be (Seq(expectedResults))
  }

  it should "process intricate rename chains properly" in {
    val events = userEventSet()(
      "time  eventType  oldText  newText",
      "01    create     UserA    UserA", // Non-historical names! Created with name: UserA (coincidence).
      "02    create     UserC    UserC", // Non-historical names! Created with name: UserB.
      "03    rename     UserA    UserC", // Renames user created at 01.
      "04    create     UserB    UserB", // Non-historical names! Created with name: UserA.
      "05    rename     UserB    UserD", // Renames user created at 02.
      "06    rename     UserA    UserB", // Renames user created at 04.
      "07    rename     UserC    UserA",
      "08    rename     UserD    UserC"
    )
    val states = userStateSet()(
      "textH  id  creation",
      "UserA  1   01", // User created with name: UserA (at 01).
      "UserC  2   02", // User created with name: UserB.
      "UserB  3   04"  // User created with name: UserA (at 04).
    )
    val expectedResultsUser1 = userStateSet(userId = Some(1L), userCreation = Some(new Timestamp(1L)))(
      "start  end   textH  text eventType",
      "01     03    UserA  UserA  create",
      "03     07    UserC  UserA  rename",
      "07     None  UserA  UserA  rename"
    )
    val expectedResultsUser2 = userStateSet(userId = Some(2L), userCreation = Some(new Timestamp(2L)))(
      "start  end   textH   text eventType",
      "02     05    UserB  UserC  create",
      "05     08    UserD  UserC  rename",
      "08     None  UserC  UserC  rename"
    )
    val expectedResultsUser3 = userStateSet(userId = Some(3L), userCreation = Some(new Timestamp(4L)))(
      "start  end   textH   text eventType",
      "04     06    UserA  UserB  create",
      "06     None  UserB  UserB  rename"
    )
    process(events, states) should be (Seq(
      expectedResultsUser1,
      expectedResultsUser2,
      expectedResultsUser3
    ))
  }

  it should "count successes and failures" in {
    val events = userEventSet()(
      "time  eventType  oldText  newText",
      "01    create     UserA    UserA", // Non-historical names! Created with name: UserA (coincidence).
      "02    create     UserC    UserC", // Non-historical names! Created with name: UserB.
      "03    rename     UserA    UserC", // Renames user created at 01.
      "04    create     UserB    UserB", // Non-historical names! Created with name: UserA.
      "05    rename     UserB    UserD", // Renames user created at 02.
      "06    rename     UserA    UserB", // Renames user created at 04.
      "07    rename     UserC    UserA",
      "08    rename     UserD    UserC",
      "10    rename     UserX    UserY" // Unliked event to check for count
    )
    val states = userStateSet()(
      "textH  id  creation",
      "UserA  1   01", // User created with name: UserA (at 01).
      "UserC  2   02", // User created with name: UserB.
      "UserB  3   04"  // User created with name: UserA (at 04).
    )

    process(events, states)
    val stats = statsAccumulator.get.value
    stats.size() should equal(2)
    stats.get("testwiki.userHistory.eventsMatching.OK") should equal(8)
    stats.get("testwiki.userHistory.eventsMatching.KO") should equal(1)
  }
}
