package org.wikimedia.analytics.refinery.job.mediawikihistory.page

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{BeforeAndAfterEach, Matchers, FlatSpec}
import org.wikimedia.analytics.refinery.core.TimestampHelpers


class TestPageHistoryBuilder
  extends FlatSpec
  with Matchers
  with BeforeAndAfterEach
  with DataFrameSuiteBase {

  import java.sql.Timestamp

  import org.wikimedia.analytics.refinery.spark.utils.MapAccumulator
  import org.wikimedia.analytics.refinery.job.mediawikihistory.page.TestPageHistoryHelpers._


  /**
   * Note: Big timestamps are needed to test move by id, as move event pageIds
   *       reference created redirect page for events before 2014-09-25
   */

  implicit def sumLongs = (a: Long, b: Long) => a + b

  var statsAccumulator = None.asInstanceOf[Option[MapAccumulator[String, Long]]]
  var pageHistoryBuilder = null.asInstanceOf[PageHistoryBuilder]

  override def beforeEach(): Unit = {
    statsAccumulator = Some(new MapAccumulator[String, Long])
    statsAccumulator.foreach(statsAcc => spark.sparkContext.register(statsAcc))
    pageHistoryBuilder = new PageHistoryBuilder(spark, statsAccumulator)
  }

  /**
   * Helper to execute processSubgraph with the given input
   * and organize the output so that it can be asserted easily.
   */
  def process(e: Iterable[PageEvent], s: Iterable[PageState]): Seq[Seq[PageState]] = {
    pageHistoryBuilder.processSubgraph(e, s)._1
      .groupBy(state => state.pageId.getOrElse(state.pageArtificialId).toString)
      .toSeq.sortBy(g =>
      (g._2.head.startTimestamp.getOrElse(new Timestamp(Long.MinValue)).getTime,
        g._2.head.endTimestamp.getOrElse(new Timestamp(Long.MaxValue)).getTime)).map {
      case (pageId, pageStates) => pageStates.sortBy(state =>
        (state.startTimestamp.getOrElse(new Timestamp(Long.MinValue)).getTime,
          state.endTimestamp.getOrElse(new Timestamp(Long.MaxValue)).getTime))
    }
  }

  "PageHistoryBuilder" should "flush potential state if its page creation is after current event" in {
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP",
      "01    move       Title1    Title2"
    )
    val states = pageStateSet()(
      "titleH   id  creation  firstEdit  eventType",
      "Title2   1   02        02           create"
    )
    val expectedResults = pageStateSet()(
      "start  end   first  titleH   id  creation  firstEdit  eventType inferred",
      "02     None  true   Title2   1   02          02       create    event-before-page-creation"
    )
    process(events, states) should be(Seq(expectedResults))
  }

  it should "flush restored state if its page-creation is after current event" in {
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP",
      "01    move      Title1    Title2",
      "03    restore   Title2    Title2"
    )
    val states = pageStateSet()(
      "titleH   id  creation  firstEdit  eventType",
      "Title2  1   02          02        create"
    )
    val expectedResults = pageStateSet()(
      "start  end   first  titleH   id  creation  firstEdit  eventType inferred",
      "02     03    true   Title2   1   02         02        create     event-before-page-creation",
      "03     None  false  Title2   1   02         02        restore    None"
    )
    process(events, states) should be(Seq(expectedResults))
  }

  it should "correctly propagate page-state byId" in {
    val events = pageEventSet()(
      "time            id  eventType  oldTitle  newTitleWP  adminId",
      "20170101000200  2   move       Title1    Title2      2"
    )
    val states = pageStateSet()(
      "titleH  id  creation        firstEdit       eventType  adminId  inferred",
      "Title2  2   20170101000100  20170101000100  create     1        original-state"

    )
    val expectedResultsBase = pageStateSet(
      pageCreationTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20170101000100"),
      pageFirstEditTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20170101000100")
    )(
      "start           end              first  titleH  title  id  eventType  adminId  inferred",
      "20170101000100  20170101000200   true   Title1  Title2  2  create      1        original-state",
      "20170101000200  None             false  Title2  Title2  2  move        2        None"
    )
    process(events, states) should be(Seq(expectedResultsBase))
  }

  it should "correctly propagate page-state byTitle" in {
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP  adminId",
      "4000  move       Title1    Title2      2"
    )
    val states = pageStateSet()(
      "titleH  id  creation  firstEdit  eventType  adminId  inferred",
      "Title2  2   02        02         create     1        original-state"

    )
    val expectedResultsBase = pageStateSet(
      pageCreationTimestamp = Some(new Timestamp(2L)),
      pageFirstEditTimestamp = Some(new Timestamp(2L))
    )(
      "start  end   first  titleH  title  id  eventType  adminId  inferred",
      "2     4000   true   Title1  Title2  2  create      1        original-state",
      "4000  None   false  Title2  Title2  2  move        2        None"
    )
    process(events, states) should be(Seq(expectedResultsBase))
  }

  it should "flush states that conflict with move events byId without changing timestamp" in {
    val events = pageEventSet()(
      "time            id  eventType  oldTitle  newTitleWP  adminId",
      "20170101000300  2   move       Title1    Title2      2"
    )
    val states = pageStateSet()(
      "titleH  id  creation        firstEdit       eventType",
      "Title1  1   20170101000100  20170101000100  create",
      "Title2  2   20170101000200  20170101000100  create"

    )
    val expectedResultsConflict = pageStateSet(
      pageCreationTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20170101000300"),
      pageFirstEditTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20170101000100"),
      firstState = Some(true)
    )(
      "start           end   titleH  id  eventType  adminId  inferred",
      "20170101000300  None  Title1  1   create       1      move-conflict"
    )
    val expectedResultsBase = pageStateSet(
      pageId = Some(2L),
      pageCreationTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20170101000200"),
      pageFirstEditTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20170101000100")
    )(
      "start           end             first  titleH  title   eventType  adminId",
      "20170101000100  20170101000300  true   Title1  Title2  create      1",
      "20170101000300  None            false  Title2  Title2  move        2"
    )
    process(events, states) should be(Seq(expectedResultsBase, expectedResultsConflict))
  }

  it should "flush states that conflict with move events byTitle without changing timestamp" in {
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP  adminId",
      "4000    move       Title1    Title2      2"
    )
    val states = pageStateSet()(
      "titleH  id  creation  firstEdit  eventType  adminId",
      "Title1  1   01        01         create     1",
      "Title2  2   02        02         create     1"

    )
    val expectedResultsConflict = pageStateSet(
      firstState = Some(true),
      pageCreationTimestamp = Some(new Timestamp(4000L)),
      pageFirstEditTimestamp = Some(new Timestamp(1L))
    )(
      "start  end   titleH  id  creation  eventType  adminId  inferred",
      "4000   None  Title1  1   4000      create      1       move-conflict"
    )
    val expectedResultsBase = pageStateSet(
      pageCreationTimestamp = Some(new Timestamp(2L)),
      pageFirstEditTimestamp = Some(new Timestamp(2L))
    )(
      "start  end   first  titleH  title  id  eventType  adminId  inferred",
      "2     4000   true   Title1  Title2  2  create      1        None",
      "4000  None   false  Title2  Title2  2  move        2        None"
    )
    process(events, states) should be(Seq(expectedResultsBase, expectedResultsConflict))
  }

  it should "flush states that conflict with delete events byTitle without change timestamps" in {
    val events = pageEventSet()(
      "logId  time  eventType  oldTitle  newTitleWP",
      "1      4000    delete     Title     Title"
    )
    val states = pageStateSet()(
      "titleH  id  creation  eventType",
      "Title   1   01        create"
    )
    val processedStates = process(events, states)
    val expectedResultsA = pageStateSet(
      // Inserting random id coming from results.
      pageIdArtificial = processedStates.head.head.pageArtificialId,
      isDeleted = Some(true)
    )(
      "start  end   first  titleH  id    creation  eventType  adminId  adminText  adminAnon  inferred  logId",
      "None   4000  true   Title   None  None      create     None     None       None       delete    1",
      "4000   4000  false  Title   None  None      delete      1       User       false      None      1"
    )
    val expectedResults1 = pageStateSet(
      firstState = Some(true)
    )(
      "start  end   titleH  id  creation  eventType  inferred         logId",
      "4000   None  Title   1   4000        create   delete-conflict  1"
    )
    processedStates should be(Seq(expectedResultsA, expectedResults1))
  }

  it should "flush restore state followed by another restore event byId" in {
    val events = pageEventSet()(
      "time  id  eventType  oldTitle  newTitleWP",
      "02     2   restore     Title1    Title1",
      "04     2   restore     Title1    Title1"

    )
    val states = pageStateSet()(
      "titleH  id  creation  firstEdit  eventType  inferred",
      "Title1  2   01        01         create     original-state"
    )
    val processedStates = process(events, states)

    val expectedResults1 = pageStateSet(
      pageCreationTimestamp = Some(new Timestamp(1L)),
      pageFirstEditTimestamp = Some(new Timestamp(1L))
    )(
      "start  end  first  titleH   id  eventType  inferred",
      "01     02   true   Title1   2   create     original-state",
      "02     04   false  Title1   2   restore    None",
      "04    None  false  Title1   2   restore    None"
    )

    processedStates should be(Seq(expectedResults1))
  }

  it should "flush restore state followed by another restore event byTitle" in {
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP",
      "02    restore     Title1    Title1",
      "04    restore     Title1    Title1"
    )
    val states = pageStateSet()(
      "titleH  id  creation  firstEdit  eventType",
      "Title1  1   01        01         create"
    )
    val processedStates = process(events, states)

    val expectedResults1 = pageStateSet(
      pageId = Some(1L),
      pageCreationTimestamp = Some(new Timestamp(1L)),
      pageFirstEditTimestamp = Some(new Timestamp(1L))
    )(
      "start  end   first  titleH   eventType",
      "01     02    true   Title1   create",
      "02     04    false  Title1   restore",
      "04     None  false  Title1   restore"
    )

    processedStates should be(Seq(expectedResults1))
  }

  it should "ignore move events that do not match any state byTitle nor byId" in {
    val events = pageEventSet()(
      "time            id  eventType  oldTitle  newTitleWP",
      "20170101000300  2    move       Title1    Title2"
    )
    val states = pageStateSet()(
      "titleH  id  creation        firstEdit       eventType",
      "Title3  1   20170101000200  20170101000100  create"
    )
    val expectedResults = pageStateSet(
      firstState = Some(true),
      pageId = Some(1L),
      pageCreationTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20170101000200"),
      pageFirstEditTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20170101000100")
    )(
      "start           end   titleH  eventType",
      "20170101000100  None  Title3  create"
    )
    process(events, states) should be(Seq(expectedResults))
  }

  it should "ignore restore events that do not match any state byTitle nor byId" in {
    val events = pageEventSet()(
      "time  id  eventType  oldTitle  newTitleWP",
      "03     2   restore       Title1    Title2"
    )
    val states = pageStateSet()(
      "titleH  id  creation  firstEdit  eventType",
      "Title3  1   02        01         create"
    )
    val expectedResults = pageStateSet(
      firstState = Some(true),
      pageId = Some(1L),
      pageCreationTimestamp = Some(new Timestamp(2L)),
      pageFirstEditTimestamp = Some(new Timestamp(1L))
    )(
      "start  end   titleH  eventType",
      "01     None  Title3  create"
    )
    process(events, states) should be(Seq(expectedResults))
  }

  it should "join merge event byId " in {
    val events = pageEventSet()(
      "time  id  eventType  oldTitle  newTitleWP",
      "03    2    merge       Title3    Title3"
    )
    val states = pageStateSet()(
      "titleH  id  creation  firstEdit  eventType",
      "Title3  2   02        01         create"
    )
    val expectedResults = pageStateSet(
      pageId = Some(2L),
      title = Some("Title3"),
      titleHistorical = Some("Title3"),
      pageCreationTimestamp = Some(new Timestamp(2L)),
      pageFirstEditTimestamp = Some(new Timestamp(1L))
    )(
      "start  end   first  eventType",
      "01     03    true   create",
      "03     None  false  merge"
    )
    process(events, states) should be(Seq(expectedResults))
  }

  it should "join merge event byTitle " in {
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP",
      "03     merge       Title3    Title3"
    )
    val states = pageStateSet()(
      "titleH  id  creation  firstEdit  eventType",
      "Title3  2   01        02         create"
    )
    val expectedResults = pageStateSet(
      pageId = Some(2L),
      title = Some("Title3"),
      titleHistorical = Some("Title3"),
      pageCreationTimestamp = Some(new Timestamp(1L)),
      pageFirstEditTimestamp = Some(new Timestamp(2L))
    )(
      "start  end   first  eventType",
      "02     03    true   create",
      "03     None  false  merge"
    )
    process(events, states) should be(Seq(expectedResults))
  }

  it should "join move event by id even if new-title title don't match" in {
    val events = pageEventSet()(
      "time              id  eventType  oldTitle  newTitleWP",
      "20150101000002    2    move       Title1    Title2"
    )
    val states = pageStateSet()(
      "titleH  id  creation        firstEdit       eventType",
      "Title3  2   20150101000001  20150101000001  create"
    )
    val expectedResults = pageStateSet(
    pageId = Some(2L),
    pageCreationTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20150101000001"),
    pageFirstEditTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20150101000001")
    )(
      "start           end               first  titleH  title   eventType",
      "20150101000001  20150101000002    true   Title1  Title3  create",
      "20150101000002  None              false  Title3  Title3  move"
    )
    process(events, states) should be(Seq(expectedResults))
  }

  it should "use a baseDeleted state as page-lineage base byId" in {
    val events = pageEventSet()(
      "time              id  eventType  oldTitle  newTitleWP",
      "20170101020000    3    delete       Title2    Title2"
    )
    val states = pageStateSet()(
      "titleH  id   eventType  creation  firstEdit       deleted",
      "Title2  3    create     None      20170101010000  true"
    )
    val expectedResults = pageStateSet(
      pageId = Some(3L),
      title = Some("Title2"),
      titleHistorical = Some("Title2"),
      pageCreationTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20170101010000"),
      pageFirstEditTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20170101010000"),
      isDeleted = Some(true)
    )(
      "start           end               first  eventType",
      "20170101010000  20170101020000    true   create",
      "20170101020000  None              false  delete"
    )
    process(events, states) should be(Seq(expectedResults))
  }

  it should "use a baseDeleted state as page-lineage base byTitle" in {
    val events = pageEventSet()(
      "time              id  eventType  oldTitle  newTitleWP",
      "20150101020000    0    delete       Title2    Title2"
    )
    val states = pageStateSet()(
      "titleH  id   eventType  creation  firstEdit       deleted",
      "Title2  3    create     None      20150101010000  true"
    )
    val expectedResults = pageStateSet(
      pageId = Some(3L),
      title = Some("Title2"),
      titleHistorical = Some("Title2"),
      pageCreationTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20150101010000"),
      pageFirstEditTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20150101010000"),
      isDeleted = Some(true)
    )(
      "start           end             first  eventType",
      "20150101010000  20150101020000  true   create",
      "20150101020000  None            false  delete"
    )
    process(events, states) should be(Seq(expectedResults))
  }

  // This case should never happen in real life as baseDeleted have a pageId by construction
  it should "use a baseDeleted state as page-lineage base byTitle generating a pageArtificialId" in {
    val events = pageEventSet()(
      "time              id  eventType  oldTitle  newTitleWP",
      "20150101020000    0    delete       Title2    Title2"
    )
    val states = pageStateSet()(
      "titleH  id   eventType  creation  firstEdit       deleted",
      "Title2  None    create  None      20150101010000  true"
    )
    val processedStates = process(events, states)
    val expectedResults = pageStateSet(
      pageId = None,
      pageIdArtificial = processedStates.head.head.pageArtificialId,
      title = Some("Title2"),
      titleHistorical = Some("Title2"),
      pageCreationTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20150101010000"),
      pageFirstEditTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20150101010000"),
      isDeleted = Some(true)
    )(
      "start           end             first  eventType",
      "20150101010000  20150101020000  true   create",
      "20150101020000  None            false  delete"
    )
    processedStates should be(Seq(expectedResults))
  }

  it should "discard move event with a non-matching id and a matching title" in {
    val events = pageEventSet()(
      "time              id  eventType  oldTitle  newTitleWP",
      "20150101000002    2    move       Title1    Title2"
    )
    val states = pageStateSet()(
      "titleH  id  creation        firstEdit       eventType  deleted",
      "Title2  3   20150101000002  20150101000001  create     false"
    )
    val expectedResults = pageStateSet(
      firstState = Some(true),
      pageId = Some(3L),
      title = Some("Title2"),
      titleHistorical = Some("Title2"),
      pageCreationTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20150101000002"),
      pageFirstEditTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20150101000001"),
      isDeleted = Some(false)
    )(
      "start           end   eventType",
      "20150101000001  None  create"
    )
    process(events, states) should be(Seq(expectedResults))
  }

  it should "process event chain ending with a state coming from the page table" in {
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP  adminId",
      "02    move       Title1    Title2      10",
      "03    move       Title2    Title1      20",
      "04    move       Title1    Title3      30"
    )
    val states = pageStateSet()(
      "titleH  id  creation  firstEdit  eventType  adminId",
      "Title3  1   01        01         create     40"
    )
    val expectedResults = pageStateSet(
      pageId = Some(1L),
      pageCreationTimestamp = Some(new Timestamp(1L)),
      pageFirstEditTimestamp = Some(new Timestamp(1L))
    )(
      "start  end   first  titleH  title   eventType  adminId",
      "01     02    true   Title1  Title3  create     40",
      "02     03    false  Title2  Title3  move       10",
      "03     04    false  Title1  Title3  move       20",
      "04     None  false  Title3  Title3  move       30"
    )
    process(events, states) should be(Seq(expectedResults))
  }

  it should "process event chain ending with a create-page  event" in {
    val events = pageEventSet()(
      "time  eventType    oldTitle  newTitleWP  adminId",
      "01    create-page  Title1    Title1      50",
      "02    move         Title1    Title2      10",
      "03    move         Title2    Title1      20",
      "04    move         Title1    Title3      30"
    )
    val states = pageStateSet()(
      "titleH  id  creation  firstEdit  eventType  adminId  inferred",
      "Title3  1   01        01         create     40       original-state"
    )
    val expectedResults = pageStateSet(
      pageId = Some(1L),
      pageCreationTimestamp = Some(new Timestamp(1L)),
      pageFirstEditTimestamp = Some(new Timestamp(1L))
    )(
      "start  end   first  titleH  title   eventType    adminId  inferred",
      "01     01    true   Title1  Title3  create       40       original-state",
      "01     02    false  Title1  Title3  create-page  50       None",
      "02     03    false  Title2  Title3  move         10       None",
      "03     04    false  Title1  Title3  move         20       None",
      "04     None  false  Title3  Title3  move         30       None"
    )
    process(events, states) should be(Seq(expectedResults))
  }

  it should "process event chain ending with a state coming from a delete event" in {
    val events = pageEventSet()(
      "logId  time  eventType  oldTitle  newTitleWP  adminId  adminText adminAnon",
      "1      01    move       Title1    Title2      10       u10       false",
      "2      02    move       Title2    Title1      20       u20       false",
      "3      03    move       Title1    Title3      30       u30       false",
      "4      04    delete     Title3    Title3      40       u40       false"
    )
    val states = Seq.empty // No initial states.
    val processedStates = process(events, states)
    val expectedResults = pageStateSet(
      pageId = None,
      pageIdArtificial = processedStates.head.head.pageArtificialId,
      isDeleted = Some(true),
      pageCreationTimestamp = None,
      pageFirstEditTimestamp = None
    )(
      "start  end   first  titleH  title   eventType  adminId  adminText  adminAnon  inferred  logId",
      "None   01    true   Title1  Title3  create     None     None       None         delete    4",
      "01     02    false  Title2  Title3  move       10       u10        false        None      1",
      "02     03    false  Title1  Title3  move       20       u20        false        None      2",
      "03     04    false  Title3  Title3  move       30       u30        false        None      3",
      "04     None  false  Title3  Title3  delete     40       u40        false        None      4"
    )
    processedStates should be(Seq(expectedResults))
  }

  it should "take the namespace into account when joining byTitle" in {
    val events = pageEventSet()(
      "time   eventType  oldNs  newNs  oldTitle  newTitleWP",
      "20000    move       0      0      Title1    Title2",
      "30000    move       1      1      Title1    Title2",
      "40000    move       0      1      Title1    Title1"
    )
    val states = pageStateSet(causedByEventType = Some("create"))(
      "nsH  titleH  id  creation  firstEdit",
      "0    Title2  1   02        01",
      "1    Title2  2   02        01",
      "1    Title1  3   02        01"
    )
    val expectedResults1 = pageStateSet(
      pageId = Some(1L),
      pageCreationTimestamp = Some(new Timestamp(2L)),
      pageFirstEditTimestamp = Some(new Timestamp(1L))
    )(
      "start   end    first  nsH  titleH  title   eventType",
      "1       20000  true    0   Title1  Title2  create",
      "20000   None   false   0   Title2  Title2  move"
    )
    val expectedResults2 = pageStateSet(
      pageId = Some(2L),
      pageCreationTimestamp = Some(new Timestamp(2L)),
      pageFirstEditTimestamp = Some(new Timestamp(1L))
    )(
      "start  end     first  nsH  titleH   title   eventType",
      "1      30000   true   1    Title1   Title2  create",
      "30000  None    false  1    Title2   Title2  move"
    )
    val expectedResults3 = pageStateSet(
      // Note modified startTimestamp and pageCreationTimestamp plus
      // no admin id because of oldTitle conflict with first event.
      pageId = Some(3L),
      pageCreationTimestamp = Some(new Timestamp(20000L)),
      pageFirstEditTimestamp = Some(new Timestamp(1L))
    )(
      "start    end   first  nsH  ns   titleH  eventType  inferred",
      "20000  40000   true    0   1    Title1  create     move-conflict",
      "40000   None   false   1   1    Title1  move       None"
    )
    process(events, states) should be(
      Seq(expectedResults1, expectedResults2, expectedResults3)
    )
  }

  it should "process moves before deletions to handle the move_redir case byTitle" in {
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP",
      "02    move       TitleA    TitleB",
      "03    move       TitleB    TitleA", // Move_redir event.
      "03    delete     TitleA    TitleA" // Deletion belonging of the move_redir.
    )
    val states = pageStateSet()(
      "titleH  id  creation  firstEdit eventType",
      "TitleA  1   01        01        create"
    )
    val processedStates = process(events, states)
    val expectedResultsArtificial = pageStateSet(
      pageId = None,
      pageIdArtificial = processedStates(1).head.pageArtificialId,
      isDeleted = Some(true),
      pageCreationTimestamp = Some(new Timestamp(2L)),
      pageFirstEditTimestamp = None
    )(
      "start  end   first  titleH  eventType  adminId  adminText  adminAnon  inferred",
      "02     03    true   TitleA  create     None     None       None       move-conflict",
      "03     None  false  TitleA  delete     1        User       false      None"
    )
    val expectedResults1 = pageStateSet(
      pageId = Some(1L),
      pageCreationTimestamp = Some(new Timestamp(1L)),
      pageFirstEditTimestamp = Some(new Timestamp(1L))
    )(
      "start  end   first  titleH  title   eventType",
      "01     02    true   TitleA  TitleA  create",
      "02     03    false  TitleB  TitleA  move",
      "03     None  false  TitleA  TitleA  move"
    )
    processedStates should be(
      Seq(expectedResults1, expectedResultsArtificial)
    )
  }

  it should "solve the NN-B problem (byTitle by definition)" in {
    /*
     * The NN-B problem happens when there's an event that could potentially
     * join with 2 or more states at the same time. Here, as the creation
     * timestamp of p3 is unknown, t2(A->B) could potentially join with
     * either p1 or p3. The algorithm should be able to recognize that and
     * join t2(A->B) with p1 and give p3 an approximated creation timestamp.
     *       ||    t1    |    t2    |    t3    |    t4    ||  States
     * ------||-------------------------------------------||----------
     *   p1  ||    +A    |   A->B   |   B->C   |          ||    C
     *   p3  ||          |          |    +B    |   delB   ||
     * Note: Redirects created as a side effect of the move events which do
     *       not affect the test's results are ignored.
     */
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP",
      "02    move       TitleA    TitleB",
      "03    move       TitleB    TitleC",
      "04    delete     TitleB    TitleB" // Deletion of the redirect created at 03.
    )
    val states = pageStateSet()(
      "titleH  id  creation  firstEdit  eventType",
      "TitleC  1   01        01         create"
    )
    val processedStates = process(events, states)
    val expectedResultsArtificial = pageStateSet(
      pageId = None,
      pageIdArtificial = processedStates(1).head.pageArtificialId,
      isDeleted = Some(true),
      pageCreationTimestamp = Some(new Timestamp(3L)),
      pageFirstEditTimestamp = None
    )(
      "start  end   first  titleH  eventType  adminId  adminText  adminAnon  inferred",
      "03     04    true   TitleB  create     None     None       None       move-conflict",
      "04     None  false  TitleB  delete     1        User       false      None"
    )
    val expectedResults1 = pageStateSet(
      pageId = Some(1L),
      pageCreationTimestamp = Some(new Timestamp(1L)),
      pageFirstEditTimestamp = Some(new Timestamp(1L))
    )(
      "start  end   first  titleH  title  eventType",
      "01     02    true   TitleA  TitleC  create",
      "02     03    false  TitleB  TitleC  move",
      "03     None  false  TitleC  TitleC  move"
    )
    processedStates should be(
      Seq(expectedResults1, expectedResultsArtificial)
    )
  }

  it should "solve the S&C problem (byTitle by definition)" in {
    /*
     * The S&C problem happens when there are 2 events that could potentially
     * join with the same single state at the same time. Here, t2(A->X) and
     * t4(B->X) could potentially join with p4. The algorithm should be able
     * to recognize that and join t24(B->X) with p4, leaving t2(A->X) for p1.
     *       ||    t1    |    t2    |    t3    |    t4    ||  States
     * ------||-------------------------------------------||----------
     *   p1  ||    +A    |   A->X   |   X->C   |          ||    C
     *   p4  ||    +B    |          |          |   B->X   ||    X
     * Note: Redirects created as a side effect of the move events which do
     *       not affect the test's results are ignored.
     * (* = redirect)
     */
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP",
      "02    move       TitleA    TitleX",
      "03    move       TitleX    TitleC",
      "04    move       TitleB    TitleX"
    )
    val states = pageStateSet(causedByEventType = Some("create"))(
      "titleH  id  creation firstEdit",
      "TitleC  1   01       01",
      "TitleX  4   01       01"
    )
    val processedStates = process(events, states)
    val expectedResults1 = pageStateSet(
      pageId = Some(1L),
      pageCreationTimestamp = Some(new Timestamp(1L)),
      pageFirstEditTimestamp = Some(new Timestamp(1L))
    )(
      "start  end   first  titleH  title  eventType",
      "01     02    true   TitleA  TitleC  create",
      "02     03    false  TitleX  TitleC  move",
      "03     None  false  TitleC  TitleC  move"
    )
    val expectedResults4 = pageStateSet(
      pageId = Some(4L),
      pageCreationTimestamp = Some(new Timestamp(1L)),
      pageFirstEditTimestamp = Some(new Timestamp(1L))
    )(
      "start  end   first  titleH  title  eventType",
      "01     04    true   TitleB  TitleX  create",
      "04     None  false  TitleX  TitleX  move"
    )
    processedStates should be(
      Seq(expectedResults1, expectedResults4)
    )
  }

  it should "solve the simple-restore problem byTitle" in {
    /*
     * The simple-restore problem is:
     * Page A is deleted, then restored. Those events happening before 2016-05-06,
     * restore generates a new page, so anew page-lineage is created for the delete.
     *       ||     t1    |    t2    |    t3    ||  States
     * ------||---------------------------------||----------
     *   pU1 ||     +A    |   delA   |          ||    A
     *   p1  ||           |          | +A/restA ||    A
     */
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP",
      "20000    delete     TitleA    TitleA",
      "30000    restore    TitleA    TitleA"
    )
    val states = pageStateSet(causedByEventType = Some("create"))(
      "titleH  id  creation firstEdit",
      "TitleA  1   10000     10000"
    )
    val processedStates = process(events, states)
    val expectedResultsU1 = pageStateSet(
      pageId = None,
      pageIdArtificial = processedStates.head.head.pageArtificialId,
      title = Some("TitleA"),
      titleHistorical = Some("TitleA"),
      isDeleted = Some(true)
    )(
      "start     end    first  eventType  adminId  adminText  adminAnon  inferred",
      "None      20000  true   create      None     None      None       delete",
      "20000     30000  false  delete       1       User      false      None"
    )
    val expectedResults1 = pageStateSet(
      pageId = Some(1L),
      title = Some("TitleA"),
      titleHistorical = Some("TitleA"),
      pageCreationTimestamp = Some(new Timestamp(30000L)),
      pageFirstEditTimestamp = Some(new Timestamp(10000L))
    )(
      "start     end    first  eventType  inferred",
      "30000     30000  true   create     delete-conflict",
      "30000     None   false  restore    None"
    )
    processedStates should be(
      Seq(expectedResultsU1, expectedResults1)
    )
  }

  it should "solve the simple-restore problem byId for recent events" in {
    /*
     * The simple-restore problem is:
     * Page A is deleted, then restored. No fake-id-lineage is created
     * as the restore happens after a delete and timestamp is greater than
     * 2016-05-06
     *       ||     t1    |    t2    |    t3    ||  States
     * ------||---------------------------------||----------
     *   p2  ||     +A    |   delA   |   restA  ||    A
     */
    val events = pageEventSet()(
      "time            id  eventType  oldTitle  newTitleWP",
      "20170101020000  2   delete     TitleA    TitleA",
      "20170101030000  2   restore    TitleA    TitleA"
    )
    val states = pageStateSet(causedByEventType = Some("create"))(
      "titleH  id  creation        firstEdit",
      "TitleA  2   20170101010000  20170101010000"
    )
    val processedStates = process(events, states)
    val expectedResults1 = pageStateSet(
      pageId = Some(2L),
      title = Some("TitleA"),
      titleHistorical = Some("TitleA"),
      pageCreationTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20170101010000"),
      pageFirstEditTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20170101010000")
    )(
      "start           end             first  eventType",
      "20170101010000  20170101020000  true   create",
      "20170101020000  20170101030000  false  delete",
      "20170101030000  None            false  restore"
    )
    processedStates should be(
      Seq(expectedResults1)
    )
  }

  it should "solve the simple-restore problem for old events (by Title even if Id defined, same Ids)" in {
    /*
     * The simple-restore problem is:
     * Page A is deleted, then restored. No fake-id-lineage is created,
     * but a new page is created as the restore happens after a delete but
     * before 2016-05-06 (no page_id recycling before that).
     * Since Ids are defined, there are 2 create events for the same id.
     *       ||     t1    |    t2    |    t3    ||  States
     * ------||---------------------------------||----------
     *   p2  ||     +A    |   delA   | +A/restA ||    A
     */
    val events = pageEventSet()(
      "time            id  eventType  oldTitle  newTitleWP",
      "20150101020000  2   delete     TitleA    TitleA",
      "20150101030000  2   restore    TitleA    TitleA"
    )
    val states = pageStateSet(causedByEventType = Some("create"))(
      "titleH  id  creation        firstEdit",
      "TitleA  2    20150101010000  20150101010000"
    )
    val processedStates = process(events, states)
    val expectedResults1 = pageStateSet(
      pageId = Some(2L),
      title = Some("TitleA"),
      titleHistorical = Some("TitleA"),
      pageCreationTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20150101010000"),
      pageFirstEditTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20150101010000")
    )(
      "start           end             first  eventType  adminId  adminText  adminAnon  inferred",
      "20150101010000  20150101020000  true   create      None     None      None       delete",
      "20150101020000  20150101030000  false  delete       1       User      false      None",
      "20150101030000  20150101030000  false  create       1       User      false      delete-conflict",
      "20150101030000  None            false  restore      1       User      false      None"
    )
    processedStates should be(
      Seq(expectedResults1)
    )
  }

  it should "solve the simple-restore problem for old events (by Title even if Id defined, different Ids)" in {
    /*
     * The simple-restore problem is:
     * Page A is deleted, then restored. No fake-id-lineage is created,
     * but a new page is created as the restore happens after a delete but
     * before 2016-05-06 (no page_id recycling before that)
     *       ||     t1    |    t2    |    t3    ||  States
     * ------||---------------------------------||----------
     *   p1  ||     +A    |   delA   |          ||
     *   p2  ||           |          | +A/restA ||    A
     */
    val events = pageEventSet()(
      "time            id  eventType  oldTitle  newTitleWP",
      "20150101020000  1   delete     TitleA    TitleA",
      "20150101030000  2   restore    TitleA    TitleA"
    )
    val states = pageStateSet(causedByEventType = Some("create"))(
      "titleH  id  creation        firstEdit",
      "TitleA  2   20150101010000  20150101010000"
    )
    val processedStates = process(events, states)
    val expectedResults1 = pageStateSet(
      pageId = Some(1L),
      pageCreationTimestamp = None,
      pageFirstEditTimestamp = None,
      title = Some("TitleA"),
      titleHistorical = Some("TitleA"),
      isDeleted = Some(true)
    )(
      "start           end             first  eventType  adminId  adminText  adminAnon  inferred",
      "None            20150101020000  true   create       None     None     None       delete",
      "20150101020000  20150101030000  false  delete       1        User     false      None"
    )
    val expectedResults2 = pageStateSet(
      pageId = Some(2L),
      pageCreationTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20150101030000"),
      pageFirstEditTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20150101010000"),
      title = Some("TitleA"),
      titleHistorical = Some("TitleA"),
      isDeleted = Some(false)
    )(
      "start           end             first  eventType  inferred",
      "20150101030000  20150101030000  true   create     delete-conflict",
      "20150101030000  None            false  restore    None"
    )
    processedStates should be(
      Seq(expectedResults1, expectedResults2)
    )
  }

  it should "solve the multi-delete problem byTitle" in {
    /*
     * The multi-delete problem happens when a page is deleted
     * multiple times with no restores.
     * TODO - Add File-Namespace special case (multiple restores without create in between)
     * TODO - Refine pageCreationTimestamp using revisions
     *        ||    null  |    t1    |    t2    |    t3    ||  States
     * -------||-------------------------------------------||----------
     *   pU1  ||     +A   |    delA  |          |          ||
     *   pU2  ||          |     +A   |   delA   |          ||
     *   p1   ||          |          |          |   +A     ||    A
     *
     *   This test also checks that when a page_id is defined for a delete-event, it is
     *   used instead of an artificial-page-id
     */
    val events = pageEventSet()(
      "logId  time  eventType  oldTitle  newTitleWP  id",
      "1      20000    delete     TitleA    TitleA  None",
      "2      30000    delete     TitleA    TitleA   100"
    )
    val states = pageStateSet()(
      "titleH  id  eventType  creation firstEdit",
      "TitleA  1   create     10000    10000"
    )
    val processedStates = process(events, states)
    val expectedResultsU1 = pageStateSet(
      pageId = None,
      pageIdArtificial = processedStates.head.head.pageArtificialId,
      title = Some("TitleA"),
      titleHistorical = Some("TitleA"),
      isDeleted = Some(true)
    )(
      "start   end   first  eventType  adminId  adminText  adminAnon  inferred  logId",
      "None   20000  true   create     None     None       None       delete    1",
      "20000  20000  false  delete     1        User       false      None      1"
    )

    val expectedResultsU2 = pageStateSet(
      pageId = Some(100L),
      title = Some("TitleA"),
      titleHistorical = Some("TitleA"),
      isDeleted = Some(true),
      pageCreationTimestamp = Some(new Timestamp(20000L))
    )(
      "start   end   first  eventType  adminId  adminText  adminAnon  inferred        logId",
      "20000  30000  true   create     None     None       None       delete-conflict  1",
      "30000  30000  false  delete     1        User       false      None             2"
    )

    val expectedResults1 = pageStateSet(
      pageId = Some(1L),
      title = Some("TitleA"),
      titleHistorical = Some("TitleA"),
      pageCreationTimestamp = Some(new Timestamp(30000L)),
      pageFirstEditTimestamp = Some(new Timestamp(10000L)),
      firstState = Some(true)
    )(
      "start     end    eventType log  inferred",
      "30000     None   create    2    delete-conflict"
    )
    processedStates should be(
      Seq(expectedResultsU1, expectedResultsU2, expectedResults1)
    )
  }

  it should "solve the multi-delete problem byId (multiple pages) for recent events" in {
    /*
     * The multi-delete problem happens when a page is deleted
     * multiple times with no restores.
     * TODO - Add File-Namespace special case (multiple restores without create in between)
     * TODO - Refine pageCreationTimestamp using revisions
     *        ||    t1    |    t2    |    t3    ||  States
     * -------||--------------------------------||----------
     *   p2   ||    +A    |  delA/+A |  delA    ||
     *   p3   ||          |          |    +A    ||    A
     *
     *   This test also checks that when a page_id is defined for a delete-event, it is
     *   used instead of an artificial-page-id
     */
    val events = pageEventSet()(
      "logId  id  time            eventType  oldTitle  newTitleWP",
      "1      2   20170101020000  delete     TitleA    TitleA",
      "2      2   20170101030000  delete     TitleA    TitleA"
    )
    val states = pageStateSet(causedByEventType = Some("create"))(
      "titleH  id  creation        firstEdit",
      "TitleA  3   20170101010000  20170101010000"
    )
    val processedStates = process(events, states)
    val expectedResultsU1 = pageStateSet(
      pageId = Some(2L),
      title = Some("TitleA"),
      titleHistorical = Some("TitleA"),
      isDeleted = Some(true)
    )(
      "start           end             first  eventType  adminId  adminText  adminAnon  inferred         logId",
      "None            20170101020000  true   create     None     None       None       delete           1",
      "20170101020000  20170101020000  false  delete     1        User       false      None             1",
      "20170101020000  20170101030000  false  create     None     None       None       delete-conflict  1",
      "20170101030000  20170101030000  false  delete     1        User       false      None             2"
    )
    val expectedResultsU2 = pageStateSet(
      pageId = Some(3L),
      title = Some("TitleA"),
      titleHistorical = Some("TitleA"),
      pageCreationTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20170101030000"),
      pageFirstEditTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20170101010000"),
      isDeleted = Some(false),
      firstState = Some(true)
    )(
      "start           end   eventType  inferred         logId",
      "20170101030000  None  create     delete-conflict  2"
    )
    processedStates should be(
      Seq(expectedResultsU1, expectedResultsU2)
    )
  }

  it should "solve the multi-delete problem byId (single page) for recent events" in {
    /*
     * The multi-delete problem happens when a page is deleted
     * multiple times with no restores.
     * TODO - Add File-Namespace special case (multiple restores without create in between)
     * TODO - Refine pageCreationTimestamp using revisions
     *        ||    t1    |    t2    |    t3    ||  States
     * -------||--------------------------------||----------
     *   p2   ||    +A    |  delA/+A |  delA/+A ||    A
     *
     *   This test also checks that when a page_id is defined for a delete-event, it is
     *   used instead of an artificial-page-id
     */
    val events = pageEventSet()(
      "logId  id  time            eventType  oldTitle  newTitleWP",
      "1      2   20170101020000  delete     TitleA    TitleA",
      "2      2   20170101030000  delete     TitleA    TitleA"
    )
    val states = pageStateSet(causedByEventType = Some("create"))(
      "titleH  id  creation        firstEdit",
      "TitleA  2   20170101010000  20170101010000"
    )
    val processedStates = process(events, states)
    val expectedResultsU1 = pageStateSet(
      pageId = Some(2L),
      title = Some("TitleA"),
      titleHistorical = Some("TitleA"),
      pageCreationTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20170101010000"),
      pageFirstEditTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20170101010000"),
      isDeleted = Some(false)
    )(
      "start           end             first  eventType  adminId  adminText  adminAnon  inferred         logId",
      "20170101010000  20170101020000  true   create     None     None       None       delete           1",
      "20170101020000  20170101020000  false  delete     1        User       false      None             1",
      "20170101020000  20170101030000  false  create     None     None       None       delete-conflict  1",
      "20170101030000  20170101030000  false  delete     1        User       false      None             2",
      "20170101030000  None            false  create     1        User       false      delete-conflict  2"
    )
    processedStates should be(
      Seq(expectedResultsU1)
    )
  }

  it should "solve the multi-delete problem byId (single page with id change) for recent events" in {
    /*
     * The multi-delete problem happens when a page is deleted
     * multiple times with no restores.
     * TODO - Add File-Namespace special case (multiple restores without create in between)
     * TODO - Refine pageCreationTimestamp using revisions
     *        ||    t1    |    t2    |    t3    ||  States
     * -------||--------------------------------||----------
     *   p1   ||    +A    |   delA   |          ||
     *   p2   ||          |    +A    |  delA/+A ||    A
     *
     *   This test also checks that when a page_id is defined for a delete-event, it is
     *   used instead of an artificial-page-id
     */
    val events = pageEventSet()(
      "logId  id  time            eventType  oldTitle  newTitleWP",
      "1      1   20170101020000  delete     TitleA    TitleA",
      "2      2   20170101030000  delete     TitleA    TitleA"
    )
    val states = pageStateSet(causedByEventType = Some("create"))(
      "titleH  id  creation        firstEdit",
      "TitleA  2   20170101010000  20170101010000"
    )
    val processedStates = process(events, states)
    val expectedResultsU1 = pageStateSet(
      pageId = Some(1L),
      title = Some("TitleA"),
      titleHistorical = Some("TitleA"),
      pageCreationTimestamp = None,
      pageFirstEditTimestamp = None,
      isDeleted = Some(true)
    )(
      "start           end             first  eventType  adminId  adminText  adminAnon  inferred  logId",
      "None            20170101020000  true   create     None     None       None       delete      1",
      "20170101020000  20170101020000  false  delete     1        User       false      None        1"
    )
    val expectedResultsU2 = pageStateSet(
      pageId = Some(2L),
      title = Some("TitleA"),
      titleHistorical = Some("TitleA"),
      pageCreationTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20170101020000"),
      pageFirstEditTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20170101010000"),
      isDeleted = Some(false)
    )(
      "start           end             first  eventType  adminId  adminText  adminAnon  inferred         logId",
      "20170101020000  20170101030000  true   create     None     None       None       delete-conflict    1",
      "20170101030000  20170101030000  false  delete     1        User       false      None               2",
      "20170101030000  None            false  create     1        User       false      delete-conflict    2"
    )
    processedStates should be(
      Seq(expectedResultsU1, expectedResultsU2)
    )
  }


  it should "solve the multi-delete-and-restore problem byTitle" in {
    /*
     * The multi-delete-and-restore problem happens when a page is deleted multiple times then restored.
     *        ||    null  |    t2    |    t3    |    t4    ||  States
     * -------||-------------------------------------------||----------
     *   pU1  ||     +A   |    delA  |          |          ||
     *   pU2  ||          |     +A   |   delA   |          ||    A
     *   p1   ||          |          |          | +A/restA ||    A
     */
    val events = pageEventSet()(
      "logId  time  eventType  oldTitle  newTitleWP",
      "2      20000    delete     TitleA    TitleA",
      "3      30000    delete     TitleA    TitleA",
      "4      40000    restore    TitleA    TitleA"
    )
    val states = pageStateSet(causedByEventType = Some("create"))(
      "titleH  id  creation firstEdit",
      "TitleA  1   10000     10000"
    )
    val processedStates = process(events, states)
    val expectedResultsU1 = pageStateSet(
      pageId = None,
      title = Some("TitleA"),
      titleHistorical = Some("TitleA"),
      pageIdArtificial = processedStates.head.head.pageArtificialId,
      isDeleted = Some(true)
    )(
      "start   end    first  eventType  adminId  adminText  adminAnon  inferred  logId",
      "None   20000   true   create     None     None       None       delete    2",
      "20000  20000   false  delete     1        User       false      None      2"
    )
    val expectedResultsU2 = pageStateSet(
      pageId = None,
      pageCreationTimestamp = Some(new Timestamp(20000L)),
      title = Some("TitleA"),
      titleHistorical = Some("TitleA"),
      pageIdArtificial = processedStates(1).head.pageArtificialId,
      isDeleted = Some(true)
    )(
      "start   end    first  eventType  adminId  adminText  adminAnon  inferred         logId",
      "20000  30000   true   create     None     None       None       delete-conflict    2",
      "30000  40000   false  delete     1        User       false      None               3"
    )

    // The start-timestamp of create event is set to firstEdit timestamp
    // to artificially get all revisions inside timebound. This case
    // shouldn't happen in real life.
    val expectedResults1 = pageStateSet(
      pageId = Some(1L),
      title = Some("TitleA"),
      titleHistorical = Some("TitleA"),
      pageCreationTimestamp = Some(new Timestamp(40000L)),
      pageFirstEditTimestamp = Some(new Timestamp(10000L))
    )(
      "start     end    first  eventType  adminId  adminText  adminAnon  inferred         logId",
      "40000     40000  true    create     1       User       false      delete-conflict  3",
      "40000     None   false   restore    1       User       false      None             4"
    )
    processedStates should be(
      Seq(expectedResultsU1, expectedResultsU2, expectedResults1)
    )
  }


  it should "solve the restore-over problem byId for recent events" in {
    /*
     * The restore-over problem happens on page_id 27264 in simplewiki between 2006-05-16 2006-05-17.
     * Correct page is moved, with redirects coming into play, then at some
     * point deleted with a false page moved to correct name. After, correct
     * revisions gets restored into currently correctly named page.
     *       ||    null  |    t1    |    t2    |    t3    |    t4    |    t5   |    t6    ||  States
     * ------||------------------------------------------------------|---------|----------||----------
     *   p2  ||     +A   |          |   A->X   |   X->A   |   delA   |         |          ||
     *   p3  ||          |    +X    |          |   (+X*)  |          |   X->A  |   restA  ||    A
     */
    val events = pageEventSet()(
      "logId  id    time            eventType  oldTitle  newTitleWP",
      "2      None  20170101020000  move       TitleA    TitleX",
      "3      2     20170101030000  move       TitleX    TitleA",
      "4      2     20170101040000  delete     TitleA    TitleA",
      "5      3     20170101050000  move       TitleX    TitleA",
      "6      3     20170101060000  restore    TitleA    TitleA"
    )
    val states = pageStateSet(causedByEventType = Some("create"))(
      "titleH  id  creation        firstEdit",
      "TitleA  3   20170101010000  20170101010000"
    )
    val processedStates = process(events, states)
    val expectedResultsX = pageStateSet(
      pageId = Some(2L),
      isDeleted = Some(true)
    )(
      "start           end             first  titleH   title   eventType  adminId  adminText  adminAnon  inferred  logId",
      "None            20170101020000  true   TitleA  TitleA   create     None     None       None       delete    4",
      "20170101020000  20170101030000  false  TitleX  TitleA   move       1        User       false      None      2",
      "20170101030000  20170101040000  false  TitleA  TitleA   move       1        User       false      None      3",
      "20170101040000  None            false  TitleA  TitleA   delete     1        User       false      None      4"
    )

    val expectedResults1 = pageStateSet(
      pageId = Some(3L),
      pageCreationTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20170101030000"),
      pageFirstEditTimestamp = TimestampHelpers.makeMediawikiTimestampOption("20170101010000")
    )(
      "start           end             first  titleH  title   eventType  inferred       logId",
      "20170101030000  20170101050000  true   TitleX  TitleA  create     move-conflict  3",
      "20170101050000  20170101060000  false  TitleA  TitleA  move       None           5",
      "20170101060000  None            false  TitleA  TitleA  restore    None           6"
    )

    processedStates should be(
      Seq(expectedResultsX, expectedResults1)
    )
  }

  it should "solve the restore-over problem byTitle" in {
    /*
     * The restore-over problem happens on page_id 27264 in simplewiki between 2006-05-16 2006-05-17.
     * Correct page is moved, with redirects coming into play, then at some
     * point deleted with a false page moved to correct name. After, correct
     * revisions gets restored into currently correctly named page.
     *       ||    null  |    t1    |    t2    |    t3    |    t4    |    t5   |    t6    ||  States
     * ------||------------------------------------------------------|---------|----------||----------
     *   pU  ||     +A   |          |   A->X   |   X->A   |   delA   |         |          ||
     *   p1  ||          |    +X    |          |   (+X*)  |          |   X->A  |   restA  ||    A
     */
    val events = pageEventSet()(
      "logId  time  eventType  oldTitle  newTitleWP",
      "2      20000    move       TitleA    TitleX",
      "3      30000    move       TitleX    TitleA",
      "4      40000    delete     TitleA    TitleA",
      "5      50000    move       TitleX    TitleA",
      "6      60000    restore    TitleA    TitleA"
    )
    val states = pageStateSet(causedByEventType = Some("create"))(
      "titleH  id  creation firstEdit",
      "TitleA  1   10000     10000"
    )
    val processedStates = process(events, states)
    val expectedResultsX = pageStateSet(
      pageId = None,
      pageIdArtificial = processedStates.head.head.pageArtificialId,
      isDeleted = Some(true)
    )(
      "start     end    first  titleH   title   eventType  adminId  adminText  adminAnon  inferred  logId",
      "None      20000  true   TitleA  TitleA   create     None     None       None       delete    4",
      "20000     30000  false  TitleX  TitleA   move       1        User       false      None      2",
      "30000     40000  false  TitleA  TitleA   move       1        User       false      None      3",
      "40000     None   false  TitleA  TitleA   delete     1        User       false      None      4"
    )

    val expectedResults1 = pageStateSet(
      pageId = Some(1L),
      pageCreationTimestamp = Some(new Timestamp(30000L)),
      pageFirstEditTimestamp = Some(new Timestamp(10000L))
    )(
      "start   end   first  titleH  title   eventType  inferred       logId",
      "30000  50000  true   TitleX  TitleA  create     move-conflict  3",
      "50000  60000  false  TitleA  TitleA  move       None           5",
      "60000  None   false  TitleA  TitleA  restore    None           6"
    )

    processedStates should be(
      Seq(expectedResultsX, expectedResults1)
    )
  }

  it should "solve the restore-merge problem byTitle" in {
    /*
     * The restore-merge problem happens on page_id 12691 in simplewiki between on 2009-08-28 and 2009-09-10.
     * Page1 is moved to page2, deleting it. Then restore is made, merging edits of page2 into page1.
     * Then page1 is moved back to its original title.
     *       ||  null    |    t1    |    t2    |    t3    |    t4   |  States
     * ------||-------------------------------------------|---------|----------
     *   pU  ||    +B    |          |   delB   |          |         |
     *   p1  ||          |    +A    |   A->B   |   restB  |   B->A  |    A
     */
    val events = pageEventSet()(
      "logId  time  eventType  oldTitle  newTitleWP",
      "1      20000    move       TitleA    TitleB",
      "2      20000    delete     TitleB    TitleB",
      "3      30000    restore    TitleB    TitleB",
      "4      40000    move       TitleB    TitleA"
    )
    val states = pageStateSet(causedByEventType = Some("create"))(
      "titleH  id  creation firstEdit",
      "TitleA  1   10000    10000"
    )
    val processedStates = process(events, states)
    val expectedResultsU = pageStateSet(
      pageId = None,
      pageIdArtificial = processedStates.head.head.pageArtificialId,
      isDeleted = Some(true)
    )(
      "start   end   first  titleH  title   eventType  adminId  adminText  adminAnon  inferred  logId",
      "None   20000  true   TitleB  TitleB  create     None     None       None       delete    2",
      "20000  None   false  TitleB  TitleB  delete     1        User       false      None      2"
    )

    val expectedResults1 = pageStateSet(
      pageId = Some(1L),
      pageCreationTimestamp = Some(new Timestamp(10000L)),
      pageFirstEditTimestamp = Some(new Timestamp(10000L))
    )(
      "start     end    first  titleH   title    eventType  logId",
      "10000     20000  true   TitleA   TitleA   create     0",
      "20000     30000  false  TitleB   TitleA   move       1",
      "30000     40000  false  TitleB   TitleA   restore    3",
      "40000     None   false  TitleA   TitleA   move       4"
    )
    processedStates should be(
      Seq(expectedResultsU, expectedResults1)
    )
  }

  it should "solve the multi-restore problem byId" in {
    /*
     * The multi-restore problem happens on page_id 12691, 24119274, 24296206
     * in enwiki, on 2009-08-26 and 2009-09-10. This is a good example of
     * https://en.wikipedia.org/wiki/Wikipedia:Administrators%27_guide/Fixing_cut-and-paste_moves
     * - Correct page is moved to the one which history is be merged inside, deleted the latter.
     * - Restore of the just-deleted-page is made over the new moved one, merging history into new one,
     * - Then page is moved back to correct name, leaving a new redirect for the page whose history
     * has been merged into the correct one.
     * - Later another restore moves some potential revisions leftovers into the redirect.
     * Thing to notice is that, for the period of time when the merged-page was alive
     * (before it's been deleted by the move event), we actually can't reconstruct page_title_historical
     * (nor namespace) with certainty: since histories were merged, we don't know if a revisions belongs
     * to correct-page or to merged-page ...
     *
     * This pattern can happen on multiple pages (as in the real-life example cited above).
     * We implement a test for a single case
     *
     *       ||    t1    |    t2    |    t3    |    t4    |    t5    |    t6    |    t7    ||  States
     * ------||-------------------------------------------|--------- |----------|----------||----------
     *   p2  ||    +A    |          |   A->B   |   restB  |   B->A   |          |          ||    A
     *   p3  ||          |   +B     |   delB   |          |          |          |  restB   ||    B
     *   p4  ||          |          |          |          |   +B*    |   delB   |          ||
     *
     *
     *   NOTE: We Need artificially big timestamp to by-pass move-event special case
     *         (page move_events were referencing the redirected-to page_id before 2014-09-25)
     */

    val events = pageEventSet()(
      "logId  id  time              eventType  oldTitle  newTitleWP",
      "1      2   20170101030000    move       TitleA    TitleB",
      "2      3   20170101030000    delete     TitleB    TitleB",
      "3      2   20170101040000    restore    TitleB    TitleB",
      "4      2   20170101050000    move       TitleB    TitleA",
      "5      4   20170101060000    delete     TitleB    TitleB",
      "6      3   20170101070000    restore    TitleB    TitleB"
    )
    val states = pageStateSet(causedByEventType = Some("create"))(
      "titleH  id  creation          firstEdit",
      "TitleA  2   20170101010000    20170101010000",
      "TitleB  3   20170101020000    20170101020000"
    )
    val processedStates = process(events, states)

    val expectedResults2 = pageStateSet(
      pageId = Some(2L),
      pageCreationTimestamp = Some(TimestampHelpers.makeMediawikiTimestamp("20170101010000")),
      pageFirstEditTimestamp = Some(TimestampHelpers.makeMediawikiTimestamp("20170101010000"))
    )(
      "start              end             first  titleH   title    eventType  logId",
      "20170101010000     20170101030000  true   TitleA   TitleA   create     0",
      "20170101030000     20170101040000  false  TitleB   TitleA   move       1",
      "20170101040000     20170101050000  false  TitleB   TitleA   restore    3",
      "20170101050000     None            false  TitleA   TitleA   move       4"
    )
    val expectedResults3 = pageStateSet(
      pageId = Some(3L),
      pageCreationTimestamp = Some(TimestampHelpers.makeMediawikiTimestamp("20170101020000")),
      pageFirstEditTimestamp = Some(TimestampHelpers.makeMediawikiTimestamp("20170101020000"))
    )(
      "start              end             first  titleH   title    eventType  logId",
      "20170101020000     20170101030000  true   TitleB   TitleB   create     0",
      "20170101030000     20170101070000  false  TitleB   TitleB   delete     2",
      "20170101070000     None            false  TitleB   TitleB   restore    6"
    )
    val expectedResults4 = pageStateSet(
      pageId = Some(4L),
      pageCreationTimestamp = Some(TimestampHelpers.makeMediawikiTimestamp("20170101050000")),
      isDeleted = Some(true)
    )(
      "start           end             first  titleH  title   eventType  adminId  adminText  adminAnon  inferred       logId",
      "20170101050000  20170101060000  true   TitleB  TitleB  create     None     None       None       move-conflict  4",
      "20170101060000  None            false  TitleB  TitleB  delete     1        User       false      None           5"
    )

    processedStates should be(
      Seq(expectedResults2, expectedResults3, expectedResults4)
    )

  }

  it should "solve the multi-restore problem byTitle" in {
    /*
     * In the byTitle multi-restore, the redirect-page B created when moving page B to A
     * after the merge occurs cannot be worked correctly, as the final restore is assumed to
     * have covered for the redirect-deletion
     *
     *       ||    t1    |    t3    |    t4    |    t5    |    t6    ||  States
     * ------||--------------------------------|--------- |----------||----------
     *   p2  ||    +A    |   A->B   |   restB  |   B->A   |          ||    A
     *   pU3 ||          |   delB   |          |          |          ||
     *   p3  ||          |          |          |          | +B/restB ||    B
     *
     */

    val events = pageEventSet(pageId = None)(
      "logId  time  eventType  oldTitle  newTitleWP",
      "1      30000    move       TitleA    TitleB",
      "2      30000    delete     TitleB    TitleB",
      "3      40000    restore    TitleB    TitleB",
      "4      50000    move       TitleB    TitleA",
      "5      60000    restore    TitleB    TitleB"
    )
    val states = pageStateSet(causedByEventType = Some("create"))(
      "titleH  id  creation firstEdit",
      "TitleA  2   10000    10000",
      "TitleB  3   20000    20000"
    )
    val processedStates = process(events, states)

    val expectedResults2 = pageStateSet(
      pageId = Some(2L),
      pageCreationTimestamp = Some(new Timestamp(10000L)),
      pageFirstEditTimestamp = Some(new Timestamp(10000L))
    )(
      "start     end    first  titleH   title    eventType  logId",
      "10000     30000  true   TitleA   TitleA   create     0",
      "30000     40000  false  TitleB   TitleA   move       1",
      "40000     50000  false  TitleB   TitleA   restore    3",
      "50000     None   false  TitleA   TitleA   move       4"
    )
    val expectedResultsU3 = pageStateSet(
      pageId = None,
      pageIdArtificial = processedStates.head.head.pageArtificialId,
      isDeleted = Some(true)
    )(
      "start     end    first  titleH   title    eventType  inferred  adminId  adminText  adminAnon  logId",
      "None      30000  true   TitleB   TitleB   create      delete    None     None      None        2",
      "30000     60000  false  TitleB   TitleB   delete      None       1       User      false       2"
    )
    val expectedResults3 = pageStateSet(
      pageId = Some(3L),
      pageCreationTimestamp = Some(new Timestamp(60000L)),
      pageFirstEditTimestamp = Some(new Timestamp(20000L))
    )(
      "start     end    first  titleH   title    eventType  adminId  adminText adminAnon  inferred         logId",
      "60000     60000  true   TitleB   TitleB   create       1        User    false      delete-conflict  2",
      "60000     None   false  TitleB   TitleB   restore      1        User    false      None             5"
    )

    processedStates should be(
      Seq(expectedResultsU3, expectedResults2, expectedResults3)
    )

  }

  // In this task we show an example of broken page history, where 2 sequential move events
  // would lead to a wrong pageCreation if not for the correction applied in
  // [[PageHistoryBuilder.propagatePageCreationAndFirstEdit]].
  // Note: timestamp coherence is tested here, but history is wrong in any case because of
  //       corrupted sequence of events (experienced in enwiki for log_ids 85702582 and 85702584)
  it should "enforce lineage first state pageCreationTimestamp to equal its startTimestamp for correctness" in {
    val events = pageEventSet(pageId = None)(
      "logId  time             id  eventType  oldTitle  newTitleWP",
      "1      20170101020000    1  move       TitleA    TitleB",
      "2      20170101030000    1  move       TitleA    TitleB"
    )
    val states = pageStateSet()(
      "id  titleH  eventType",
      "1   TitleB  create"
    )
    val expectedResults = pageStateSet(
      pageId = Some(1L),
      pageCreationTimestamp = Some(TimestampHelpers.makeMediawikiTimestamp("20170101020000"))
    )(
      "start           end             first  titleH   title   eventType  adminId  adminText  adminAnon  logId  inferred",
      "20170101020000  20170101030000  true   TitleA   TitleB  create      1       User       false       1     move-conflict",
      "20170101030000  None            false  TitleB   TitleB  move        1       User       false       2     None"
    )
    process(events, states) should be(Seq(expectedResults))
  }

  /*
   * Here is the example:
   *
   *       ||    t1    |    t2    |    t3   ||  States  || deletedStates
   * ------||-------------------------------||----------||---------------
   *   p1  ||          |    +B    |   B->A  ||    A     ||
   *   p2  ||    +B    |   delB   |         ||          ||      B
   */
  it should "work correctly with baseDelete-states in case of conflicting state (broken history) by title" in {
    val events = pageEventSet()(
      "logId  id   time  eventType  oldTitle  newTitleWP",
      "1      None  02    delete     TitleB    TitleB",
      "2      None  03    move       TitleB    TitleA"
    )
    val states = pageStateSet(
      causedByUserId = None,
      causedByUserText = None,
      causedByAnonymousUser = None,
      sourceLogId = None
    )(
      "id  titleH  eventType deleted creation firstEdit inferred",
      " 1  TitleA  create    false   None      None     original-live",
      " 2  TitleB  create    true    01        01       original-delete"
    )
    val expectedResultsA = pageStateSet(
      pageId = Some(1L),
      pageCreationTimestamp = Some(new Timestamp(2L))
    )(
      "start  end   first  eventType  titleH   title   adminId  adminText  adminAnon  logId  inferred",
      "02     03    true   create     TitleB   TitleA   None    None       None        1     delete-conflict",
      "03     None  false  move       TitleA   TitleA    1      User       false       2     None"
    )
    val expectedResultsB = pageStateSet(
      pageId = Some(2L),
      pageCreationTimestamp = Some(new Timestamp(1L)),
      pageFirstEditTimestamp = Some(new Timestamp(1L)),
      title = Some("TitleB"),
      titleHistorical = Some("TitleB"),
      isDeleted = Some(true)
    )(
      "start  end  first  eventType  adminId  adminText  adminAnon  logId  inferred",
      "01     02   true   create      None    None       None        None  original-delete",
      "02     02   false  delete       1      User       false       1     None"
    )
    process(events, states) should be(Seq(expectedResultsB, expectedResultsA))
  }


  // We Need artificially big timestamp to by-pass move-event special case
  // (page move_events were referencing the redirected-to page_id before 2014-09-25)
  it should "count successes and failures" in {
    val events = pageEventSet()(
      "time              id    eventType  oldTitle  newTitleWP  adminId",
      "20170101020000    None  move       Title1    Title2      10",
      "20170101030000    None  move       Title2    Title1      20",
      "20170101040000    None  move       Title1    Title3      30",
      "20170101040000    None  move       TitleX    TitleY      30",
      "20170101020000    2     move       TitleA    TitleB      10",
      "20170101030000    2     move       TitleB    TitleC      10",
      "20170101040000    2     restore    TitleC    TitleC      10",
      "20170101040000    3     restore    TitleD    TitleD      10"
    )
    val states = pageStateSet()(
      "titleH  id  creation         eventType  adminId",
      "Title3  1   20170101010000   create     40",
      "TitleC  2   20170101010000   create     40"
    )

    process(events, states)
    val stats = statsAccumulator.get.value
    stats.size() should equal(4)
    stats.get("testwiki.pageHistory.eventsMatching.OK.byTitle") should equal(3)
    stats.get("testwiki.pageHistory.eventsMatching.OK.byId") should equal(3)
    stats.get("testwiki.pageHistory.eventsMatching.KO.byTitle") should equal(1)
    stats.get("testwiki.pageHistory.eventsMatching.KO.byId") should equal(1)
  }

}
