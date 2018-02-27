package org.wikimedia.analytics.refinery.job.mediawikihistory.page


import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{BeforeAndAfterEach, Matchers, FlatSpec}


class TestPageHistoryBuilder extends FlatSpec with Matchers with BeforeAndAfterEach with DataFrameSuiteBase {

  import org.apache.spark.sql.SparkSession
  import org.wikimedia.analytics.refinery.job.mediawikihistory.page.TestPageHistoryHelpers._
  import java.sql.Timestamp
  import org.wikimedia.analytics.refinery.job.mediawikihistory.utils.MapAccumulator
  // Implicit needed to sort by timestamps
  //import org.wikimedia.analytics.refinery.job.mediawikihistory.utils.TimestampHelpers.orderedTimestamp

  implicit def sumLongs = (a: Long, b: Long) => a + b
  var statsAccumulator = null.asInstanceOf[MapAccumulator[String, Long]]
  var pageHistoryBuilder = null.asInstanceOf[PageHistoryBuilder]

  override def beforeEach(): Unit = {
    statsAccumulator = new MapAccumulator[String, Long]
    spark.sparkContext.register(statsAccumulator)
    pageHistoryBuilder = new PageHistoryBuilder(
      null.asInstanceOf[SparkSession],
    statsAccumulator
    )
  }

  /**
   * Helper to execute processSubgraph with the given input
   * and organize the output so that it can be asserted easily.
   */
  def process(e: Iterable[PageEvent], s: Iterable[PageState]): Seq[Seq[PageState]] = {
    pageHistoryBuilder.processSubgraph(e, s)._1
      .groupBy(state => state.pageId.getOrElse(state.pageIdArtificial).toString)
      .toSeq.sortBy(g =>
      (g._2.head.startTimestamp.getOrElse(new Timestamp(Long.MinValue)).getTime,
        g._2.head.endTimestamp.getOrElse(new Timestamp(Long.MaxValue)).getTime)).map {
      case (pageId, pageStates) => pageStates.sortBy(state =>
        (state.startTimestamp.getOrElse(new Timestamp(Long.MinValue)).getTime,
          state.endTimestamp.getOrElse(new Timestamp(Long.MaxValue)).getTime))
    }
  }

  "PageHistoryBuilder" should "flush states that are younger than current event" in {
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP",
      "01    move       Title1    Title2"
    )
    val states = pageStateSet()(
      "titleH   id  creation  eventType",
      "Title2  1   02        create"
    )
    val expectedResults = pageStateSet()(
      "start  end   titleH   id  creation  eventType",
      "02     None  Title2  1   02        create"
    )
    process(events, states) should be (Seq(expectedResults))
  }

  it should "flush states that conflict with move events without changing timestamp" in {
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP  adminId",
      "4000    move       Title1    Title2      0"
    )
    val states = pageStateSet()(
      "titleH  id  creation  eventType  adminId",
      "Title1  1   01        create     1"
    )
    val expectedResults = pageStateSet()(
      "start  end   titleH  id  creation  eventType  adminId  inferred",
      "4000   None  Title1  1   4000        create     1     move-conflict"
    )
    process(events, states) should be (Seq(expectedResults))
  }

  it should "flush states that conflict with move events and change timestamp if less than 2 seconds diff" in {
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP  adminId",
      "2000    move       Title1    Title2      0"
    )
    val states = pageStateSet()(
      "titleH  id  creation  eventType  adminId",
      "Title1  1   1000        create    1"
    )
    val expectedResults = pageStateSet()(
      "start  end   titleH   id  creation  eventType  adminId  inferred",
      "1000   None  Title1   1   1000        create     1     move-conflict"
    )
    process(events, states) should be (Seq(expectedResults))
  }

  it should "flush states that conflict with delete events without change timestamps" in {
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP  adminId",
      "4000    delete     Title     Title       0"
    )
    val states = pageStateSet()(
      "titleH  id  creation  eventType  adminId",
      "Title   1   01        create     0"
    )
    val processedStates = process(events, states)
    val expectedResultsA = pageStateSet(
      // Inserting random id coming from results.
      pageIdArtificial = processedStates.head.head.pageIdArtificial
    )(
      "start  end   titleH  id    creation  eventType  adminId  inferred",
      "None   4000  Title   None  None      create     None     delete",
      "4000   4000  Title   None  None      delete     0        None"
    )
    val expectedResults1 = pageStateSet()(
      "start  end   titleH  id  creation  eventType  adminId  inferred",
      "4000   None  Title   1   4000        create     None     delete-conflict"
    )
    processedStates should be (Seq(expectedResultsA, expectedResults1))
  }

  it should "flush states that conflict with delete events updating timestamps if less than 2 seconds" in {
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP  adminId",
      "2000    delete     Title     Title       0"
    )
    val states = pageStateSet()(
      "titleH  id  creation  eventType  adminId",
      "Title   1   1000        create     0"
    )
    val processedStates = process(events, states)
    val expectedResultsA = pageStateSet(
      // Inserting random id coming from results.
      pageIdArtificial = processedStates.head.head.pageIdArtificial
    )(
      "start  end   titleH  id    creation  eventType  adminId  inferred",
      "None   1000  Title   None  None      create     None     delete",
      "1000   1000  Title   None  None      delete     0        None"
    )
    val expectedResults1 = pageStateSet()(
      "start  end   titleH  id  creation  eventType  adminId  inferred",
      "1000   None  Title   1   1000      create     None     delete-conflict"
    )
    processedStates should be (Seq(expectedResultsA, expectedResults1))
  }


  it should "flush states that conflict with restore events" in {
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP  adminId",
      "02    restore     Title1    Title1       0",
      "04    restore     Title1    Title1       0"

    )
    val states = pageStateSet()(
      "titleH   id  creation  eventType  adminId",
      "Title1  1   01        create     0"
    )
    val processedStates = process(events, states)

    val expectedResults1 = pageStateSet(
      // Inserting random id coming from results.
      pageIdArtificial = processedStates.head.head.pageIdArtificial
    )(
      "start  end   titleH   id    creation  eventType  adminId  inferred",
      "02     04    Title1  1       01      restore     0        restore-conflict",
      "04     None  Title1  1       01      restore     0        None"
    )

    processedStates should be (Seq(expectedResults1))
  }

  it should "ignore move events that do not match any state" in {
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP",
      "02    move       Title1    Title2"
    )
    val states = pageStateSet()(
      "titleH  id  creation  eventType",
      "Title3  1   01        create"
    )
    val expectedResults = pageStateSet()(
      "start  end   titleH  id  creation  eventType",
      "01     None  Title3  1   01        create"
    )
    process(events, states) should be (Seq(expectedResults))
  }

  it should "ignore restore events that do not match any state" in {
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP",
      "02    restore       Title1    Title2"
    )
    val states = pageStateSet()(
      "titleH  id  creation  eventType",
      "Title3  1   01        create"
    )
    val expectedResults = pageStateSet()(
      "start  end   titleH  id  creation  eventType",
      "01     None  Title3  1   01        create"
    )
    process(events, states) should be (Seq(expectedResults))
  }

  it should "process event chain ending with a state coming from the page table" in {
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP  adminId",
      "02    move       Title1    Title2      10",
      "03    move       Title2    Title1      20",
      "04    move       Title1    Title3      30"
    )
    val states = pageStateSet()(
      "titleH  id  creation  eventType  adminId",
      "Title3  1   01        create     40"
    )
    val expectedResults = pageStateSet(
      pageId = Some(1L), pageCreationTimestamp = Some(new Timestamp(1L))
    )(
      "start  end   titleH  title   eventType  adminId",
      "01     02    Title1  Title3  create     40",
      "02     03    Title2  Title3  move       10",
      "03     04    Title1  Title3  move       20",
      "04     None  Title3  Title3  move       30"
    )
    process(events, states) should be (Seq(expectedResults))
  }

  it should "process event chain ending with a state coming from a delete event" in {
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP  adminId",
      "01    move       Title1    Title2      10",
      "02    move       Title2    Title1      20",
      "03    move       Title1    Title3      30",
      "04    delete     Title3    Title3      40"
    )
    val states = Seq.empty // No initial states.
    val processedStates = process(events, states)
    val expectedResults = pageStateSet(
      pageId = None,
      pageIdArtificial = processedStates.head.head.pageIdArtificial,
      pageCreationTimestamp = None
    )(
      "start  end   titleH  title   eventType  adminId  inferred",
      "None   01    Title1  Title3  create     None     delete",
      "01     02    Title2  Title3  move       10       None",
      "02     03    Title1  Title3  move       20       None",
      "03     04    Title3  Title3  move       30       None",
      "04     04    Title3  Title3  delete     40       None"
    )
    processedStates should be (Seq(expectedResults))
  }

  it should "take the namespace into account when joining" in {
    val events = pageEventSet()(
      "time   eventType  oldNs  newNs  oldTitle  newTitleWP",
      "20000    move       0      0      Title1    Title2",
      "30000    move       1      1      Title1    Title2",
      "40000    move       0      1      Title1    Title1"
    )
    val states = pageStateSet(causedByEventType = Some("create"))(
      "nsH  titleH  id  creation",
      "0    Title2  1   01",
      "1    Title2  2   01",
      "1    Title1  3   01"
    )
    val expectedResults1 = pageStateSet(
      pageId = Some(1L), pageCreationTimestamp = Some(new Timestamp(1L))
    )(
      "start   end    nsH  titleH  title   eventType",
      "1       20000   0   Title1  Title2  create",
      "20000   None    0   Title2  Title2  move"
    )
    val expectedResults2 = pageStateSet(
      pageId = Some(2L), pageCreationTimestamp = Some(new Timestamp(1L))
    )(
      "start  end     nsH  titleH   title   eventType",
      "1      30000   1    Title1   Title2  create",
      "30000  None    1    Title2   Title2  move"
    )
    val expectedResults3 = pageStateSet(
      // Note modified startTimestamp and pageCreationTimestamp plus
      // no admin id because of oldTitle conflict with first event.
      pageId = Some(3L), pageCreationTimestamp = Some(new Timestamp(20000L))
    )(
      "start    end   nsH  ns   titleH  eventType  inferred",
      "20000  40000    0   1    Title1  create     move-conflict",
      "40000   None    1   1    Title1  move       None"
    )
    process(events, states) should be (
      Seq(expectedResults1, expectedResults2, expectedResults3)
    )
  }

  it should "process moves before deletions to handle the move_redir case" in {
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP",
      "02    move       TitleA    TitleB",
      "03    move       TitleB    TitleA", // Move_redir event.
      "03    delete     TitleA    TitleA" // Deletion belonging of the move_redir.
    )
    val states = pageStateSet()(
      "titleH  id  creation  eventType",
      "TitleA  1   01        create"
    )
    val processedStates = process(events, states)
    val expectedResultsArtificial = pageStateSet(
      pageId = None,
      pageIdArtificial = processedStates(1).head.pageIdArtificial,
      pageCreationTimestamp = Some(new Timestamp(2L))
    )(
      "start  end   titleH  eventType  adminId  inferred",
      "02     03    TitleA  create     None     move-conflict",
      "03     03    TitleA  delete     0        None"
    )
    val expectedResults1 = pageStateSet(
      pageId = Some(1L), pageCreationTimestamp = Some(new Timestamp(1L))
    )(
      "start  end   titleH  title   eventType",
      "01     02    TitleA  TitleA  create",
      "02     03    TitleB  TitleA  move",
      "03     None  TitleA  TitleA  move"
    )
    processedStates should be (
      Seq(expectedResults1, expectedResultsArtificial)
    )
  }

  it should "solve the NN-B problem" in {
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
      "titleH  id  creation  eventType",
      "TitleC  1   01        create"
    )
    val processedStates = process(events, states)
    val expectedResultsArtificial = pageStateSet(
      pageId = None,
      pageIdArtificial = processedStates(1).head.pageIdArtificial,
      pageCreationTimestamp = Some(new Timestamp(3L))
    )(
      "start  end   titleH  eventType  adminId  inferred",
      "03     04    TitleB  create     None     move-conflict",
      "04     04    TitleB  delete     0        None"
    )
    val expectedResults1 = pageStateSet(
      pageId = Some(1L), pageCreationTimestamp = Some(new Timestamp(1L))
    )(
      "start  end   titleH  title  eventType",
      "01     02    TitleA  TitleC  create",
      "02     03    TitleB  TitleC  move",
      "03     None  TitleC  TitleC  move"
    )
    processedStates should be (
      Seq(expectedResults1, expectedResultsArtificial)
    )
  }

  it should "solve the S&C problem"  in {
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
      "titleH  id  creation",
      "TitleC  1   01",
      "TitleX  4   01"
    )
    val processedStates = process(events, states)
    val expectedResults1 = pageStateSet(
      pageId = Some(1L), pageCreationTimestamp = Some(new Timestamp(1L))
    )(
      "start  end   titleH  title  eventType",
      "01     02    TitleA  TitleC  create",
      "02     03    TitleX  TitleC  move",
      "03     None  TitleC  TitleC  move"
    )
    val expectedResults4 = pageStateSet(
      pageId = Some(4L), pageCreationTimestamp = Some(new Timestamp(1L))
    )(
      "start  end   titleH  title  eventType",
      "01     04    TitleB  TitleX  create",
      "04     None  TitleX  TitleX  move"
    )
    processedStates should be (
      Seq(expectedResults1, expectedResults4)
    )
  }

  /**
    * 2017-11-03 -- We are going to simplify restore treatment, event if not
    * as precise in term of history as we could do. Purpose is to deliver on
    * high-level metrics faster even if correctness is not at its highest standard.
    *
    * TODO: Make delete/restore history rebuilding better.
    * We have updated the original restore test to document the new expected behavior,
    * created the template for the multi-restore test (content to be written),
    * and we keep them for later use
    *
    */
  it should "solve the simple-restore problem"  in {
    /*
     * The simple-restore problem is:
     * Page A is deleted, then restored. This involves creating a new page
     * at restore time since restore doesn't happen on an existing page.
     *       ||    null  |    t1    |    t2    |    t3    ||  States
     * ------||-------------------------------------------||----------
     *   pU  ||     +A   |          |   delA   |          ||
     *   p1  ||          |    +A    |          |   restA  ||    A
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
    val expectedResultsX = pageStateSet(
      pageId = None,
      pageIdArtificial = processedStates.head.head.pageIdArtificial
    )(
      "start     end      titleH   title   eventType  adminId  inferred",
      "None      20000    TitleA  TitleA   create      None     delete",
      "20000     20000    TitleA  TitleA   delete       0       None"
    )

    val expectedResults1 = pageStateSet(
      pageId = Some(1L),
      pageCreationTimestamp = Some(new Timestamp(30000L)),
      pageFirstEditTimestamp = Some(new Timestamp(10000L))
    )(
      "start   end     titleH  title   eventType   inferred",
      "10000  30000    TitleA  TitleA  create       restore",
      "30000  None     TitleA  TitleA  restore       None"
    )
    processedStates should be (
      Seq(expectedResultsX, expectedResults1)
    )
  }

  it should "solve the multi-delete problem"  in {
    /*
     * The multi-delete problem happens when a page is deleted multiple times.
     * TODO - Refine pageCreationTimestamp using revisions
     *        ||    null  |    t1    |    t2    |    t3    ||  States
     * -------||-------------------------------------------||----------
     *   pU1  ||     +A   |    delA  |          |          ||
     *   pU2  ||          |     +A   |   delA   |          ||
     *   p1   ||          |          |          |   +A     ||    A
     */
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP",
      "10000    delete     TitleA    TitleA",
      "20000    delete     TitleA    TitleA"
    )
    val states = pageStateSet(causedByEventType = Some("create"))(
      "titleH  id  creation firstEdit",
      "TitleA  1   30000     30000"
    )
    val processedStates = process(events, states)
    val expectedResultsU1 = pageStateSet(
      pageId = None,
      pageIdArtificial = processedStates.head.head.pageIdArtificial
    )(
      "start   end     titleH  title   eventType  adminId  inferred",
      "None   10000    TitleA  TitleA  create     None     delete",
      "10000  10000    TitleA  TitleA  delete     0        None"
    )

    val expectedResultsU2 = pageStateSet(
      pageId = None,
      pageIdArtificial = processedStates(1).head.pageIdArtificial,
      pageCreationTimestamp = Some(new Timestamp(10000L))
    )(
      "start   end     titleH  title   eventType  adminId  inferred",
      "10000  20000    TitleA  TitleA  create     None     delete-conflict",
      "20000  20000    TitleA  TitleA  delete     0        None"
    )

    val expectedResults1 = pageStateSet(
      pageId = Some(1L),
      pageCreationTimestamp = Some(new Timestamp(30000L)),
      pageFirstEditTimestamp = Some(new Timestamp(30000L))
    )(
      "start     end      titleH   title   eventType",
      "30000     None     TitleA  TitleA   create"
    )
    processedStates should be (
      Seq(expectedResultsU1, expectedResultsU2, expectedResults1)
    )
  }

  it should "solve the multi-delete-and-restore problem"  in {
    /*
     * The multi-delete-and-restore problem happens when a page is deleted multiple times then restored.
     * TODO - Refine pageCreationTimestamp using revisions
     *        ||    null  |    t1    |    t2    |    t3    |    t4    ||  States
     * -------||------------------------------------------------------||----------
     *   pU1  ||     +A   |          |    delA  |          |          ||
     *   p1   ||          |    +A    |          |          |   restA  ||    A
     *   pU2  ||          |          |     +A   |   delA   |          ||
     */
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP",
      "20000    delete     TitleA    TitleA",
      "30000    delete     TitleA    TitleA",
      "40000    restore    TitleA    TitleA"
    )
    val states = pageStateSet(causedByEventType = Some("create"))(
      "titleH  id  creation firstEdit",
      "TitleA  1   10000     10000"
    )
    val processedStates = process(events, states)
    val expectedResultsU1 = pageStateSet(
      pageId = None,
      pageIdArtificial = processedStates.head.head.pageIdArtificial
    )(
      "start   end     titleH  title   eventType  adminId  inferred",
      "None   20000    TitleA  TitleA  create     None     delete",
      "20000  20000    TitleA  TitleA  delete     0        None"
    )

    val expectedResultsU2 = pageStateSet(
      pageId = None,
      pageIdArtificial = processedStates(2).head.pageIdArtificial,
      pageCreationTimestamp = Some(new Timestamp(20000L))
    )(
      "start   end     titleH  title   eventType  adminId  inferred",
      "20000  30000    TitleA  TitleA  create     None     delete-conflict",
      "30000  30000    TitleA  TitleA  delete     0        None"
    )

    val expectedResults1 = pageStateSet(
      pageId = Some(1L),
      pageCreationTimestamp = Some(new Timestamp(40000L)),
      pageFirstEditTimestamp = Some(new Timestamp(10000L))
    )(
      "start     end      titleH   title   eventType  inferred",
      "10000     40000    TitleA   TitleA   create     restore",
      "40000     None     TitleA   TitleA   restore    None"
    )
    processedStates should be (
      Seq(expectedResultsU1, expectedResults1, expectedResultsU2)
    )
  }


  it should "solve the restore-over problem"  in {
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
      "time  eventType  oldTitle  newTitleWP",
      "20000    move       TitleA    TitleX",
      "30000    move       TitleX    TitleA",
      "40000    delete     TitleA    TitleA",
      "50000    move       TitleX    TitleA",
      "60000    restore    TitleA    TitleA"
    )
    val states = pageStateSet(causedByEventType = Some("create"))(
      "titleH  id  creation firstEdit",
      "TitleA  1   10000     10000"
    )
    val processedStates = process(events, states)
    val expectedResultsX = pageStateSet(
      pageId = None,
      pageIdArtificial = processedStates.head.head.pageIdArtificial
    )(
      "start     end      titleH   title   eventType  adminId  inferred",
      "None      20000    TitleA  TitleA   create     None     delete",
      "20000     30000    TitleX  TitleA   move       0        None",
      "30000     40000    TitleA  TitleA   move       0        None",
      "40000     40000    TitleA  TitleA   delete     0        None"
    )

    val expectedResults1 = pageStateSet(
      pageId = Some(1L),
      pageCreationTimestamp = Some(new Timestamp(30000L)),
      pageFirstEditTimestamp =  Some(new Timestamp(10000L))
    )(
      "start   end     titleH  title   eventType  inferred",
      "10000  50000    TitleX  TitleA  create     move-conflict",
      "50000  60000    TitleA  TitleA  move       None",
      "60000  None     TitleA  TitleA  restore    None"
    )

    processedStates should be (
      Seq(expectedResultsX, expectedResults1)
    )
  }

  it should "solve the restore-merge problem"  in {
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
      "time  eventType  oldTitle  newTitleWP",
      "20000    move       TitleA    TitleB",
      "20000    delete     TitleB    TitleB",
      "30000    restore    TitleB    TitleB",
      "40000    move       TitleB    TitleA"
    )
    val states = pageStateSet(causedByEventType = Some("create"))(
      "titleH  id  creation firstEdit",
      "TitleA  1   10000    10000"
    )
    val processedStates = process(events, states)
    val expectedResultsU = pageStateSet(
      pageId = None,
      pageIdArtificial = processedStates.head.head.pageIdArtificial
    )(
      "start   end     titleH  title   eventType  adminId  inferred",
      "None   20000    TitleB  TitleB  create     None     delete",
      "20000  20000    TitleB  TitleB  delete     0        None"
    )

    val expectedResults1 = pageStateSet(
      pageId = Some(1L),
      pageCreationTimestamp = Some(new Timestamp(10000L)),
      pageFirstEditTimestamp = Some(new Timestamp(10000L))
    )(
      "start     end      titleH   title    eventType",
      "10000     20000    TitleA   TitleA   create",
      "20000     30000    TitleB   TitleA   move",
      "30000     40000    TitleB   TitleA   restore",
      "40000     None     TitleA   TitleA   move"
    )
    processedStates should be (
      Seq(expectedResultsU, expectedResults1)
    )
  }

  it should "solve the multi-restore problem"  in {
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
     *       ||    t1    |    t2    |    t3    |    t4    |    t5   |    t6    ||  States
     * ------||-------------------------------------------|---------|----------||----------
     *   p1  ||    +A    |          |   A->B   |   restB  |   B->A  |          ||    A
     *   pU1 ||          |   +B     |   delB   |          |         |          ||
     *   pU2 ||          |          |          |          |   +B*   |   restB  ||    B
     */

  }

  it should "count successes and failures" in {
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP  adminId",
      "02    move       Title1    Title2      10",
      "03    move       Title2    Title1      20",
      "04    move       Title1    Title3      30",
      "04    move       TitleX    TitleY      30"
    )
    val states = pageStateSet()(
      "titleH  id  creation  eventType  adminId",
      "Title3  1   01        create     40"
    )

    process(events, states)
    val stats = statsAccumulator.value
    stats.size() should equal(2)
    stats.get("testwiki.pages.eventsMatching.OK") should equal(3)
    stats.get("testwiki.pages.eventsMatching.KO") should equal(1)
  }

}
