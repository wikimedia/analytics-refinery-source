package org.wikimedia.analytics.refinery.job.mediawikihistory.page

import java.sql.Timestamp

import org.scalatest.{BeforeAndAfterEach, Matchers, FlatSpec}

class TestPageHistoryBuilder extends FlatSpec with Matchers with BeforeAndAfterEach {

  import org.apache.spark.sql.SQLContext
  import org.wikimedia.analytics.refinery.job.mediawikihistory.page.TestPageHistoryHelpers._
  // Implicit needed to sort by timestamps
  import org.wikimedia.analytics.refinery.job.mediawikihistory.utils.TimestampHelpers.orderedTimestamp

  var pageHistoryBuilder = null.asInstanceOf[PageHistoryBuilder]

  override def beforeEach(): Unit = {
    pageHistoryBuilder = new PageHistoryBuilder(null.asInstanceOf[SQLContext])
  }

  /**
   * Helper to execute processSubgraph with the given input
   * and organize the output so that it can be asserted easily.
   */
  def process(e: Iterable[PageEvent], s: Iterable[PageState]): Seq[Seq[PageState]] = {
    pageHistoryBuilder.processSubgraph(e, s)._1.groupBy(_.pageId).toSeq.sortBy(_._1).map{
      case (pageId, pageStates) => pageStates.sortBy(_.startTimestamp.getOrElse(new Timestamp(0L)))
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

  it should "flush states that conflict with move events" in {
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP  adminId",
      "02    move       Title1    Title2      0"
    )
    val states = pageStateSet()(
      "titleH  id  creation  eventType",
      "Title1  1   01        create"
    )
    val expectedResults = pageStateSet()(
      "start  end   titleH  id  creation  eventType  adminId  inferred",
      "02     None  Title1  1   02        create     None     move-conflict"
    )
    process(events, states) should be (Seq(expectedResults))
  }

  it should "flush states that conflict with delete events" in {
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP  adminId",
      "02    delete     Title     Title       0"
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
      "None   02    Title   None  None      create     None     delete",
      "02     None  Title   None  None      delete     0        None"
    )
    val expectedResults1 = pageStateSet()(
      "start  end   titleH  id  creation  eventType  adminId  inferred",
      "02     None  Title   1   02        create     None     delete-conflict"
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
      "04     None  Title3  Title3  delete     40       None"
    )
    processedStates should be (Seq(expectedResults))
  }

  it should "take the namespace into account when joining" in {
    val events = pageEventSet()(
      "time  eventType  oldNs  newNs  oldTitle  newTitleWP",
      "02    move       0      0      Title1    Title2",
      "03    move       1      1      Title1    Title2",
      "04    move       0      1      Title1    Title1"
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
      "start  end   nsH  titleH   title   eventType",
      "01     02    0    Title1   Title2  create",
      "02     None  0    Title2   Title2  move"
    )
    val expectedResults2 = pageStateSet(
      pageId = Some(2L), pageCreationTimestamp = Some(new Timestamp(1L))
    )(
      "start  end   nsH  titleH   title   eventType",
      "01     03    1    Title1   Title2  create",
      "03     None  1    Title2   Title2  move"
    )
    val expectedResults3 = pageStateSet(
      // Note modified startTimestamp and pageCreationTimestamp plus
      // no admin id because of oldTitle conflict with first event.
      pageId = Some(3L), pageCreationTimestamp = Some(new Timestamp(2L))
    )(
      "start  end   nsH  ns  titleH  eventType  adminId  inferred",
      "02     04    0    1   Title1  create     None     move-conflict",
      "04     None  1    1   Title1  move       0        None"
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
      pageIdArtificial = processedStates.head.head.pageIdArtificial,
      pageCreationTimestamp = Some(new Timestamp(2L))
    )(
      "start  end   titleH  eventType  adminId  inferred",
      "02     03    TitleA  create     None     move-conflict",
      "03     None  TitleA  delete     0        None"
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
      Seq(expectedResultsArtificial, expectedResults1)
    )
  }


  it should "process restore event in a usual delete case" in {
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP",
      "02    delete     TitleA    TitleA",
      "03    restore    TitleA    TitleA"
    )
    val states = pageStateSet()(
      "titleH  id  creation  eventType",
      "TitleA  1   01        create"
    )
    val processedStates = process(events, states)
    val expectedResultsArtificial = pageStateSet(
      pageId = Some(1),
      pageIdArtificial = processedStates.head.head.pageIdArtificial,
      pageCreationTimestamp = Some(new Timestamp(1L))
    )(
      "start  end   titleH  eventType",
      "01     02    TitleA  create",
      "02     03    TitleA  delete",
      "03     None  TitleA  restore"
    )
    processedStates should be (
      Seq(expectedResultsArtificial)
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
      pageIdArtificial = processedStates.head.head.pageIdArtificial,
      pageCreationTimestamp = Some(new Timestamp(3L))
    )(
      "start  end   titleH  eventType  adminId  inferred",
      "03     04    TitleB  create     None     move-conflict",
      "04     None  TitleB  delete     0        None"
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
      Seq(expectedResultsArtificial, expectedResults1)
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

  it should "solve the restore-over problem"  in {
    /*
     * The restore problem happens on page_id 27264 around 2016-05-16.
     * Correct page is moved, with redirects coming into play, then at some
     * point deleted with a false page moved to correct name. After, correct
     * page gets restored, overwriting the incorrect page already in place.
     *       ||    t1    |    t2    |    t3    |    t4    |    t5   |    t6    ||  States
     * ------||-------------------------------------------|---------|----------||----------
     *   p1  ||    +A    |   A->X   |   X->A   |   delA   |         |   restA  ||    A
     *   pU  ||          |          |   +X*    |          |   X->A  |   delA   ||
     */
    val events = pageEventSet()(
      "time  eventType  oldTitle  newTitleWP",
      "02    move       TitleA    TitleX",
      "03    move       TitleX    TitleA",
      "04    delete     TitleA    TitleA",
      "05    move       TitleX    TitleA",
      "06    restore    TitleA    TitleA"
    )
    val states = pageStateSet(causedByEventType = Some("create"))(
      "titleH  id  creation",
      "TitleA  1   01"
    )
    val processedStates = process(events, states)
    val expectedResultsX = pageStateSet(
      pageId = None,
      pageIdArtificial = processedStates.head.head.pageIdArtificial,
      pageCreationTimestamp = Some(new Timestamp(3L))
    )(
      "start  end   titleH  title   eventType  adminId  inferred",
      "03     05    TitleX  TitleA  create      None    move-conflict",
      "05     06    TitleA  TitleA  move        0       None",
      "06     None  TitleA  TitleA  delete      0       restore"
    )

    val expectedResults1 = pageStateSet(
      pageId = Some(1L), pageCreationTimestamp = Some(new Timestamp(1L))
    )(
      "start  end   titleH  title   eventType",
      "01     02    TitleA  TitleA  create",
      "02     03    TitleX  TitleA  move",
      "03     04    TitleA  TitleA  move",
      "04     06    TitleA  TitleA  delete",
      "06     None  TitleA  TitleA  restore"
    )
    processedStates should be (
      Seq(expectedResultsX, expectedResults1)
    )
  }

}
