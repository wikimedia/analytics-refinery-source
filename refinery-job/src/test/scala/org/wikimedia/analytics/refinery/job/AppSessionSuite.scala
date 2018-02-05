import org.scalatest.FunSuite
import org.wikimedia.analytics.refinery.job.AppSessionMetrics
import scala.collection.immutable.HashMap


class AppSessionSuite extends FunSuite {

  test("Timestamps that are further apart than 1800 secs belong to different sessions") {
    // this line below  might be marked an error on intelliJ IDE, it is not a real error
    val input: List[(String, List[Long])] = List(("02c17d16-c7a8-4e73-aa35-78076a4c20ec", List(1426556733, 1426556748, 1426556798)),
      // uuid below should have two sessions
      ("b418f244-413d-4935-8dc5-662eab516c14", List(1426556468, 1426556498, 1426559098, 1426559200)),
      ("d4417a46-f99e-45cb-9435-242ec04510cd", List(1426557279, 1426557379, 1426557399)),
      ("bff1bbec-fb5f-4f5a-9168-8e84a7b488e0", List(1426555022)),
      ("0b26337f-80f7-4301-bf92-8bd4f4b89fb8", List(1426554780, 1426554808, 1426554833)))

    val emptySessions = List.empty[List[Long]]
    val output = input.map { case (uuid, listOfTimestampLists) => uuid -> listOfTimestampLists.foldLeft(emptySessions)(AppSessionMetrics.sessionize) }

    //assert 1st user's session is like: (02c17d16-c7a8-4e73-aa35-78076a4c20ec,List(List(1426556733, 1426556748, 1426556798))
    val session1 = output.head
    assert(session1._2.length == 1)

    //assert 2nd user session is
    // (b418f244-413d-4935-8dc5-662eab516c14,List(List(1426556468, 1426556498), List(1426559098, 1426559200)))
    val session2 = output.tail.head
    assert(session2._2.length==2)

  }


  test("Quantile generation from session data") {
    // this line below  might be marked an error on intelliJ IDE, it is not a real error
    val input: List[(String, List[Long])] = List(("02c17d16-c7a8-4e73-aa35-78076a4c20ec", List(1426556733, 1426556748, 1426556798)),
      // uuid below should have two sessions
      ("b418f244-413d-4935-8dc5-662eab516c14", List(1426556468, 1426556498, 1426559098, 1426559200)),
      ("d4417a46-f99e-45cb-9435-242ec04510cd", List(1426557279, 1426557379, 1426557399)),
      ("bff1bbec-fb5f-4f5a-9168-8e84a7b488e0", List(1426555022)),
      ("0b26337f-80f7-4301-bf92-8bd4f4b89fb8", List(1426554780, 1426554808, 1426554833)))

    val emptySessions = List.empty[List[Long]]
    val output = input.map { case (uuid, listOfTimestampLists) => uuid -> listOfTimestampLists.foldLeft(emptySessions)(AppSessionMetrics.sessionize) }


    // output looks like (only first two elements)
    //     (02c17d16-c7a8-4e73-aa35-78076a4c20ec,List(List(1426556733, 1426556748, 1426556798)))
    //    (b418f244-413d-4935-8dc5-662eab516c14,List(List(1426556468, 1426556498), List(1426559098, 1426559200)))

   // now see what quantiles calculation says



  }


  test("Report date range string should be generated correctly based on the report run date and period") {
    val datesInfo = HashMap("year" -> 2015, "month" -> 5, "day" -> 10, "periodDays" -> 10)
    assert(AppSessionMetrics.dateRangeToString(datesInfo) == "2015-5-10 -- 2015-5-19")
  }

  test("List of parquet paths is generated correctly based on the report run date and period") {
    val datesInfo = HashMap("year" -> 2015, "month" -> 5, "day" -> 10, "periodDays" -> 10)
    val webrequestTextPath = ".../webrequest_source=text"
    val pathList = AppSessionMetrics.dateRangeToPathList(webrequestTextPath, datesInfo)

    //Assert the length of the list equals report period in days
    assert(pathList.length == datesInfo("periodDays"))

    //Assert the paths are being generated correctly
    assert(pathList.head == ".../webrequest_source=text/year=2015/month=5/day=10/*")
    assert(pathList.last == ".../webrequest_source=text/year=2015/month=5/day=19/*")
  }

}