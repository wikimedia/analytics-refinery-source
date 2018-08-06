package org.wikimedia.analytics.refinery.camus

import java.io.File
import java.nio.file.Files


import org.apache.hadoop.fs.Path
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.reflect.io.Directory


class TestCamusPartitionChecker extends FlatSpec with Matchers with BeforeAndAfterEach {

  val camusHistoryTestFolder = "../refinery-camus/src/test/resources/camus-test-data"
  val failedRunFolder = "2015-08-15-17-52-01"
  val noHourRunFolder = "2015-09-29-15-20-08"
  val hourSpanRunFolder = "2015-10-02-08-00-07"
  val wrongFolder = "wrong-folder"
  var tmpDir:File = null

  override def beforeEach(): Unit = {
    CamusPartitionChecker.props.clear()
  }

  override def afterEach(): Unit = {
    if (tmpDir != null && tmpDir.exists()) {
      val d = new Directory(tmpDir)
      d.deleteRecursively()
    }
  }

  "A CamusChecker" should "find hours in between timestamps" in {
    val t1: Long = 1443428181000L // 2015-09-28T08:16:21  UTC
    val t2: Long = 1443436242000L // 2015-09-28T10:30:42  UTC

    val hours = CamusPartitionChecker.finishedHoursInBetween(t1, t2)
    val expectedHours = Seq((2015, 9, 28, 8), (2015, 9, 28, 9))
    hours should equal (expectedHours)
  }

  it should "find no hours in between inversed timestamps" in {
    val t1: Long = 1443428181000L // 2015-09-28T10:16:21
    val t2: Long = 1443436242000L // 2015-09-28T12:30:42

    val hoursEmpty = CamusPartitionChecker.finishedHoursInBetween(t2, t1)
    hoursEmpty should equal (Seq.empty)
  }

  it should "find hours no hours in between equal timestamps" in {
    val t1: Long = 1443428181000L // 2015-09-28T10:16:21
    val t2: Long = 1443428181000L // 2015-09-28T10:16:21

    val hoursEmpty = CamusPartitionChecker.finishedHoursInBetween(t2, t1)
    hoursEmpty should equal (Seq.empty)
  }

  it should "compute partition directory" in {
    val base = "/test/base/folder"
    val topic = "topic"
    val (year, month, day, hour) = (2015, 9, 28, 1)

    val partitionDir = CamusPartitionChecker.partitionDirectory(base, topic, year, month, day, hour)
    val expectedDir = "/test/base/folder/topic/hourly/2015/09/28/01"

    partitionDir should equal(expectedDir)
  }

  it should "compute partition directory with dotted topic" in {
    val base = "/test/base/folder"
    val topic = "topic.test"
    val (year, month, day, hour) = (2015, 9, 28, 1)

    val partitionDir = CamusPartitionChecker.partitionDirectory(base, topic, year, month, day, hour)
    val expectedDir = "/test/base/folder/topic_test/hourly/2015/09/28/01"

    partitionDir should equal(expectedDir)
  }

  it should "fail computing partition directory if no base" in {
    val base = null
    val topic = "topic"
    val (year, month, day, hour) = (2015, 9, 28, 1)

    intercept[IllegalArgumentException] {
      CamusPartitionChecker.partitionDirectory(base, topic, year, month, day, hour)
    }
  }

  it should "fail computing partition directory if no topic" in {
    val base = "/test/base/folder"
    val topic = null
    val (year, month, day, hour) = (2015, 9, 28, 1)

    intercept[IllegalArgumentException] {
      CamusPartitionChecker.partitionDirectory(base, topic, year, month, day, hour)
    }
  }

  it should "get topics and hours in a camus-run folder with whitelist" in {
    val folder: String = camusHistoryTestFolder + "/" + hourSpanRunFolder
    val path: Path = new Path(folder)

    // correct Whitelist, no blacklist --> Should work, one topic in historical data needs to be left aside
    CamusPartitionChecker.props.setProperty(CamusPartitionChecker.WHITELIST_TOPICS,
      "webrequest_maps,webrequest_text,webrequest_upload,webrequest_misc")

    val topicsAndHours = CamusPartitionChecker.getCamusPartitionsToFlag(path).topicsAndHours

    topicsAndHours.size should equal (4)
    // Some topics have hour to flag
    for ((t, o) <- topicsAndHours)
      if (! t.equals("webrequest_maps")) o.size should equal (1)
      else o.size should equal (0)
  }

  it should "get topics and hours in a camus-run folder with blacklist" in {
    val folder: String = camusHistoryTestFolder + "/" + hourSpanRunFolder
    val path: Path = new Path(folder)

    // No whitelist, correct blacklist --> Should work, one topic in historical data needs to be left aside
    CamusPartitionChecker.props.setProperty(CamusPartitionChecker.BLACKLIST_TOPICS,
      ".*_bits,.*_test")

    val topicsAndHours = CamusPartitionChecker.getCamusPartitionsToFlag(path).topicsAndHours

    topicsAndHours.size should equal (5)
    // Some topics have hour to flag
    for ((t, o) <- topicsAndHours)
      if (! t.equals("webrequest_maps")) o.size should equal (1)
      else o.size should equal (0)
  }

  it should "Get topics and hours in a camus-run folder with incorrect whitelist only for successful topics" in {
    val folder: String = camusHistoryTestFolder + "/" + hourSpanRunFolder
    val path: Path = new Path(folder)

    // everything whitelist and no blacklist (by default)
    // --> Should return 5 topics over 6, one topic in historical data should fail
    val topicsAndHours = CamusPartitionChecker.getCamusPartitionsToFlag(path).topicsAndHours

    topicsAndHours.size should equal (5)
    // Some topics have hour to flag
    for ((t, o) <- topicsAndHours)
      if (! t.equals("webrequest_maps")) o.size should equal (1)
      else o.size should equal (0)

  }

  it should "Get topics and hours in a camus-run folder with incorrect blacklist only for successful topics" in {
    val folder: String = camusHistoryTestFolder + "/" + hourSpanRunFolder
    val path: Path = new Path(folder)

    // No whitelist, incorrect blacklist
    // --> Should return 5 topics over 6, one topic in historical data should fail
    CamusPartitionChecker.props.setProperty(CamusPartitionChecker.BLACKLIST_TOPICS,
      ".*_test")

    val topicsAndHours = CamusPartitionChecker.getCamusPartitionsToFlag(path).topicsAndHours

    topicsAndHours.size should equal (5)
    // Some topics have hour to flag
    for ((t, o) <- topicsAndHours)
      if (! t.equals("webrequest_maps")) o.size should equal (1)
      else o.size should equal (0)
  }

  it should "get topics and hours in a camus-run folder with whitelist with no hours to flag" in {
    val folder: String = camusHistoryTestFolder + "/" + noHourRunFolder
    val path: Path = new Path(folder)

    // correct Whitelist/blacklist config
    CamusPartitionChecker.props.setProperty(CamusPartitionChecker.WHITELIST_TOPICS,
      "webrequest_maps,webrequest_text,webrequest_upload,webrequest_misc")

    val topicsAndHours = CamusPartitionChecker.getCamusPartitionsToFlag(path).topicsAndHours

    topicsAndHours.size should equal (4)
    // No hours to flag
    for ((t, o) <- topicsAndHours)
      o.size should equal (0)
  }

  it should "Get topics and hours in an error camus-run folder only for successful topics" in {
    val folder: String = camusHistoryTestFolder + "/" + failedRunFolder
    val path: Path = new Path(folder)

    // correct Whitelist/blacklist config
    CamusPartitionChecker.props.setProperty(CamusPartitionChecker.WHITELIST_TOPICS,
      "webrequest_maps,webrequest_text,webrequest_upload,webrequest_misc")

    val camusPartitionsToFlag = CamusPartitionChecker.getCamusPartitionsToFlag(path)

    val topicsAndHours = camusPartitionsToFlag.topicsAndHours

    // 2 / 4 topics have no errors, so we will only have 2 topics, with no hours.
    topicsAndHours.size should equal (2)
    // No hours to flag
    for ((t, o) <- topicsAndHours)
      o.size should equal (0)

    // should have 2 error messages
    camusPartitionsToFlag.errors.size should equal (2)
  }

  it should "write the file flag for a given partition hour" in {
    tmpDir = Files.createTempDirectory("testcamus").toFile
    val partitionFolder = "testtopic/hourly/2015/10/02/08"
    val d = new Directory(new File(tmpDir, partitionFolder))
    d.createDirectory()

    d.list shouldBe empty

    // correct partition base path config
    CamusPartitionChecker.props.setProperty(CamusPartitionChecker.PARTITION_BASE_PATH, tmpDir.getAbsolutePath)

    CamusPartitionChecker.flagFullyImportedPartitions("_TESTFLAG", dryRun = false, Map("testtopic" -> Seq((2015, 10, 2, 8))))

    d.list should not be empty
    d.list.toSeq.map(_.toString()) should contain (tmpDir.getAbsolutePath + "/testtopic/hourly/2015/10/02/08/_TESTFLAG")

  }

  it should "not write the file flag if the given partition hour folder doesn't exist" in {
    tmpDir = Files.createTempDirectory("testcamus").toFile
    val partitionFolder = "testtopic/hourly/2015/10/02/08"
    val d = new Directory (new File(tmpDir, partitionFolder))

    d.exists shouldBe false

    // correct partition base path config
    CamusPartitionChecker.props.setProperty(CamusPartitionChecker.PARTITION_BASE_PATH, tmpDir.getAbsolutePath)

    CamusPartitionChecker.flagFullyImportedPartitions("_TESTFLAG", dryRun = false, Map("testtopic" -> Seq((2015, 10, 2, 8))))

    d.exists shouldBe false
  }

}
