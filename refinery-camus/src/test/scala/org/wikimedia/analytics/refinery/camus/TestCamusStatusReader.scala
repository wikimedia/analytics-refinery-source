package org.wikimedia.analytics.refinery.camus

import com.linkedin.camus.etl.kafka.common.EtlKey
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{Matchers, FlatSpec}


class TestCamusStatusReader extends FlatSpec with Matchers {

  val camusHistoryTestFolder = "src/test/resources/camus-test-data"
  val runFolder = "2015-08-15-17-52-01"
  val mostRecentRunFolder = "2015-10-02-08-00-07"
  val twoMostRecentRunFolders = Seq("2015-09-29-15-20-08", "2015-10-02-08-00-07")
  val wrongFolder = "wrong-folder"
  val fs = FileSystem.get(new Configuration)
  val cr = new CamusStatusReader(fs)

  "A CamusStatusReader" should "read EtlKey values in offset-m-XXXXX sequence file" in {

    val file: String = camusHistoryTestFolder + "/" + runFolder + "/" + "offsets-m-00035"
    val path: Path = new Path(file)

    val keys = cr.readEtlKeys(path)
    val expectedKey = new EtlKey("webrequest_maps", "", 1, 29811L, 29812L, 1198780952L)
    expectedKey.setTime(1439661148000L)
    expectedKey.setServer("unknown_server")
    expectedKey.setService("unknown_service")
    
    keys.size should equal (1)
    // Terrible hack because equality function returns false while objects are equal ...
    // TODO: investigate why equals method return false in that case
    keys.head.toString should equal (expectedKey.toString)
  }

  it should "read EtlKey values in offset-previous sequence file" in {

    val file: String = camusHistoryTestFolder + "/" + runFolder + "/" + "offsets-previous"
    val path: Path = new Path(file)

    val keys = cr.readEtlKeys(path)

    keys.size should equal (72)
    keys.filter(_.getTopic.equals("webrequest_upload")).size should equal (12)
    keys.filter(_.getTopic.equals("webrequest_text")).size should equal (12)
    keys.filter(_.getTopic.equals("webrequest_misc")).size should equal (12)
    keys.filter(_.getTopic.equals("webrequest_maps")).size should equal (12)
  }

  it should "read EtlKey values in multiple offset-m-XXXXX sequence file" in {

    val files: Seq[String] = for (i <- 0 to 35)
                              yield camusHistoryTestFolder + "/" + runFolder +
                                    "/" + "offsets-m-000%02d".format(i)
    val paths: Seq[Path] = files.map(new Path(_))

    val keys = cr.readEtlKeys(paths)

    keys.size should equal (36)
    keys.filter(_.getTopic.equals("webrequest_misc")).size should equal (12)
    keys.filter(_.getTopic.equals("webrequest_maps")).size should equal (12)

  }

  it should "list offsets files in a correct camus-run folder" in {
    val folder: String = camusHistoryTestFolder + "/" + runFolder
    val path: Path = new Path(folder)

    val offsetsFilesNames = cr.offsetsFiles(path).map(_.getName)
    val expectedOffsetsFilesNames = for (i <- 0 to 35) yield "offsets-m-000%02d".format(i)
    for (offsetsFileName <- offsetsFilesNames)
      expectedOffsetsFilesNames should contain (offsetsFileName)

    val previousOffsetsFilesNames = cr.previousOffsetsFiles(path).map(_.getName)
    val expectedPreviousOffsetsFilesNames = List("offsets-previous")
    for (previousOffsetsFileName <- previousOffsetsFilesNames)
      expectedPreviousOffsetsFilesNames should contain (previousOffsetsFileName)
  }

  it should "return empty list in non camus-run folder" in {
    val folder: String = camusHistoryTestFolder + "/" + wrongFolder
    val path: Path = new Path(folder)

    val offsetsFilesNames = cr.offsetsFiles(path)
    offsetsFilesNames should be (Seq.empty)
    val previousOffsetsFilesNames = cr.previousOffsetsFiles(path)
    previousOffsetsFilesNames should be (Seq.empty)
  }

  it should "return the most recent camus runs in a camus-history folder" in {
    val folder: String = camusHistoryTestFolder
    val path: Path = new Path(folder)

    val mostRecentRunsPath = cr.mostRecentRuns(path, 2)

    (twoMostRecentRunFolders zip mostRecentRunsPath).foreach( {
      case (expected_path: String, path: Path) => {
        path.getName should equal (expected_path)
      }
    })
  }

  it should "return the most recent camus run in a camus-history folder" in {
    val folder: String = camusHistoryTestFolder
    val path: Path = new Path(folder)

    val mostRecentRunPath = cr.mostRecentRun(path)

    mostRecentRunPath.getName should equal (mostRecentRunFolder)
  }

  it should "throw an exception looking for most recent run in a non camus-history folder" in {
    val folder: String = camusHistoryTestFolder + "/" + wrongFolder
    val path: Path = new Path(folder)

    intercept[IllegalArgumentException] {
      cr.mostRecentRun(path)
    }
  }

}
