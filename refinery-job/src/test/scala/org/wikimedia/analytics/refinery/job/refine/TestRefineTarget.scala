package org.wikimedia.analytics.refinery.job.refine

import java.io.File
import scala.collection.immutable.ListMap
import org.scalatest._
import com.github.nscala_time.time.Imports.DateTime
import com.github.nscala_time.time.Imports._
import com.holdenkarau.spark.testing.SharedSparkContext

import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext}

import org.wikimedia.analytics.refinery.core.HivePartition



class TestRefineTarget extends FlatSpec with Matchers with TestDirectory with SharedSparkContext with BeforeAndAfter {

    var inputDir: String = _
    var outputDir: String = _
    var inputDf: DataFrame = _
    var target: RefineTarget = _

    before {
        val sqlContext = new SQLContext(sc)
        inputDf = sqlContext.createDataFrame(sc.parallelize(Seq(TestA())))

        inputDir = testDirectory + "/input"
        outputDir = testDirectory + "/output"

        val fs = new LocalFileSystem()

        val inputPath = new Path("file:///" + inputDir)

        // infer the format of inputPath
        target = RefineTarget(
            fs,
            inputPath,
            HivePartition("test", "testjson", outputDir, ListMap("year" -> "2018"))
        )
    }

    it should "hoursInBetween gives hours in between two DateTimes" in {
        val d1 = DateTime.now - 3.hours
        val d2 = DateTime.now

        val hoursShouldBe = Seq(
            (new DateTime(DateTime.now, DateTimeZone.UTC) - 3.hours).hourOfDay.roundFloorCopy,
            (new DateTime(DateTime.now, DateTimeZone.UTC) - 2.hours).hourOfDay.roundFloorCopy,
            (new DateTime(DateTime.now, DateTimeZone.UTC) - 1.hours).hourOfDay.roundFloorCopy
        )

        val hours = RefineTarget.hoursInBetween(d1, d2)
        hours should equal (hoursShouldBe)
    }

    it should "inferInputFormat as empty and get empty DataFrame" in {
        // write the df out as json sequence file
        target.fs.mkdirs(target.inputPath)
        target.inferInputFormat(sc) should equal ("empty")

        val df = target.inputDataFrame(new SQLContext(sc))
        df.count should equal(0)
    }

    it should "inferInputFormat as parquet and read DataFrame" in {
        // write the df out as json sequence file
        inputDf.write.parquet(inputDir)
        target.inferInputFormat(sc) should equal ("parquet")

        // should automatically read written parquet file
        val df = target.inputDataFrame(new SQLContext(sc))
        df.count should equal(1)
        df.select("a").take(1)(0).getString(0) should equal("a")
    }

    it should "inferInputFormat as json and read DataFrame" in {
        // write the df out as json to the tmpDir
        inputDf.write.json(inputDir)
        target.inferInputFormat(sc) should equal ("json")

        // should automatically read written json file
        val df = target.inputDataFrame(new SQLContext(sc))
        df.count should equal(1)
        df.select("a").take(1)(0).getString(0) should equal("a")
    }

    it should "inferInputFormat as sequence file json and read DataFrame" in {
        // write the df out as json sequence file
        inputDf.toJSON.map((0L, _)).saveAsSequenceFile(inputDir)
        target.inferInputFormat(sc) should equal ("sequence_json")

        // should automatically read written json sequence file
        val df = target.inputDataFrame(new SQLContext(sc))
        df.count should equal(1)
        df.select("a").take(1)(0).getString(0) should equal("a")
    }

}


case class NestedA(
    n: String = "n"
)
case class TestA(
    a: String = "a",
    b: NestedA = NestedA()
)


// Stolen from:
// https://github.com/robey/scalatest-mixins/blob/master/src/main/scala/com/twitter/scalatest/TestFolder.scala
/**
  * Creates a temporary directory for the lifetime of a single test.
  * The directory's name will exist in a `File` field named `testDirectory`.
  */
trait TestDirectory extends SuiteMixin { self: Suite =>
    var testDirectory: File = _
    var testPath: Path = _

    private def deleteFile(file: File) {
        if (!file.exists) return
        if (file.isFile) {
            file.delete()
        } else {
            file.listFiles().foreach(deleteFile)
            file.delete()
        }
    }

    abstract override def withFixture(test: NoArgTest): Outcome = {
        val tmpdir = System.getProperty("java.io.tmpdir", "/tmp")
        var tempTestDirectory: File = null

        do {
            tempTestDirectory = new File(tmpdir, "scalatest-" + System.nanoTime)
        } while (! tempTestDirectory.mkdir())

        testDirectory = tempTestDirectory

        try {
            super.withFixture(test)
        } finally {
            deleteFile(testDirectory)
        }
    }
}