package org.wikimedia.analytics.refinery.job

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}

import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._


class TestDataPivoter extends FlatSpec with Matchers with DataFrameSuiteBase {

    val validSourceDirectory = getClass.getResource("/data_pivoter/valid_source").toString
    val validExpectedDataFile = getClass.getResource("/data_pivoter/valid_expected/expected_data.csv")
    val validDestinationDirectory = Files.createTempDirectory(
        s"data_pivoter_test_${System.currentTimeMillis / 1000}"
    ).toFile.getAbsolutePath

    val duplicationSourceDirectory = getClass.getResource("/data_pivoter/duplication_source").toString

    it should "pivot a file" in {
        val testConfig = DataPivoter.Config(
            validSourceDirectory,
            validDestinationDirectory
        )

        DataPivoter(spark)(testConfig).isSuccess should equal(true)
        val destDir : Path = Paths.get(validDestinationDirectory)
        Files.exists(destDir) should equal(true)
        Files.isDirectory(destDir) should equal(true)

        val validFilesInResDir = Files.list(destDir).filter(p => {
            val fileName = p.getFileName.toString
            !fileName.startsWith(".") && !fileName.startsWith("_")
        }).iterator().asScala.toList

        validFilesInResDir.size should equal(1)
        val resFile = validFilesInResDir.head
        val resLines = Files.readAllLines(resFile).iterator().asScala.toList
        val expectedLines = Files.readAllLines(Paths.get(validExpectedDataFile.toURI)).iterator().asScala.toList

        val zippedLines = resLines.zip(expectedLines)

        // Validating header
        zippedLines.head._1 should equal(zippedLines.head._2)

        // Validating values (double precision differences between spark and python)
        zippedLines.tail.foreach { case (line, expectedLine) =>
            val values = line.split("\t")
            val expectedValues = expectedLine.split("\t")
            // Validate dates
            values.head should equal(expectedValues.head)
            // Validate values
            val zippedValues = values.tail.zip(expectedValues.tail)
            zippedValues.foreach { case (value, expectedValue) =>
                // Validate that values don't differ by more than 1e-10d
                // This small difference is due to python vs spark aggregation
                // of double encoded numbers.
                Math.abs(value.toDouble - expectedValue.toDouble)  < 1e-10d should equal(true)
            }
        }
    }

    it should "fail to pivot a file with duplication in dimension-columns values" in {
        val testConfig = DataPivoter.Config(
            duplicationSourceDirectory,
            "fake/output/folder",
        )

        val jobRes = DataPivoter(spark)(testConfig)
        jobRes.isFailure should equal(true)
        jobRes.failed.get.getClass.toString should equal("class java.lang.IllegalStateException")
    }

    it should "fail to open a non-existing file" in {
        val testConfig = DataPivoter.Config(
            "fake/directory",
            "fake/directory",
        )
        val jobRes = DataPivoter(spark)(testConfig)
        jobRes.isFailure should equal(true)
        jobRes.failed.get.getClass.toString should equal("class org.apache.spark.sql.AnalysisException")
    }
}
