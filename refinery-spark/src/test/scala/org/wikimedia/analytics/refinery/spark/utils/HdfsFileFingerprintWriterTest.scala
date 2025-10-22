package org.wikimedia.analytics.refinery.spark.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}

import java.io.{File, FileWriter, PrintWriter}
import java.nio.file.Files
import scala.io.Source

class HdfsFileFingerprintWriterTest extends FlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  private var spark: SparkSession = _
  private var fs: FileSystem = _
  private var testDir: File = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("HdfsFileFingerprintWriterTest")
      .getOrCreate()

    fs = FileSystem.getLocal(new Configuration())
  }

  override def beforeEach(): Unit = {
    testDir = Files.createTempDirectory("fingerprint_test").toFile
    testDir.deleteOnExit()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
  }

  override def afterEach(): Unit = {
    deleteRecursively(testDir)
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles.foreach(deleteRecursively)
    }
    file.delete()
  }

  private def createTestFile(name: String, content: String): File = {
    val file = new File(testDir, name)
    val writer = new PrintWriter(new FileWriter(file))
    try {
      writer.write(content)
    } finally {
      writer.close()
    }
    file
  }

  "HdfsFileFingerprintWriter" should "calculate correct SHA-256 for a simple file" in {
    val testContent = "Hello, World!"
    val testFile = createTestFile("simple.txt", testContent)
    val expectedHash = "dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f"

    val fingerprint = HdfsFileFingerprintWriter.calculateSHA256(fs, new Path(testFile.getPath))
    fingerprint should be(expectedHash)
  }

  it should "handle empty files correctly" in {
    val testFile = createTestFile("empty.txt", "")
    val expectedHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    val fingerprint = HdfsFileFingerprintWriter.calculateSHA256(fs, new Path(testFile.getPath))
    fingerprint should be(expectedHash)
  }

  it should "handle large files correctly" in {
    val largeContent = "a" * 1024 * 1024 // 1MB of 'a' characters
    val testFile = createTestFile("large.txt", largeContent)

    val fingerprint = HdfsFileFingerprintWriter.calculateSHA256(fs, new Path(testFile.getPath))
    fingerprint.length should be(64) // SHA-256 hash is always 64 characters in hex
  }

  it should "create manifest file with correct format" in {
    // Create test files
    val file1 = createTestFile("file1.txt", "content1")
    val file2 = createTestFile("file2.txt", "content2")

    // Create manifest
    HdfsFileFingerprintWriter.apply(testDir.getPath)

    // Read and verify manifest
    val manifestFile = new File(testDir, "SHA256SUMS")
    manifestFile.exists() should be(true)

    val manifest = Source.fromFile(manifestFile).mkString

    val reference =
      """d0b425e00e15a0d36b9b361f02bab63563aed6cb4665083905386c55d5b679fa  file1.txt
        |dab741b6289e7dccc1ed42330cae1accc2b755ce8079c2cd5d4b5366c9f769a6  file2.txt
        |""".stripMargin

    manifest should equal (reference)
  }

  it should "handle special characters in filenames" in {
    val testContent = "special content"
    val testFile = createTestFile("special!@#$%^&*.txt", testContent)

    val fingerprint = HdfsFileFingerprintWriter.calculateSHA256(fs, new Path(testFile.getPath))
    fingerprint.length should be(64)
  }

  it should "throw exception for non-existent file" in {
    val nonExistentPath = new Path(testDir.getPath, "non-existent.txt")

    an[Exception] should be thrownBy {
      HdfsFileFingerprintWriter.calculateSHA256(fs, nonExistentPath)
    }
  }

}