package org.wikimedia.analytics.refinery.job.mediawikidumper

import java.io.{ByteArrayOutputStream, File, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import scala.collection.JavaConverters._

import org.apache.commons.compress.archivers.sevenz.{SevenZFile, SevenZMethod}
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}
import org.tukaani.xz.LZMA2Options

// -Dsuites="org.wikimedia.analytics.refinery.job.mediawikidumper.MediawikiDumperSevenZipRecompressorSpec"
class MediawikiDumperSevenZipRecompressorSpec
    extends FlatSpec
    with Matchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll {

    lazy val spark: SparkSession = SparkSession
        .builder()
        .master("local[2]")
        .appName("MediawikiDumperSevenZipRecompressorSpec")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()

    override def afterAll(): Unit = spark.stop()

    private var tmpDir: File = _
    private var inputFolder: File = _
    private var outputFolder: File = _

    /** The production dictionary size, which is also the size v1 uses. A measured
      * sweep shows that a larger one costs core time for little gain. See T437454.
      */
    private val testDictionarySizeMB: Int = 4

    private def baseParams: MediawikiDumperSevenZipRecompressor.Params = {
        MediawikiDumperSevenZipRecompressor.Params(
          inputFolder = inputFolder.getAbsolutePath,
          outputFolder = outputFolder.getAbsolutePath,
          dictionarySizeMB = testDictionarySizeMB,
          // Production runs with this off, because it costs 35% of the core time. The
          // test data is tiny, so keep it on here to exercise the read back path.
          verify = true
        )
    }

    override def beforeEach(): Unit = {
        // Build the session before the job runs. The job calls
        // SparkSession.builder.getOrCreate, which needs a master, and this suite is the
        // only place that sets one.
        spark

        tmpDir = Files
            .createTempDirectory(
              s"mediawikidumper_7z_test_${System.currentTimeMillis / 1000}"
            )
            .toFile
        inputFolder = new File(tmpDir, "bzip2")
        outputFolder = new File(tmpDir, "7z")
        inputFolder.mkdirs()
    }

    override def afterEach(): Unit = {
        deleteRecursively(tmpDir)
    }

    private def deleteRecursively(file: File): Unit = {
        if (file.isDirectory) {
            Option(file.listFiles).foreach(_.foreach(deleteRecursively))
        }
        file.delete()
    }

    /** Builds XML that looks like a history dump.
      *
      * Every revision repeats most of the page text. This is the redundancy that
      * makes a large LZMA2 dictionary worth the cost, so the test data must have
      * it too.
      *
      * @param pageId
      *   the page ID to put in the XML
      * @param revisionCount
      *   the number of revisions to generate
      * @return
      *   the XML as a string
      */
    private def buildHistoryXML(pageId: Int, revisionCount: Int): String = {
        val body = (1 to 40)
            .map(n => s"This is line $n of a page that changes very little.")
            .mkString("\n")

        val revisions = (1 to revisionCount).map { revision =>
            s"""    <revision>
               |      <id>${pageId * 1000 + revision}</id>
               |      <timestamp>2023-09-0${revision % 9 + 1}T00:00:00Z</timestamp>
               |      <contributor><username>Editor$revision</username></contributor>
               |      <text xml:space="preserve">$body
               |Revision $revision added this one line.</text>
               |    </revision>""".stripMargin
        }

        s"""<mediawiki>
           |  <page>
           |    <title>Page $pageId</title>
           |    <id>$pageId</id>
           |${revisions.mkString("\n")}
           |  </page>
           |</mediawiki>
           |""".stripMargin
    }

    /** Writes a bzip2 dump file into the input folder.
      *
      * @param name
      *   the file name, including the .xml.bz2 extension
      * @param xml
      *   the content to compress
      */
    private def writeBzip2Input(name: String, xml: String): Unit = {
        val output = new BZip2CompressorOutputStream(
          new FileOutputStream(new File(inputFolder, name))
        )
        try {
            output.write(xml.getBytes(StandardCharsets.UTF_8))
        } finally {
            output.close()
        }
    }

    /** Reads the single entry of a 7z archive.
      *
      * @param archive
      *   the archive to read
      * @return
      *   the entry name, the content, and the dictionary size that the archive
      *   reports
      */
    private def readSingleEntry(archive: File): (String, String, Int) = {
        val sevenZFile = new SevenZFile(archive)
        try {
            val entry = sevenZFile.getNextEntry
            entry should not be null

            val buffer = new Array[Byte](8192)
            val content = new ByteArrayOutputStream()
            var read = sevenZFile.read(buffer)
            while (read != -1) {
                content.write(buffer, 0, read)
                read = sevenZFile.read(buffer)
            }

            sevenZFile.getNextEntry shouldBe null

            val methods = entry.getContentMethods.asScala.toList
            methods should have size 1
            methods.head.getMethod should equal(SevenZMethod.LZMA2)

            (
              entry.getName,
              new String(content.toByteArray, StandardCharsets.UTF_8),
              methods.head.getOptions.asInstanceOf[Integer].intValue()
            )
        } finally {
            sevenZFile.close()
        }
    }

    private def localFileSystem: FileSystem = {
        FileSystem.get(
          new Path(inputFolder.getAbsolutePath).toUri,
          spark.sparkContext.hadoopConfiguration
        )
    }

    "validDictionarySizes" should "hold only the sizes that 7z can store" in {
        val sizes = MediawikiDumperSevenZipRecompressor.validDictionarySizes

        // 7z holds the dictionary size in one byte, which can express 2^n and
        // 3 * 2^(n-1) only.
        sizes should contain((4L * 1024 * 1024)) // 2^22, the v1 size and our default
        sizes should contain((64L * 1024 * 1024)) // 2^26
        sizes should contain((96L * 1024 * 1024)) // 3 * 2^25
        sizes should contain((128L * 1024 * 1024)) // 2^27
        sizes should contain((768L * 1024 * 1024)) // 3 * 2^28, the maximum

        // A round number of MiB is not automatically valid.
        sizes should not contain (100L * 1024 * 1024)
        sizes should not contain (200L * 1024 * 1024)

        sizes.max should equal(LZMA2Options.DICT_SIZE_MAX.toLong)
        sizes should equal(sizes.sorted)
    }

    "validate" should "reject a dictionary size that 7z cannot store" in {
        // Commons Compress does not check this. It writes a wrong property byte,
        // and other tools then read a wrong dictionary size.
        val thrown = intercept[IllegalArgumentException] {
            MediawikiDumperSevenZipRecompressor.validate(
              baseParams.copy(dictionarySizeMB = 100)
            )
        }
        thrown.getMessage should include("100 MiB")
        thrown.getMessage should include("128")
    }

    "validate" should "reject a dictionary size above the maximum" in {
        intercept[IllegalArgumentException] {
            MediawikiDumperSevenZipRecompressor.validate(
              baseParams.copy(dictionarySizeMB = 1024)
            )
        }
    }

    "validate" should "reject settings outside the LZMA2 ranges" in {
        intercept[IllegalArgumentException] {
            MediawikiDumperSevenZipRecompressor.validate(
              baseParams.copy(niceLength = 274)
            )
        }
        intercept[IllegalArgumentException] {
            MediawikiDumperSevenZipRecompressor.validate(
              baseParams.copy(niceLength = 7)
            )
        }
        intercept[IllegalArgumentException] {
            MediawikiDumperSevenZipRecompressor.validate(
              baseParams.copy(positionBits = 5)
            )
        }
        intercept[IllegalArgumentException] {
            MediawikiDumperSevenZipRecompressor.validate(
              baseParams.copy(positionBits = -1)
            )
        }
    }

    "validate" should "reject extensions that would overwrite the input" in {
        intercept[IllegalArgumentException] {
            MediawikiDumperSevenZipRecompressor.validate(
              baseParams
                  .copy(inputExtension = ".xml.7z", outputExtension = ".xml.7z")
            )
        }
        intercept[IllegalArgumentException] {
            MediawikiDumperSevenZipRecompressor.validate(
              baseParams.copy(outputExtension = ".xml.xz")
            )
        }
    }

    "validate" should "accept the defaults" in {
        MediawikiDumperSevenZipRecompressor.validate(
          MediawikiDumperSevenZipRecompressor.Params(
            inputFolder = "in",
            outputFolder = "out"
          )
        )
    }

    "the defaults" should "match the measured settings" in {
        // These values come from measured runs on simplewiki. See T437454.
        val defaults = MediawikiDumperSevenZipRecompressor.Params(
          inputFolder = "in",
          outputFolder = "out"
        )

        // 64 is the LZMA2 preset 9 value. 273 costs 1.66 times the time for 5% of size.
        defaults.niceLength should equal(64)
        // The read back costs 35% of the core time, so production does not pay it.
        defaults.verify shouldBe false
        // XML has no byte alignment pattern, so the LZMA2 default of 2 wastes bits.
        defaults.positionBits should equal(0)
        // 0 lets the encoder derive the depth from the nice length.
        defaults.depthLimit should equal(0)
        // A rerun must not redo a file that is already complete.
        defaults.skipExisting shouldBe true
        // The v1 dictionary size. A sweep to 128 MiB shows no knee, and 32 times the
        // dictionary gives only 8% less size. Keeping the v1 size holds the cluster cost
        // down, and keeps the memory a reader needs unchanged.
        defaults.dictionarySizeMB should equal(4)
        // 7z must be able to store the dictionary size in its one property byte.
        MediawikiDumperSevenZipRecompressor
            .validDictionarySizes should contain(defaults.dictionarySizeMB.toLong * 1024 * 1024)
    }

    "apply" should "write the same bytes whether or not it verifies" in {
        // Verification reads the archive back. It must not change the output.
        val xml = buildHistoryXML(1, 20)
        writeBzip2Input("simplewiki-2023-09-01-p1p1.xml.bz2", xml)

        MediawikiDumperSevenZipRecompressor(baseParams.copy(verify = true))
        val archive = new File(outputFolder, "simplewiki-2023-09-01-p1p1.xml.7z")
        val verifiedBytes = Files.readAllBytes(archive.toPath)

        deleteRecursively(outputFolder)
        MediawikiDumperSevenZipRecompressor(baseParams.copy(verify = false))

        Files.readAllBytes(archive.toPath) should equal(verifiedBytes)
    }

    "buildLZMA2Options" should "apply the parameters" in {
        val options = MediawikiDumperSevenZipRecompressor.buildLZMA2Options(
          baseParams.copy(
            dictionarySizeMB = 64,
            niceLength = 273,
            positionBits = 0
          )
        )

        options.getDictSize should equal(64 * 1024 * 1024)
        options.getNiceLen should equal(273)
        options.getPb should equal(0)
        options.getMatchFinder should equal(LZMA2Options.MF_BT4)
        options.getMode should equal(LZMA2Options.MODE_NORMAL)

        // The encoder allocates about 10.5 times the dictionary on the heap, for
        // every task that runs at the same time.
        val encoderMB = options.getEncoderMemoryUsage / 1024
        encoderMB should be > 500
        encoderMB should be < 800
    }

    "listInputFiles" should "ignore hidden files and other extensions" in {
        writeBzip2Input("simplewiki-2023-09-01-p2p4.xml.bz2", "<a/>")
        writeBzip2Input("simplewiki-2023-09-01-p1p1.xml.bz2", "<a/>")
        writeBzip2Input(".hidden.xml.bz2", "<a/>")
        writeBzip2Input("_SUCCESS.xml.bz2", "<a/>")
        new File(inputFolder, "SHA256SUMS").createNewFile()
        new File(inputFolder, "simplewiki-2023-09-01-p9p9.xml.zst")
            .createNewFile()

        val found = MediawikiDumperSevenZipRecompressor.listInputFiles(
          localFileSystem,
          new Path(inputFolder.getAbsolutePath),
          ".xml.bz2"
        )

        found.map(_.getPath.getName) should equal(
          Seq(
            "simplewiki-2023-09-01-p1p1.xml.bz2",
            "simplewiki-2023-09-01-p2p4.xml.bz2"
          )
        )
    }

    "buildWorkList" should "swap the extension and put the largest file first" in {
        // Different revision counts give different file sizes.
        writeBzip2Input(
          "simplewiki-2023-09-01-p1p1.xml.bz2",
          buildHistoryXML(1, 4)
        )
        writeBzip2Input(
          "simplewiki-2023-09-01-p2p4.xml.bz2",
          buildHistoryXML(2, 60)
        )

        val work = MediawikiDumperSevenZipRecompressor.buildWorkList(
          localFileSystem,
          new Path(inputFolder.getAbsolutePath),
          new Path(outputFolder.getAbsolutePath),
          baseParams
        )

        work should have size 2

        // Largest input first, so the slowest task starts before the fast ones.
        work.head._1 should endWith("simplewiki-2023-09-01-p2p4.xml.bz2")
        work.head._2 should endWith("simplewiki-2023-09-01-p2p4.xml.7z")
        work(1)._1 should endWith("simplewiki-2023-09-01-p1p1.xml.bz2")
        work(1)._2 should endWith("simplewiki-2023-09-01-p1p1.xml.7z")

        work.foreach { case (_, outputPath) =>
            outputPath should include(outputFolder.getAbsolutePath)
        }
    }

    "buildWorkList" should "skip a file whose output already exists" in {
        writeBzip2Input(
          "simplewiki-2023-09-01-p1p1.xml.bz2",
          buildHistoryXML(1, 4)
        )
        writeBzip2Input(
          "simplewiki-2023-09-01-p2p4.xml.bz2",
          buildHistoryXML(2, 4)
        )
        outputFolder.mkdirs()
        new File(outputFolder, "simplewiki-2023-09-01-p1p1.xml.7z")
            .createNewFile()

        val skipping = MediawikiDumperSevenZipRecompressor.buildWorkList(
          localFileSystem,
          new Path(inputFolder.getAbsolutePath),
          new Path(outputFolder.getAbsolutePath),
          baseParams
        )
        skipping.map(_._1) should have size 1
        skipping.head._1 should endWith("simplewiki-2023-09-01-p2p4.xml.bz2")

        val notSkipping = MediawikiDumperSevenZipRecompressor.buildWorkList(
          localFileSystem,
          new Path(inputFolder.getAbsolutePath),
          new Path(outputFolder.getAbsolutePath),
          baseParams.copy(skipExisting = false)
        )
        notSkipping should have size 2
    }

    "buildWorkList" should "fail when the input folder holds no dump file" in {
        val thrown = intercept[IllegalStateException] {
            MediawikiDumperSevenZipRecompressor.buildWorkList(
              localFileSystem,
              new Path(inputFolder.getAbsolutePath),
              new Path(outputFolder.getAbsolutePath),
              baseParams
            )
        }
        thrown.getMessage should include(".xml.bz2")
    }

    "localScratchDir" should "return a directory that exists" in {
        // LOCAL_DIRS is not set outside YARN, so this falls back to the temp
        // directory of the JVM.
        val dir = MediawikiDumperSevenZipRecompressor.localScratchDir(0)
        dir.isDirectory shouldBe true
    }

    "apply" should "recompress every bzip2 file as 7z with the same content" in {
        val expected = Map(
          "simplewiki-2023-09-01-p1p1" -> buildHistoryXML(1, 30),
          "simplewiki-2023-09-01-p2p4" -> buildHistoryXML(2, 12),
          "simplewiki-2023-09-01-p5r10r90" -> buildHistoryXML(5, 50)
        )

        expected.foreach { case (base, xml) =>
            writeBzip2Input(s"$base.xml.bz2", xml)
        }

        MediawikiDumperSevenZipRecompressor(baseParams)

        val produced = outputFolder.listFiles.filter(_.isFile)

        produced.map(_.getName).toSeq.sorted should equal(
          expected.keys.map(base => s"$base.xml.7z").toSeq.sorted
        )

        expected.foreach { case (base, xml) =>
            val archive = new File(outputFolder, s"$base.xml.7z")
            val (entryName, content, dictionarySize) = readSingleEntry(archive)

            // v1 pipes stdin into 7-Zip, which names the entry after the archive
            // with '.7z' removed. A reader of v2 must get the same name.
            entryName should equal(s"$base.xml")
            content should equal(xml)

            // The dictionary size survives the single 7z property byte. This
            // fails for any size outside the LZMA2 table.
            dictionarySize should equal(testDictionarySizeMB * 1024 * 1024)
        }
    }

    "apply" should "leave no staging file behind" in {
        writeBzip2Input(
          "simplewiki-2023-09-01-p1p1.xml.bz2",
          buildHistoryXML(1, 20)
        )

        MediawikiDumperSevenZipRecompressor(baseParams)

        val leftovers = outputFolder
            .listFiles
            .map(_.getName)
            .filter(name => name.startsWith(".") || name.endsWith(".inprogress"))

        leftovers shouldBe empty
    }

    "apply" should "do nothing on a rerun when the output already exists" in {
        writeBzip2Input(
          "simplewiki-2023-09-01-p1p1.xml.bz2",
          buildHistoryXML(1, 20)
        )

        MediawikiDumperSevenZipRecompressor(baseParams)

        val archive = new File(outputFolder, "simplewiki-2023-09-01-p1p1.xml.7z")
        val firstRunBytes = Files.readAllBytes(archive.toPath)
        val firstRunModified = archive.lastModified()

        MediawikiDumperSevenZipRecompressor(baseParams)

        archive.lastModified() should equal(firstRunModified)
        Files.readAllBytes(archive.toPath) should equal(firstRunBytes)
    }

    "apply" should "write a repeatable archive" in {
        // The entry carries no modification date, so the same input always gives
        // the same bytes. The SHA256SUMS manifest of the output folder needs this.
        val xml = buildHistoryXML(1, 20)
        writeBzip2Input("simplewiki-2023-09-01-p1p1.xml.bz2", xml)

        MediawikiDumperSevenZipRecompressor(baseParams)
        val archive = new File(outputFolder, "simplewiki-2023-09-01-p1p1.xml.7z")
        val firstRunBytes = Files.readAllBytes(archive.toPath)

        deleteRecursively(outputFolder)
        MediawikiDumperSevenZipRecompressor(baseParams)

        Files.readAllBytes(archive.toPath) should equal(firstRunBytes)
    }

    "apply" should "actually compress the content" in {
        // MediaWiki history XML repeats the page text in every revision, so LZMA2
        // must reach a high ratio on it. This guards against a settings mistake
        // that writes the entry in store mode.
        //
        // The test does not compare the 7z size against the bz2 size. On a sample
        // this small, bzip2 holds the whole file in one 900 KB block, so the
        // comparison says nothing about production files, which are ~460 MB.
        val name = "simplewiki-2023-09-01-p1p1"
        val xml = buildHistoryXML(1, 200)
        writeBzip2Input(s"$name.xml.bz2", xml)

        MediawikiDumperSevenZipRecompressor(baseParams)

        val xmlSize = xml.getBytes(StandardCharsets.UTF_8).length
        val sevenZipSize = new File(outputFolder, s"$name.xml.7z").length

        sevenZipSize should be < (xmlSize / 10).toLong
    }

    "apply" should "honour the input and output extensions" in {
        writeBzip2Input("simplewiki-2023-09-01-p1p1.xml.bz2", buildHistoryXML(1, 8))

        MediawikiDumperSevenZipRecompressor(
          baseParams.copy(outputExtension = ".history.7z")
        )

        val produced = outputFolder.listFiles.filter(_.isFile).map(_.getName)
        produced should equal(Array("simplewiki-2023-09-01-p1p1.history.7z"))

        val (entryName, _, _) = readSingleEntry(
          new File(outputFolder, "simplewiki-2023-09-01-p1p1.history.7z")
        )
        entryName should equal("simplewiki-2023-09-01-p1p1.history")
    }
}
