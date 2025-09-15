package org.wikimedia.analytics.refinery.job.mediawikidumper

import java.io._
import java.nio.file.{Files, StandardCopyOption}

import scala.language.postfixOps

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.job.mediawikidumper.MediawikiDumperSpec.assertSinglePartitionForRevision
import org.wikimedia.analytics.refinery.tools.LogHelper

// -Dsuites="org.wikimedia.analytics.refinery.job.mediawikidumper.MediawikiDumperSpec"
class MediawikiDumperSpec
    extends FlatSpec
    with Matchers
    with BeforeAndAfterEach
    with DataFrameSuiteBase
    with LogHelper {

    val tmpDir: File = {
        Files
            .createTempDirectory(
              s"mediawikidumper_test_db_${System.currentTimeMillis / 1000}"
            )
            .toFile
    }

    val outputFolder: Path = new Path(s"${tmpDir.getAbsolutePath}/output")

    val testResourcesDir: String = {
        if (System.getProperty("user.dir").contains("refinery-job")) {
            "src/test/resources/mediawikidumper"
        } else {
            // If we are not running from refinery-job, we need to add the refinery-job prefix to the path
            // in order to find the test resources.
            "refinery-job/src/test/resources/mediawikidumper"
        }
    }

    val sourceDBName: String = "wmf_dumps"
    val sourceTableName: String = s"$sourceDBName.wikitext_raw_rc1"
    val namespaceTableName: String = {
        s"$sourceDBName.mediawiki_project_namespace_map"
    }
    val snapshot: String = "2023-09"

    def createBaseTables(): Unit = {
        spark.sql(
          s"CREATE DATABASE IF NOT EXISTS $sourceDBName LOCATION '${tmpDir.getAbsolutePath}';"
        )
        spark
            .read
            .schema(wikitextSchema)
            .option("header", "true")
            .option("inferSchema", "false")
            .option("compression", "gzip")
            .json(
              s"${testResourcesDir}/wmf_content_mediawiki_content_history_v1.json.gz"
            )
            .write
            .option("compression", "none")
            .saveAsTable(sourceTableName)

        spark
            .read
            .schema(namespacesSchema)
            .option("header", "true")
            .option("inferSchema", "false")
            .option("compression", "gzip")
            .json(
              s"${testResourcesDir}/wmf_raw_mediawiki_project_namespace_map.json.gz"
            )
            .write
            .option("compression", "none")
            .saveAsTable(namespaceTableName)
    }

    val wikitextSchema = {
        new StructType(
          Array(
            StructField("page_id", LongType),
            StructField("page_namespace_id", LongType),
            StructField("page_title", StringType),
            StructField("user_id", LongType),
            StructField("user_text", StringType),
            StructField("user_is_visible", BooleanType),
            StructField("revision_id", LongType),
            StructField("revision_parent_id", LongType),
            StructField("revision_dt", TimestampType),
            StructField("revision_comment", StringType),
            StructField("revision_comment_is_visible", BooleanType),
            StructField("revision_sha1", StringType),
            StructField("revision_size", LongType),
            StructField("revision_is_minor_edit", BooleanType),
            StructField(
              "revision_content_slots",
              MapType(
                StringType,
                new StructType(
                  Array(
                    StructField("content_body", StringType),
                    StructField("content_format", StringType),
                    StructField("content_model", StringType),
                    StructField("content_sha1", StringType),
                    StructField("content_size", LongType)
                  )
                )
              )
            ),
            StructField("revision_content_is_visible", BooleanType),
            StructField("wiki_id", StringType)
          )
        )
    }

    val namespacesSchema = {
        new StructType(
          Array(
            StructField("hostname", StringType),
            StructField("language", StringType),
            StructField("sitename", StringType),
            StructField("dbname", StringType),
            StructField("home_page", StringType),
            StructField("mw_version", StringType),
            StructField("case_setting", StringType),
            StructField("namespace", IntegerType),
            StructField("namespace_canonical_name", StringType),
            StructField("namespace_localized_name", StringType),
            StructField("namespace_case_setting", StringType),
            StructField("namespace_is_content", IntegerType),
            StructField("snapshot", StringType)
          )
        )
    }

    def dropTables(): Unit = {
        spark.sql(s"""DROP TABLE IF EXISTS $sourceTableName;""")
        spark.sql(s"""DROP TABLE IF EXISTS $namespaceTableName;""")
        spark.sql(s"""DROP DATABASE IF EXISTS $sourceDBName;""")
    }

    override def beforeEach(): Unit = createBaseTables()

    override def afterEach(): Unit = dropTables()

    val params: MediawikiDumper.Params = MediawikiDumper.Params(
      maxPartitionSizeMB = 1,
      sourceTable = sourceTableName,
      outputFolder = outputFolder.toString,
      // Note: update those values to match the input data from resources, after change to input file.
      publishUntil = "2023-09-01",
      wikiId = "simplewiki",
      namespacesTable = namespaceTableName,
      namespacesSnapshot = snapshot
    )

    // Setup the baseDF through a function in order to use the same Spark Session as in the test beforeEach.
    def baseDF: DataFrame = MediawikiDumper.buildBaseRevisionsDF(params)

    def fakeSize(df: DataFrame): DataFrame = {
        df.withColumn(
          "size",
          // revision 360821
          when(
            col("timestamp").equalTo(
              lit("2007-03-27T11:27:44.000Z").cast(DataTypes.TimestampType)
            ),
            params.maxPartitionSizeMB * 1024 * 1024 + 1
          ).otherwise(col("size"))
        )
    }

    "buildBaseRevisionsDF" should "create a valid dataframe" in {
        val df = baseDF
        df.count should equal(33)
        val pageIds = df.select("pageId").collect
        pageIds.distinct.length should equal(2)
    }

    private val maxPartitionSize: Long = MediawikiDumper
        .calculateMaxPartitionSize(params)

    "createPartitioner" should "create a single partition partitioner" in {
        val partitioner: RangeLookupPartitioner[RowKey, Long] = MediawikiDumper
            .createPartitioner(baseDF, maxPartitionSize)
        partitioner.numPartitions should equal(1)
        partitioner.indexOfRange(45046) should equal(Some(0))
    }

    "createPartitioner" should "create a size-aware partitioner" in {
        val partitioner = MediawikiDumper.createPartitioner(
          fakeSize(MediawikiDumper.readPartitioningData(params)),
          maxPartitionSize
        )

        // Revision 360821 has predecessors and successors,
        // so page 45046 needs three partitions: [predecessors:_*, 360821, successors:_*].
        // The last partition is for the remaining page 279900.
        partitioner.numPartitions should equal(4)
        val partitionedDF = MediawikiDumper
            .partitionByXMLFileBoundaries(baseDF, partitioner)
        partitionedDF.rdd.getNumPartitions should equal(4)
        partitionedDF
            .rdd
            .foreachPartition(assertSinglePartitionForRevision(360821))

        partitionedDF.count() should equal(33)
    }

    def fragmentsWithPageRange(
        overrideBaseDF: DataFrame = baseDF,
        params: MediawikiDumper.Params = params,
    ): Dataset[Row] = {
        val partitioner: RangeLookupPartitioner[RowKey, Long] = MediawikiDumper
            .createPartitioner(overrideBaseDF, maxPartitionSize)
        MediawikiDumper.buildXMLFragmentChunks(
          MediawikiDumper
              .partitionByXMLFileBoundaries(overrideBaseDF, partitioner),
          MediawikiDumper.getSiteInfo(params),
          params
        )
    }

    "buildXMLFragments" should "build all fragments" in {
        val df = fragmentsWithPageRange()
        df.first.getAs[String]("partition_id") should equal("p45046p279900")
        df.count should equal(2)
    }

    "buildXMLFragments" should "add revision range if applicable" in {
        val df = fragmentsWithPageRange(fakeSize(baseDF))
        df.first.getAs[String]("partition_id") should
            equal("p45046r266092r266092")
        df.count should equal(8)
    }

    private def getStringFromFilePath(path: String): String = {
        if (path.endsWith(".xml")) {
            val source = scala.io.Source.fromFile(path)
            try source.mkString
            finally source.close
        } else {
            getStringFromCompressedFile(path)
        }
    }

    private def getStringFromCompressedFile(path: String) = {
        // Create input streams for reading the compressed file
        val extension = path.split("\\.").last
        val inputStream = extension match {
            case "bz2" =>
              new BZip2CompressorInputStream(new FileInputStream(path))
            case "gz" =>
              new GzipCompressorInputStream(new FileInputStream(path))
            case "zst" =>
              new ZstdCompressorInputStream(new FileInputStream(path))
            case _ =>
              throw new IOException(s"Unsupported compression format: $extension")
        }
        val reader = {
            new BufferedReader(new InputStreamReader(inputStream, "UTF-8"))
        }

        // Read the decompressed content into a single string
        val stringBuilder = new StringBuilder
        try {
            var line = reader.readLine
            while (line != null) {
                stringBuilder.append(line).append("\n")
                line = reader.readLine
            }
        } finally {
            // Close the streams
            reader.close()
            inputStream.close()
        }

        stringBuilder.toString
    }

    private def writeXML(
        params: MediawikiDumper.Params,
        overrideFragmentsWithPageRange: DataFrame = fragmentsWithPageRange()
    ): Unit = {
        MediawikiDumper.writeXMLFiles(overrideFragmentsWithPageRange, params)
    }

    private def fetchOrRegenerateReferenceFileContent(outputFileFromCaller: File): String = {
      val referenceFile: File = {
        new File(s"$testResourcesDir/MediawikiDumperOutputTest.xml")
      }

      // Optionally, regenerate the reference file. In this case the test will always pass.
      // Run your test with env var REGENERATE_FIXTURES=true to regenerate the reference file
      val regenerateReferenceFileVar: String = sys
        .env
        .getOrElse("REGENERATE_FIXTURES", "")
      if (regenerateReferenceFileVar == "true") {
        log.info(s"Regenerating fixture file: ${referenceFile.toString}")
        Files.copy(
          outputFileFromCaller.toPath,
          referenceFile.toPath,
          StandardCopyOption.REPLACE_EXISTING
        )
      }

      val reference: String = getStringFromFilePath(referenceFile.toString)

      reference
    }

    "writeXMlFiles" should "build a BZIP2 compressed XML file" in {
        val p: MediawikiDumper.Params = params.copy(compressionAlgorithm = "bzip2")
        writeXML(p, fragmentsWithPageRange(params = p))

        val outputPartitionDir = new File(s"${outputFolder.toString}/")
        val compressedOutputFile = {
            new File(
              outputPartitionDir
                  .listFiles
                  .filter(_.isFile)
                  .filter(_.toString.matches(".*\\.xml.bz2$"))
                  .toList
                  .head
                  .toString
            )
        }

        compressedOutputFile.getName should
            equal(
              "simplewiki-2023-09-01-p45046p279900-pages-meta-history.xml.bz2"
            )

        val output: String = {
            try getStringFromFilePath(compressedOutputFile.toString)
            catch {
                case e: IOException =>
                    throw new IOException(
                      s"Failed to read $compressedOutputFile",
                      e
                    )
            }
        }

        val reference = fetchOrRegenerateReferenceFileContent(compressedOutputFile)

        output should equal(reference)
    }

    "writeXMlFiles" should "build a ZSTD compressed XML file" in {
      val p: MediawikiDumper.Params = params.copy(compressionAlgorithm = "zstd")
      writeXML(p, fragmentsWithPageRange(params = p))

      val outputPartitionDir = new File(s"${outputFolder.toString}/")
      val compressedOutputFile = {
        new File(
          outputPartitionDir
            .listFiles
            .filter(_.isFile)
            .filter(_.toString.matches(".*\\.xml.zst$"))
            .toList
            .head
            .toString
        )
      }

      compressedOutputFile.getName should
        equal(
          "simplewiki-2023-09-01-p45046p279900-pages-meta-history.xml.zst"
        )

      val output: String = {
        try getStringFromFilePath(compressedOutputFile.toString)
        catch {
          case e: IOException =>
            throw new IOException(
              s"Failed to read $compressedOutputFile",
              e
            )
        }
      }

      val reference = fetchOrRegenerateReferenceFileContent(compressedOutputFile)

      output should equal(reference)
    }

  "writeXMlFiles" should "build a GZIP compressed XML file" in {
    val p: MediawikiDumper.Params = params.copy(compressionAlgorithm = "gzip")
    writeXML(p, fragmentsWithPageRange(params = p))

    val outputPartitionDir = new File(s"${outputFolder.toString}/")
    val compressedOutputFile = {
      new File(
        outputPartitionDir
          .listFiles
          .filter(_.isFile)
          .filter(_.toString.matches(".*\\.xml.gz$"))
          .toList
          .head
          .toString
      )
    }

    compressedOutputFile.getName should
      equal(
        "simplewiki-2023-09-01-p45046p279900-pages-meta-history.xml.gz"
      )

    val output: String = {
      try getStringFromFilePath(compressedOutputFile.toString)
      catch {
        case e: IOException =>
          throw new IOException(
            s"Failed to read $compressedOutputFile",
            e
          )
      }
    }

    val reference = fetchOrRegenerateReferenceFileContent(compressedOutputFile)

    output should equal(reference)
  }

  "writeXMlFiles" should "build an uncompressed XML file" in {
    val p: MediawikiDumper.Params = params.copy(compressionAlgorithm = "none")
    writeXML(p, fragmentsWithPageRange(params = p))

    val outputPartitionDir = new File(s"${outputFolder.toString}/")
    val compressedOutputFile = {
      new File(
        outputPartitionDir
          .listFiles
          .filter(_.isFile)
          .filter(_.toString.matches(".*\\.xml$"))
          .toList
          .head
          .toString
      )
    }

    compressedOutputFile.getName should
      equal(
        "simplewiki-2023-09-01-p45046p279900-pages-meta-history.xml"
      )

    val output: String = {
      try getStringFromFilePath(compressedOutputFile.toString)
      catch {
        case e: IOException =>
          throw new IOException(
            s"Failed to read $compressedOutputFile",
            e
          )
      }
    }

    val reference = fetchOrRegenerateReferenceFileContent(compressedOutputFile)

    output should equal(reference)
  }

}

object MediawikiDumperSpec {
    private def assertSinglePartitionForRevision(
        revisionId: Int
    ): Iterator[Revision] => Unit = { revisions =>
        val size = revisions.size
        if (revisions.exists(revision => revision.revisionId == revisionId)) {
            assert(
              size == 1,
              message = s"Unexpected number of revisions $size in partition"
            )
        }
    }
}
