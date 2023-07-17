package org.wikimedia.analytics.refinery.job.mediawikidumper

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{BooleanType, LongType, MapType, StringType, StructField, StructType, TimestampType}

import scala.io.Source
import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.tools.LogHelper

import scala.language.postfixOps

class MediawikiDumperSpec
    extends FlatSpec
    with Matchers
    with BeforeAndAfterEach
    with DataFrameSuiteBase
    with LogHelper {

    val tmpDir: File = Files.createTempDirectory(
        s"mediawikidumper_test_db_${System.currentTimeMillis / 1000}"
    ).toFile

    val outputFolder: Path = new Path(s"${tmpDir.getAbsolutePath}/output")

    val testResourcesDir: String = if (System.getProperty("user.dir").contains("refinery-job")) {
        "src/test/resources/mediawikidumper"
    } else {
        // If we are not running from refinery-job, we need to add the refinery-job prefix to the path
        // in order to find the test resources.
        "refinery-job/src/test/resources/mediawikidumper"
    }

    val sourceDBName: String = "wmf_dumps"
    val sourceTableName: String = s"$sourceDBName.wikitext_raw_rc1"

    def createBaseTable(): Unit = {
        spark.sql(s"CREATE DATABASE IF NOT EXISTS $sourceDBName LOCATION '${tmpDir.getAbsolutePath}';")
        spark.read
            .schema(wikitextSchema)
            .option("header", "true")
            .option("inferSchema", "false")
            .option("compression", "gzip")
            .json(s"${testResourcesDir}/${sourceTableName.replace(".", "_")}.json.gz")
            .write
            .option("compression", "none")
            .saveAsTable(sourceTableName)
    }

    val wikitextSchema = new StructType(Array(
        StructField("page_id", LongType),
        StructField("page_namespace", LongType),
        StructField("page_title", StringType),
        StructField("user_id", LongType),
        StructField("user_text", StringType),
        StructField("user_is_visible", BooleanType),
        StructField("revision_id", LongType),
        StructField("revision_parent_id", LongType),
        StructField("revision_timestamp", TimestampType),
        StructField("revision_comment", StringType),
        StructField("revision_comment_is_visible", BooleanType),
        StructField("revision_sha1", StringType),
        StructField("revision_size", LongType),
        StructField("revision_is_minor_edit", BooleanType),
        StructField("revision_content_slots", MapType(StringType, new StructType(Array(
            StructField("content_body", StringType),
            StructField("content_format", StringType),
            StructField("content_model", StringType),
            StructField("content_sha1", StringType),
            StructField("content_size", LongType)
        )))),
        StructField("revision_content_is_visible", BooleanType),
        StructField("wiki_db", StringType)
    ))

    def dropTable(): Unit = {
        spark.sql(s"""DROP TABLE IF EXISTS $sourceTableName;""")
        spark.sql(s"""DROP DATABASE IF EXISTS $sourceDBName;""")
    }

    override def beforeEach(): Unit = createBaseTable()

    override def afterEach(): Unit = dropTable()

    val params: MediawikiDumper.Params = MediawikiDumper.Params(
        sourceTable = sourceTableName,
        outputFolder = outputFolder.toString,
        // Note: update those values to match the input data from resources, after change to input file.
        publishUntil = "2023-09-01",
        wikiId = "simplewiki",
        outputFilesCompression = "uncompressed"
    )

    // Setup the baseDF through a function in order to use the same Spark Session as in the test beforeEach.
    def baseDF: DataFrame = MediawikiDumper.buildBaseRevisionsDF(params)

    // Same comment as above
    def partitionsDefiner(baseDF: DataFrame): PagesPartitionsDefiner = new PagesPartitionsDefiner(
        spark,
        baseDF,
        params.pageSizeOverhead,
        params.maxTargetFileSize,
        params.minChunkSize
    )

    "buildBaseRevisionsDF" should "create a valid dataframe" in {
        val df = baseDF
        df.count() should equal(33)
        val pageIds = df.select("pageId").rdd.collect()
        pageIds.distinct.length should equal(2)
    }

    "sortedAndPartitionedRevisions" should "rearrange and complete the revisions with partition information" in {
        val df = baseDF
        val rdd: RDD[Revision] = MediawikiDumper.sortedAndPartitionedRevisions(df, partitionsDefiner(df))
        rdd.count() should equal(33)
        // This size changes when data is correct (the uncommented line is currently based on bad data)
        // rdd.first().pagesPartition.get should equal(PagesPartition(681371,45046,279900,Some(0)))  // See json file for ids
        rdd.first().pagesPartition.get should equal(PagesPartition(681027,45046,279900,Some(0)))  // See json file for ids
    }

    def fragmentsRDD: RDD[XMLProducer] = {
        val df = baseDF
        val revisionsRDD: RDD[Revision] = MediawikiDumper.sortedAndPartitionedRevisions(df, partitionsDefiner(baseDF))
        MediawikiDumper.buildXMLFragments(revisionsRDD, params)
    }

    "buildXMLFragments" should "build all fragements" in {
        val rdd: RDD[XMLProducer] = fragmentsRDD
        rdd.count() should equal(39)
        rdd.first().pagesPartition should not be empty
        rdd.getNumPartitions should equal(1)
    }

    def getStringFromFilePath(path: String): String = {
        val source = Source.fromFile(path)
        val fileContents = source.getLines.mkString
        source.close()
        fileContents
    }

    def writeXML(): Unit = {
        val rdd: RDD[XMLProducer] = fragmentsRDD
        MediawikiDumper.writeXMLFiles(rdd, params)
    }

    "writeXMlFiles" should "build an XML file" in {
        writeXML()

        val outputPartitionDir = new File(s"${outputFolder.toString}/startPageId=45046/endPageId=279900/")
        val outputTxtFile = new File(
            outputPartitionDir.listFiles.filter(_.isFile).
                filter(_.toString.matches(".*\\.txt$")).toList.head.toString
        )
        val output: String = getStringFromFilePath(outputTxtFile.toString)

        val referenceFile: File = new File(s"$testResourcesDir/MediawikiDumperOutputTest.xml")

        // Optionally, regenerate the reference file, in this case the test will always pass
        // Run your test with env vor REGENERATE_FIXTURES=true to regenerate the reference file
        val regenerateReferenceFileVar: String = sys.env.getOrElse("REGENERATE_FIXTURES", "")
        if (regenerateReferenceFileVar == "true") {
            log.info(s"Regenerating fixture file: ${referenceFile.toString}")
            Files.copy(outputTxtFile.toPath, referenceFile.toPath, StandardCopyOption.REPLACE_EXISTING)
        }

        val reference: String = getStringFromFilePath(referenceFile.toString)

        output should equal(reference)
    }

    "renameXMLFiles" should "Rename the output with a flatter naming" in {
        writeXML()
        MediawikiDumper.renameXMLFiles(params)
        val outputDir = new File(s"${outputFolder.toString}")
        outputDir.listFiles.map(file => new Path(file.toString).getName).toList should contain (
            "simplewiki-2023-09-01-45046-279900-pages-meta-history.xml"
        )
    }
}
