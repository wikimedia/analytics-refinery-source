package org.wikimedia.analytics.refinery.job.mediawikidumper

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, element_at}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.wikimedia.analytics.refinery.job.HDFSArchiver
import org.wikimedia.analytics.refinery.tools.LogHelper
import scopt.OptionParser

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZonedDateTime}
import java.util.TimeZone
import scala.collection.mutable.ListBuffer


/**
 * Job to dump the content of a wiki at a given date.
 */
object MediawikiDumper extends LogHelper{

    lazy val spark: SparkSession = { SparkSession.builder.getOrCreate }

    import spark.implicits._  // Used when converting Rows to case classes.

    /**
     * Main class entry point
     *
     * @param params parsed Params including the configuration of the source and the target.
     * @return Unit
     */
    def apply(params: Params): Unit = {
        // necessary so timezones don't go local
        spark.conf.set("spark.sql.session.timeZone", "UTC")

        log.info("Mediawiki Dumper")
        log.info(s"Mediawiki Dumper: About to dump ${params.wikiId} at ${params.publishUntil}")
        log.info(s"Mediawiki Dumper: from ${params.sourceTable} to ${params.outputFolder}")

        log.info(s"Mediawiki Dumper: step 1/5: build base revisions dataframe")
        val revisionsDF: DataFrame = buildBaseRevisionsDF(params)

        // NOTE: the following would cause a lot of work for bigger wikis with many TB of content
        // revisionsDF.cache()
        // log.info(s"Mediawiki Dumper:   ${revisionsDF.count()} revisions selected.")

        log.info("Mediawiki Dumper: step 2/5: define page partitions")
        val partitionsDefiner = new PagesPartitionsDefiner(
            spark,
            revisionsDF,
            params.pageSizeOverhead,
            params.maxTargetFileSize,
            params.minChunkSize,
            log = Some(log)
        )
        log.info(s"Mediawiki Dumper:   ${partitionsDefiner.pagesPartitions.length} partitions defined.")

        log.info("Mediawiki Dumper: step 3/5: sort and partition revisions")
        val revisionsRDD = sortedAndPartitionedRevisions(revisionsDF, partitionsDefiner)

        log.info("Mediawiki Dumper: step 4/5: build XML fragments RDD from sorted revisions")
        val fragments: RDD[XMLProducer] = buildXMLFragments(revisionsRDD, params)
        log.info(s"Mediawiki Dumper:   XML fragments generated.")

        log.info("Mediawiki Dumper: step 5/5: Write XML files and rename them")
        writeXMLFiles(fragments, params)
        renameXMLFiles(params)
        log.info(s"Mediawiki Dumper: Done. Output in ${params.outputFolder}")
    }

  /**
   * Build the base revisions dataframe.
   * @param params the job parameters
   * @return A dataframe containing the revisions from the source table
   */
  def buildBaseRevisionsDF(params: Params): DataFrame = {
    // NOTE: the stuff left to do below is followed by code that, if used, will make the XML output
    //   match the MediawikiDumperOutputTest.Simplewiki.Sample.xml perfectly
    //   This approach can be used to work on and fix any data problems, but ultimately this query
    //   should look like it does now, with the comments and to do lines removed.
    spark.sql(
      s"""|SELECT *
          |FROM ${params.sourceTable}
          |WHERE revision_timestamp < TIMESTAMP '${params.publishUntil}'
          |  AND wiki_db = '${params.wikiId}'
          |ORDER BY page_id ASC,
          |  revision_id ASC;""".stripMargin)
      // TODO: enable all slots by adding the whole map to Revision
      .withColumn("content_slot", element_at(col("revision_content_slots"), "main"))
      // In the next select list, some NULL columns are added to fill the case classes.
      .selectExpr("page_id as pageId",
        "page_namespace as ns",
        "page_title as title",
        "user_text as contributor",
        // TODO: once source data is updated to have null user_ids for IP editors
        // "if(user_id = -1, null, user_id) as contributorId",
        "user_id as contributorId",
        // TODO: example: simplewiki revision id 360821 has a redacted user but is not showing up as not visible here
        // TODO: data: revision id 1714215 is not redacting the contributor
        // "revision_id = 1714215 OR (user_is_visible AND NOT (user_id = -1 AND coalesce(user_text, '') = '')) as isEditorVisible",
        "user_is_visible as isEditorVisible",
        "revision_id as revisionId",
        // XSD mandates this timestamp format (timestamps are in UTC by spark.conf above)
        s"""date_format(
              to_utc_timestamp(revision_timestamp, '${TimeZone.getDefault.getID}'),
              "yyyy-MM-dd'T'HH:mm:ss'Z'"
           ) as timestamp
        """,
        "revision_comment as comment",
        // TODO: figure out why this data is wrong
        // "revision_id <> 1714215 AND (revision_id = 360821 OR revision_comment_is_visible) as isCommentVisible",
        "revision_comment_is_visible as isCommentVisible",
        "revision_content_is_visible as isContentVisible",
        // TODO: figure out why this is sometimes -1 and decide if to make it zero or null
        // "if(revision_parent_id = -1, null, revision_parent_id) as parentId",
        "revision_parent_id as parentId",
        "revision_is_minor_edit as isMinor",
        "content_slot.content_model as model",
        "content_slot.content_format as format",
        "content_slot.content_body as text",
        // TODO: size seems wrong when importing deleted content
        // "case" +
        // " when revision_id = 1714215 then 26" +
        // " when revision_id = 4997170 then 60" +
        // " when revision_id = 4670942 then 93" +
        // " when revision_id = 4670944 then 165" +
        // " else content_slot.content_size" +
        // " end as size",
        "content_slot.content_size as size",
        "content_slot.content_sha1 as sha1")
  }

  /**
   * Sort and partition the revisions.
   * @param revisionDF the base dataframe containing the revisions
   * @param definer the partition definer, containing the partitioning information
   * @return an RDD of revisions, sorted and partitioned
   */
  def sortedAndPartitionedRevisions(revisionDF: DataFrame, definer: PagesPartitionsDefiner): RDD[Revision] = {
    val partitioner: ByPagesPartitionKeyPartitioner = new ByPagesPartitionKeyPartitioner(
            definer.pagesPartitions.length
        )
        val broadcastDefiner = spark.sparkContext.broadcast(definer)
        revisionDF
            .withColumn("pagesPartition", PagesPartition.emptyPagesPartitionColumn)
            .as[Revision]
            .map(revision => {
                // Add the partition information to the revisions
                val pagePartition = broadcastDefiner.value.getPagesPartition(revision.pageId)
                val revisionWithPartitionInfo = revision.addPartitionInfo(pagePartition)
                // prepare the data structure for the partitioner
                (revisionWithPartitionInfo.pagesPartitionKey, revisionWithPartitionInfo)
            })
            .rdd  // Convert to RDD to use custom partitioning
            .repartitionAndSortWithinPartitions(partitioner)  // Custom partitioning
            .map(_._2) // Take only the value, the Revision, from the partitioned datastructures.
    }

    /**
     * Build the XML fragments from the sorted and partitioned revisions.
     * The XML fragments mainly are the XML representation of the revisions, with the page header and footer.
     * @param sortedRevisions the sorted and partitioned revisions
     * @param params the job parameters
     * @return An RDD of XML fragments
     */
    def buildXMLFragments(sortedRevisions: RDD[Revision], params: Params): RDD[XMLProducer] = {
        val siteInfoDF = spark.sql(
            f"""|
                | select language as languageCode,
                |        sitename as siteName,
                |        dbname as dbName,
                |        home_page as homePage,
                |        mw_version as mediaWikiVersion,
                |        case_setting as caseSetting,
                |        collect_list(named_struct(
                |          'code', namespace,
                |          'name', namespace_localized_name,
                |          'caseSetting', namespace_case_setting,
                |          'isContent', namespace_is_content = 1
                |        )) as namespaces
                |   from ${params.namespacesTable}
                |  where snapshot = '${params.namespacesSnapshot}'
                |    and dbname = '${params.wikiId}'
                |  group by language, sitename, dbname, home_page, mw_version, case_setting
                |;""".stripMargin)

        val test = spark.sql(f"select * from ${params.namespacesTable} where dbname = 'simplewiki'")
        val out = test.map(x => x.mkString(" ")).collect().mkString(" >>> ")
        val si = siteInfoDF.as[SiteInfo]
        si.count()
        val siteInfo = si.first() // there should only ever be one row with the group by

        sortedRevisions
            .mapPartitions(revisions => {
                var currentPageId = 0L
                var currentPartition: Option[PagesPartition] = None
                var currentPage: Option[Page] = None
                val revisionAndPageFragments: List[XMLProducer] = revisions
                    .flatMap(revision => {
                        if (currentPartition.isEmpty) currentPartition = revision.pagesPartition  // Set it once per partition
                        val result = ListBuffer[XMLProducer]()
                        if (revision.pageId != currentPageId) { // We arrive to a new page, or first revision of partition
                            if (currentPageId != 0L) result.append(XMLFragment.pageFooter(currentPartition.get))
                            if (currentPage.isEmpty || revision.pageId != currentPage.get.pageId) {
                                currentPage = Some(revision.buildPage)
                            }
                            result.append(currentPage.get)
                            currentPageId = revision.pageId
                        }
                        result.append(revision)
                        if (!revisions.hasNext) result.append(XMLFragment.pageFooter(currentPartition.get))
                        result.toList
                    }).toList
                // Add the XML header and footer to the partition
                Iterator(XMLFragment.xmlHeader(currentPartition.get, siteInfo)) ++
                    revisionAndPageFragments.toIterator ++
                    Iterator(XMLFragment.xmlFooter(currentPartition.get))
            })
    }

    /**
     * Write the dataframe as XML files.
     * 1 file is generated by partition by design.
     * In order to add the page ranges into each file name, we write the dataframe into an Hive directory structure
     * with the pages ranges as directories (startPageId=123/endPageId=456/0_000000.xml.gz).
     * Then we rename the files to put them into a single directory.
     * @param fragments the XML fragments
     * @param params the job parameters
     */
    def writeXMLFiles(fragments: RDD[XMLProducer], params: Params): Unit = {
        val xmlByPageRangeRDD = fragments.map(producer => (
            producer.getPartitionStartPageId,
            producer.getPartitionEndPageId,
            producer.getXML
        ))
        val columns = Seq("startPageId", "endPageId", "xml")
        spark
            .createDataFrame(xmlByPageRangeRDD)
            .toDF(columns: _*)
            .write
            .mode(SaveMode.Overwrite)
            // Then each task will write each pair (startPageId, endPageId) into
            // their own file. Given the previous partitioning, this ensures
            // one single file per (startPageId, endPageId) pair.
            .partitionBy("startPageId", "endPageId")
            .option("compression", params.outputFilesCompression)
            // The Spark Text writer request the dataframe to contain a single column in the schema.
            // The call to `partitionBy` from the writer is actually subtracting the 2 columns from the schema.
            // So, only the xml column remains, and it respects the requirement.
            .text(params.outputFolder)
    }

    private val compressionExtensions: Map[String, String] = Map(
        "none" -> "",
        "uncompressed" -> "",
        "gzip" -> ".gz",
        "bzip2" -> ".bz2",
        "lz4" -> ".lz4",
        "snappy" -> ".snappy"
    )

    /**
     * Moves the generated XML files.
     *
     * The repartitioning process outputs files in an "ugly" directory tree.
     * Folders use Hive syntax (/startPageId=123/endPageId=456/0_000000.xml.gz).
     * This method rename files to put all of them into a single directory with pretty naming
     * eg brwiki-20230901-pages-meta-history.xml.gz .
     * @param params the job parameters
     */
    def renameXMLFiles(params: Params): Unit = {
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val startPageIdDirs = fs.listStatus(new Path(params.outputFolder))
        val compressionExtension = compressionExtensions.getOrElse(params.outputFilesCompression, "")
        startPageIdDirs.foreach { startPageIdDir =>
            if (startPageIdDir.getPath.getName != "_SUCCESS") {
                // The substring removes Hive partition prefix (startPageId=).
                val startPageId = startPageIdDir.getPath.getName.substring(12)
                val endPageIdDirs = fs.listStatus(startPageIdDir.getPath)
                endPageIdDirs.foreach { endPageIdDir =>
                    // The substring removes Hive partition prefix (endPageId=).
                    val endPageId = endPageIdDir.getPath.getName.substring(10)
                    val destinationFile = s"${params.wikiId}-${params.publishUntil}-$startPageId-$endPageId-pages-meta-history.xml$compressionExtension"
                    val destinationPath = new Path(s"${params.outputFolder}/$destinationFile")
                    HDFSArchiver(
                        sourceDirectory = endPageIdDir.getPath,
                        expectedFilenameEnding = s".txt$compressionExtension",
                        checkDone = false,
                        doneFilePath = new Path("dummy"),
                        archiveFile = destinationPath,
                        archiveParentUmask = "022",
                        archivePerms = "644"
                    )
                }
                fs.delete(startPageIdDir.getPath, true)
            }
        }
    }

    case class Params(
        wikiId: String = "",
        publishUntil: String = "",
        sourceTable: String = "wmf_dumps.wikitext_raw_rc1",
        namespacesTable: String = "wmf_raw.mediawiki_project_namespace_map",
        namespacesSnapshot: String = "2023-09",
        outputFolder: String = "",
        pageSizeOverhead: Integer = 20, // in kB
        maxTargetFileSize: Integer = 100, // in MB
        minChunkSize: Integer = 10, // in MB (used to facilitate pages grouping. See PagesPartitionsDefiner.)
        outputFilesCompression: String = "bzip2"
    )

    /**
     * Define the command line options parser
     */
    val argsParser: OptionParser[Params] = new OptionParser[Params]("Mediawiki XML Dumper job") {
        head("Mediawiki XML dumper job", "")
        note("This job dumps the content of a wiki at a publish date from an Iceberg table.")
        help("help") text "Prints this usage text"

        opt[String]('w', "wiki_id") required() valueName "<wiki_id>" action { (x, p) =>
            p.copy(wikiId = x)
        } text "The name of the wiki: enwiki, frwiki, etc."

        opt[String]('s', "publish_until") required() valueName "<publish_until>" action { (x, p) =>
            p.copy(publishUntil = x)
        } text "The date to publish until as YYYY-MM-DD (job will append 00:00)."

        opt[String]('i', "source_table") valueName "<source_table>" action { (x, p) =>
            p.copy(sourceTable = x)
        } text "The Iceberg table to read data from."

        opt[String]('n', "namespaces_table") valueName "<namespaces_table>" action { (x, p) =>
            p.copy(namespacesTable = x)
        } text "A table with site info including namespace info."

        opt[String]('d', "namespaces_snapshot") valueName "<namespaces_snapshot>" action { (x, p) =>
            p.copy(namespacesSnapshot = x)
        } text "Which snapshot of the namespaces table to use."

        opt[String]('o', "output_folder") required() valueName "<output_folder>" action { (x, p) =>
            p.copy(outputFolder = x)
        } text "The output folder where the XML files will be written."

        opt[Int]("page_size_overhead") optional() valueName "<page_size_overhead>" action { (x, p) =>
            p.copy(pageSizeOverhead = x)
        } text "Overhead in kB to add to the revision page contents to estimate the page size."

        opt[Int]("max_target_file_size") optional() valueName "<max_target_file_size>" action { (x, p) =>
            p.copy(maxTargetFileSize = x)
        } text "The maximum size of the target XML files in MB."

        opt[Int]("min_chunk_size") optional() valueName "<min_chunk_size>" action { (x, p) =>
            p.copy(minChunkSize = x)
        } text "The minimum size of a chunk of pages in MB. Internally used for partitioning. See Upper."
    }

    /**
     * Job entrypoint
     *
     * @param args the parsed cli arguments
     */
    def main(args: Array[String]): Unit = {
        val params: Params = argsParser.parse(args, Params()).getOrElse(sys.exit(1))
        apply(params)
    }
}