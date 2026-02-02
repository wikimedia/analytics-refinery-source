package org.wikimedia.analytics.refinery.job.mediawikidumper

import java.io.OutputStream
import java.io.PrintWriter
import java.sql.Timestamp

import scala.collection.JavaConverters

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import org.apache.commons.compress.compressors.gzip.{GzipCompressorOutputStream, GzipParameters}
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream
import org.apache.spark.Partitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.wikimedia.analytics.refinery.core.ChunkedByteArrayOutputStream
import org.wikimedia.analytics.refinery.tools.LogHelper
import scopt.OptionParser

/** Job to dump the content of a wiki at a given date.
  */
object MediawikiDumper extends LogHelper {

    lazy val spark: SparkSession = SparkSession.builder.getOrCreate

    import spark.implicits._ // Used when converting Rows to case classes.

    /** Represents the range of pages and revisions within a partition.
      *
      * Used to keep track of the state of consuming a partition and to generate filenames for XML dump files
     *  with the format:
      * - Single page: wiki-date-pPageIdStartrRevisionIdStartrRevisionIdEnd.xml
      * - Multiple pages: wiki-date-pPageIdStartpPageIdEnd.xml
      *
      * @param startPageId the first page ID in the partition
      * @param startRevisionId the first revision ID in the partition
      * @param endPageId the last page ID in the partition
      * @param endRevisionId the last revision ID in the partition
      */
    private case class PageIdRange(
        startPageId: Long,
        startRevisionId: Long,
        endPageId: Long,
        endRevisionId: Long
    )

    /** Main class entry point
      *
      * @param params
      *   parsed Params including the configuration of the source and the
      *   target.
      * @return
      *   Unit
      */
    def apply(params: Params): Unit = {
        // necessary so timezones don't go local
        spark.conf.set("spark.sql.session.timeZone", "UTC")

        log.info(s"""Mediawiki Dumper
         | Will dump ${params.wikiId} at ${params.publishUntil}
         | from ${params.sourceTable} to ${params.outputFolder}""".stripMargin)

        val siteInfo = getSiteInfo(params)

        val partitioner = createPartitioner(
          readPartitioningData(params),
          calculateMaxPartitionSize(params)
        )

        writeXMLFiles(
          buildXMLFragmentChunks(
            partitionByXMLFileBoundaries(
              buildBaseRevisionsDF(params),
              partitioner
            ),
            siteInfo,
            params
          ),
          params
        )

        log.info(s"Mediawiki Dumper: Done. Output in ${params.outputFolder}")
    }

    def calculateMaxPartitionSize(params: Params): Long = {
        params.maxPartitionSizeMB * 1024 * 1024
    }

    /** Partitions and sorts rows and converts them to [[Revision Revisions]].
      *
      * The partitions assigned by `partitioner` should match the desired file
      * boundaries.
      *
      * @param df
      *   revision rows
      * @param partitioner
      *   partitioner grouping by future
      * @return
      *   a [[Dataset]] of sorted [[Revision Revisions]]
      */
    def partitionByXMLFileBoundaries(
        df: DataFrame,
        partitioner: Partitioner
    ): Dataset[Revision] = {
        val schema = df.schema
        spark
            .createDataFrame(
              df.rdd
                  .map(row => (RowKey(row), row))
                  .repartitionAndSortWithinPartitions(partitioner)
                  .map(_._2),
              schema
            )
            .as[Revision]
    }

    /** Load minimal data to build partition LUT.
      *
      * @param params
      *   the job parameters
      * @return
      *   A dataframe containing sparse revision data from the source table
      */
    def readPartitioningData(params: Params): DataFrame = {
        spark.sql(s"""SELECT
         |page_id as pageId,
         |revision_id as revisionId,
         |to_utc_timestamp(revision_dt, 'GMT') as timestamp,
         |revision_size as revisionSize
         |FROM ${params.sourceTable}
         |WHERE revision_dt < TIMESTAMP '${params.publishUntil}'
         |  AND wiki_id = '${params.wikiId}'
         |  AND page_id IS NOT NULL;""".stripMargin)
    }

    /** Build the base revisions dataframe.
      *
      * @param params
      *   the job parameters
      * @return
      *   A dataframe containing the revisions from the source table
      */
    def buildBaseRevisionsDF(params: Params): DataFrame = {
        spark
            .sql(s"""SELECT *
           |FROM ${params.sourceTable}
           |WHERE revision_dt < TIMESTAMP '${params.publishUntil}'
           |  AND wiki_id = '${params.wikiId}';""".stripMargin)
            // In the next select list, some NULL columns are added to fill the case classes.
            .selectExpr(
              "page_id as pageId",
              "page_namespace_id as ns",
              "replace(page_title, '_', ' ') as title",
              "replace(page_redirect_target, '_', ' ') as redirectTarget",
              "user_text as contributor",
              // user_id is null when this revision was done by an IP editor
              // user_id = 0 when this revision was done by an old system user
              // when backfilling from current dumps:
              //   user_id = -1 either when the user is deleted via rev_deleted or something else went wrong
              "user_id as contributorId",
              "user_is_visible as isEditorVisible",
              "revision_id as revisionId",
              // XSD mandates this timestamp format (timestamps are in UTC by spark.conf above)
              s"to_utc_timestamp(revision_dt, 'GMT') as timestamp",
              "revision_comment as comment",
              "revision_comment_is_visible as isCommentVisible",
              "revision_content_is_visible as isContentVisible",
              // revision_parent_id is null when the revision has no parent (the first revision or sometimes imported revisions)
              // revision_parent_id = -1 when something went wrong with processing the import or backfill
              "revision_parent_id as parentId",
              "revision_is_minor_edit as isMinor",
              "revision_content_slots as contentSlots",
              "revision_size as revisionSize"
            )
    }

    /** Creates a [[Partitioner]] based on (sparse) revision rows.
      *
      * Massages `df` to come up with lists:
      *   - For regular sized pages (<= `maxPartitionSize`): a list of page ID
      *     ranges, each represented by its max. page ID
      *   - For oversize pages (> `maxPartitionSize`): a list of revision
      *     timestamp ranges, each represented by its max. timestamp, per
      *     oversize page
      *
      * Partitions are based on summed up revision sizes.
      *
      * @param df
      *   data frame of (sparse) revision rows
      * @param maxPartitionSize
      *   max size of a partition (sum of sizes of revisions in it)
      * @return
      */
    def createPartitioner(
        df: DataFrame,
        maxPartitionSize: Long
    ): RangeLookupPartitioner[RowKey, Long] = {
        // Step 1: Calculate total size for each pageId group
        val groupedByPageId = df
            .groupBy("pageId")
            .agg(sum("revisionSize").alias("pageSize"))

        // Step 2: Identify oversize pages and regular pages
        val oversizePages = groupedByPageId
            .filter($"pageSize" > maxPartitionSize)
        val regularPages = groupedByPageId
            .filter($"pageSize" <= maxPartitionSize)

        val regularWithPartition = regularPages
            .select(
              $"pageId",
              RebasedSizeBucket
                  .bucket($"pageSize", maxPartitionSize)
                  .over(Window.orderBy("pageId"))
                  .as("partitionId")
            )
            .groupBy("partitionId")
            .agg(max($"pageId").as("maxPageId"))

        // Step 3: Handle oversize pages by splitting them into chunks
        val partitionByPageIdWindow = Window.partitionBy("pageId")
        val oversizeWithPartition = df
            .join(broadcast(oversizePages.select("pageId")), "pageId")
            .select(
              $"pageId",
              $"timestamp",
              RebasedSizeBucket
                  .bucket($"revisionSize", maxPartitionSize)
                  .over(
                    partitionByPageIdWindow.orderBy($"timestamp", $"revisionId")
                  )
                  .as("partitionId")
            )
            .groupBy("pageId", "partitionId")
            .agg(
              max($"timestamp").as("maxTimestamp"),
              last($"pageId").as("pageId")
            )

        val timestampRangesPerPageId = oversizeWithPartition
            .collect()
            .groupBy(row => row.getAs[Long]("pageId"))
            .map { case (pageId, rows) =>
                (
                  pageId,
                  rows.map(row => row.getAs[Timestamp]("maxTimestamp").getTime)
                      .toIterable
                )
            }

        RangeLookupPartitioner[RowKey, Long](
          timestampRangesPerPageId,
          (rowKey: RowKey) => rowKey.timestamp.getTime,
          regularWithPartition
              .collect()
              .map(row => row.getAs[Long]("maxPageId")),
          (rowKey: RowKey) => rowKey.pageId
        )
    }

    private val COL_VALUE = "value"
    private val COL_PARTITION_ID = "partition_id"
    private val COL_PARTITION_CHECKSUM = "partition_checksum"

    private val FRAGMENT_SCHEMA = StructType(
      List(
        StructField(
          COL_VALUE,
          DataTypes.createArrayType(DataTypes.ByteType),
          nullable = false
        ),
        StructField(COL_PARTITION_ID, DataTypes.StringType, nullable = false),
        StructField(
          COL_PARTITION_CHECKSUM,
          DataTypes.LongType,
          nullable = false
        )
      )
    )

    /** Creates an appropriate compressor output stream based on the selected algorithm and level.
      *
      * @param outputStream the base output stream to compress
      * @param algorithm the compression algorithm to use (bzip2, gzip, zstd, none)
      * @param level the compression level (-1 for the default level of the algorithm)
      * @return a compressed output stream
      */
    private def getCompressorStream(outputStream: OutputStream, algorithm: String, level: Int): OutputStream = {
        def getActualLevel(defaultLevel: Int, minLevel: Int, maxLevel: Int): Int = {
            if (level == -1) defaultLevel
            else Math.max(minLevel, Math.min(maxLevel, level))
        }

        algorithm match {
            case "bzip2" =>
                // BZip2 levels: 1-9, default 9
                val bzLevel = getActualLevel(9, 1, 9)
                new BZip2CompressorOutputStream(outputStream, bzLevel)

            case "gzip" =>
                // GZip levels: 1-9, default 6
                val gzLevel = getActualLevel(6, 1, 9)
                val gzParams = new GzipParameters()
                gzParams.setCompressionLevel(gzLevel)
                new GzipCompressorOutputStream(outputStream, gzParams)

            case "zstd" =>
                // Zstd levels: 1-22, default 3
                val zstdLevel = getActualLevel(3, 1, 22)
                new ZstdCompressorOutputStream(outputStream, zstdLevel)

            case "none" =>
                outputStream

            case _ => throw new IllegalArgumentException(
                s"Unsupported compression algorithm: $algorithm. " +
                "Supported values are: bzip2, gzip, zstd, none"
            )
        }
    }

    /** Build the XML fragments from the sorted and partitioned revisions.
      *
      * The XML fragments mainly are the XML representation of the revisions,
      * with the page header and footer.
      *
      * All XML fragments of a partition are compressed using the specified algorithm and split
      * into chunks.
      *
      * @param sortedAndPartitionedRevisions
      *   the sorted and partitioned revisions
      * @param params
      *   the job parameters
      * @return
      *   An RDD of XML fragments
      */
    def buildXMLFragmentChunks(
        sortedAndPartitionedRevisions: Dataset[Revision],
        siteInfo: Broadcast[SiteInfo],
        params: Params
    ): Dataset[Row] = {

        val pageFooterFragment = XMLFragment.pageFooter().getXML

        sortedAndPartitionedRevisions.mapPartitions[Row] {
            revisions: Iterator[Revision] =>
                if (revisions.nonEmpty) {
                    val outputStream = {
                        new ChunkedByteArrayOutputStream(params.outputChunkSize)
                    }
                    val outputCompressorStream = {
                        getCompressorStream(outputStream, params.compressionAlgorithm, params.compressionLevel)
                    }
                    val outputStreamCompressorWriter = {
                        // aggressively flush with autoFlush = true
                        // to keep memory usage reasonable
                        new PrintWriter(outputCompressorStream, true)
                    }
                    try {
                        outputStreamCompressorWriter.println(
                          XMLFragment.xmlHeader(siteInfo.value).getXML
                        )
                      // use iterator rather than a functional approach like foldLeft
                      // to stream the partition rather than load it completely in memory.
                      var pageIdRange: Option[PageIdRange] = None
                      while (revisions.hasNext) {
                          val revision = revisions.next()
                          pageIdRange match {
                            // Case 1: First revision in the partition
                            // Start a new page and initialize the range tracker
                            case None =>
                              outputStreamCompressorWriter
                                .println(revision.buildPage.getXML)
                              outputStreamCompressorWriter
                                .println(revision.getXML)
                              pageIdRange = Some(
                                PageIdRange(
                                  startPageId = revision.pageId,
                                  startRevisionId = revision.revisionId,
                                  endPageId = revision.pageId,
                                  endRevisionId = revision.revisionId
                                )
                              )
                            // Case 2: Starting a new page (different from the current endPageId)
                            // Close the previous page and start a new one
                            case Some(range) if range.endPageId != revision.pageId =>
                              outputStreamCompressorWriter
                                .println(pageFooterFragment)
                              outputStreamCompressorWriter
                                .println(revision.buildPage.getXML)
                              outputStreamCompressorWriter
                                .println(revision.getXML)
                              pageIdRange = Some(
                                range.copy(
                                  endPageId = revision.pageId,
                                  endRevisionId = revision.revisionId
                                )
                              )
                            // Case 3: Processing another revision of the same page
                            // Update the end revision ID to track the latest revision
                            case Some(range) =>
                              outputStreamCompressorWriter
                                .println(revision.getXML)
                              pageIdRange = Some(
                                range.copy(
                                  endRevisionId = revision.revisionId
                                )
                              )
                          }
                        }

                        val partitionId = {
                            pageIdRange
                                .map { range =>
                                    // Single page partition: include revision range in filename
                                    if (range.startPageId == range.endPageId) {
                                        s"p${range.startPageId}r${range.startRevisionId}r${range.endRevisionId}"
                                    // Multiple pages partition: include only page range in filename
                                    } else {
                                        s"p${range.startPageId}p${range.endPageId}"
                                    }
                                }
                                .get
                        }

                        outputStreamCompressorWriter.println(pageFooterFragment)
                        outputStreamCompressorWriter
                            .println(XMLFragment.xmlFooter().getXML)
                        outputStreamCompressorWriter.close()
                        outputCompressorStream.close()
                        outputStream.close()
                        // keep things as iterable to avoid further conversion costs.
                        val chunkIterator = {
                          JavaConverters.iterableAsScalaIterable(outputStream.getChunks)
                            .map(chunk => {
                              new GenericRowWithSchema(
                                Array[Any](
                                  chunk,
                                  partitionId,
                                  outputStream.getChecksum
                                ),
                                FRAGMENT_SCHEMA
                              )
                            })
                            .toIterator
                        }

                        chunkIterator
                    } finally {
                        outputStreamCompressorWriter.close()
                        outputCompressorStream.close()
                        outputStream.close()
                    }
                } else {
                    Iterator.empty
                }

        }(RowEncoder(FRAGMENT_SCHEMA))
    }

    /** Writes the dataframe as XML files.
      *
      * Partitions `fragments` by their associated page range and Creates one
      * file per partition.
      *
      * @param fragments
      *   the XML fragments and their associated page range
      * @param params
      *   the job parameters
      */
    def writeXMLFiles(
        fragments: Dataset[Row],
        params: Params,
        outputFolderSuffix: String = ""
    ): Unit = {
        val extension = params.compressionAlgorithm match {
            case "bzip2" => ".bz2"
            case "gzip" => ".gz"
            case "zstd" => ".zst"
            case _ => "" // none, that is, plain XML
        }

        fragments
            .write
            .mode(SaveMode.Overwrite)
            .format("wmf-binary")
            .option(
              "filename-replacement",
              s"${params.wikiId}-${params.publishUntil}-$${$COL_PARTITION_ID}.xml$extension"
            )
            .save(params.outputFolder + outputFolderSuffix)
    }

    case class Params(
        wikiId: String = "",
        publishUntil: String = "",
        sourceTable: String = "wmf_content.mediawiki_content_history_v1",
        namespacesTable: String = "wmf_raw.mediawiki_project_namespace_map",
        namespacesSnapshot: String = "",
        maxPartitionSizeMB: Long = 100, // in MB
        outputChunkSize: Int = 100 * 1024 * 1024, // in B
        outputFolder: String = "",
        compressionAlgorithm: String = "bzip2", // Default compression algorithm
        compressionLevel: Int = -1 // -1 means use the default level for each algorithm
    )

    /** Define the command line options parser
      */
    val argsParser: OptionParser[Params] = {
        new OptionParser[Params]("Mediawiki XML Dumper job") {
            head("Mediawiki XML dumper job", "")
            note(
              "This job dumps the content of a wiki at a publish date from an Iceberg table."
            )
            help("help") text "Prints this usage text"

            opt[String]('w', "wiki_id") required
                () valueName "<wiki_id>" action { (x, p) =>
                    p.copy(wikiId = x)
                } text "The name of the wiki: enwiki, frwiki, etc."

            opt[String]('s', "publish_until") required
                () valueName "<publish_until>" action { (x, p) =>
                    p.copy(publishUntil = x)
                } text
                "The date to publish until as YYYY-MM-DD (job will append 00:00)."

            opt[String]('i', "source_table") valueName "<source_table>" action {
                (x, p) => p.copy(sourceTable = x)
            } text "The Iceberg table to read data from."

            opt[String]('n', "namespaces_table") valueName
                "<namespaces_table>" action { (x, p) =>
                    p.copy(namespacesTable = x)
                } text "A table with site info including namespace info."

            opt[String]('d', "namespaces_snapshot") valueName
                "<namespaces_snapshot>" action { (x, p) =>
                    p.copy(namespacesSnapshot = x)
                } text "Which snapshot of the namespaces table to use."

            opt[String]('o', "output_folder") required
                () valueName "<output_folder>" action { (x, p) =>
                    p.copy(outputFolder = x)
                } text "The output folder where the XML files will be written."

            opt[Int]("output_chunk_size") optional
                () valueName "<output_chunk_size>" action { (x, p) =>
                    p.copy(outputChunkSize = x)
                } text
                """Defines the maximum size (in bytes) of each allocated output buffer when compressing XML data.
                |The choice of chunk size impacts memory efficiency:
                |too small, and frequent allocations may slow down processing;
                |too large, and memory waste increases due to underutilized chunks.
                |As a rule of thumb, use a quarter of the avg. output file size."""
                    .stripMargin

            opt[Int]("max_partition_size") optional
                () valueName "<max_partition_size>" action { (x, p) =>
                    p.copy(maxPartitionSizeMB = x)
                } text
                """Specifies the maximum uncompressed size (in MB) of a partition of revisions.
              |This size is based on the raw revision content, not the final rendered XML.
              |Each partition is immediately compressed with bzip2.
              |Since XML is verbose, compression can achieve ratios of up to 85:1.
              |In practice, a 2,024 MB partition typically results in a ~100 MB compressed file."""
                    .stripMargin

            opt[String]('c', "compression") optional() valueName "<compression>" action { (x, p) =>
                p.copy(compressionAlgorithm = x.toLowerCase)
            } text """Compression algorithm to use:
                     |  - bzip2 (levels 1-9, default 9)
                     |  - gzip  (levels 1-9, default 6)
                     |  - zstd  (levels 1-22, default 3)
                     |  - none
                     |Defaults to bzip2""".stripMargin

            opt[Int]("compression-level") optional() valueName "<level>" action { (x, p) =>
                p.copy(compressionLevel = x)
            } text "Compression level. Use -1 for algorithm default. See --compression help for valid ranges."
        }
    }

    def getSiteInfo(params: Params): Broadcast[SiteInfo] = {
        val siteInfoDF = spark.sql(f"""|
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

        spark.sparkContext.broadcast(siteInfoDF.as[SiteInfo].first())
    }

    /** Job entrypoint
      *
      * @param args
      *   the parsed cli arguments
      */
    def main(args: Array[String]): Unit = {
        val params: Params = argsParser
            .parse(args, Params())
            .getOrElse(sys.exit(1))
        apply(params)
    }
}
