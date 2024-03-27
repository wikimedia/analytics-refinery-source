/**
  * This application is part of the Commons Impact Metrics pipeline.
  * It generates a representation of the Commons category tree,
  * including the media files pointing to those categories.
  * Only the categories and media files that belong to a given allow-list
  * of top-level (primary) categories are included in the output.
  * The generated output is a temporary intermediate dataset that will
  * be used by other steps further down the pipeline.
  *
  * Usage:
  *
  *     spark3-submit \
  *         --name commons_category_graph_builder_test \
  *         --master yarn \
  *         --deploy-mode cluster \
  *         --executor-cores 4 \
  *         --executor-memory 32G \
  *         --driver-cores 2 \
  *         --driver-memory 8G \
  *         --conf spark.dynamicAllocation.maxExecutors=64 \
  *         --conf spark.executor.memoryOverhead=2G \
  *         --conf spark.sql.shuffle.partitions=1024 \
  *         --conf spark.yarn.maxAppAttempts=1 \
  *         --conf spark.graphx.pregel.checkpointInterval=10 \
  *         --class org.wikimedia.analytics.refinery.job.CommonsCategoryGraphBuilder \
  *         hdfs:///user/mforns/artifacts/refinery-job-0.2.34-SNAPSHOT-shaded.jar \
  *         --page-table wmf_raw.mediawiki_page \
  *         --categorylinks-table wmf_raw.mediawiki_categorylinks \
  *         --category-allow-list-url https://gitlab.wikimedia.org/.../allow_list.tsv \
  *         --mediawiki-snapshot 2024-02 \
  *         --output-table mforns.commons_category_graph_2024_02 \
  *         --intermediate-partitions 1024 \
  *         --output-partitions 128 \
  *         --max-distance-to-primary 10 \
  *         --checkpoint-directory http:///some/dir
  */

package org.wikimedia.analytics.refinery.job

import org.apache.spark.graphx._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.io.Source
import scala.math.min
import scopt.OptionParser

object CommonsCategoryGraphBuilder {

    // Data case class to hold records from the categorylinks table.
    case class CategoryLink(
        clFrom: Long,
        clTo: Long,
        clType: String,
    )

    // Message case class for the Pregel graph algorithm.
    case class Message(
        parentCategories: Set[Long],
        primaryCategories: Set[Long],
        ancestorCategories: Set[Long],
        distanceToPrimary: Option[Int],
    )

    // Data case class to output the results of this calculation.
    // The snake_case is because the output table will be schema'd after this.
    case class OutputRecord(
        page_id: Long,
        page_type: String,
        parent_categories: List[Long],
        primary_categories: List[Long],
        ancestor_categories: List[Long],
    )

    case class Params(
        pageTable: String = "",
        categorylinksTable: String = "",
        categoryAllowListUrl: String = "",
        mediawikiSnapshot: String = "",
        outputTable: String = "",
        intermediatePartitions: Int = 1024,
        outputPartitions: Int = 128,
        maxDistanceToPrimary: Int = 10,
        checkpointDirectory: String = "",
    )

    val argsParser = new OptionParser[Params]("Commons Category Graph Builder") {
        help("help") text ("Print this usage text and exit.")

        opt[String]("page-table") required() valueName ("<table_name>") action { (x, p) =>
            p.copy(pageTable = x)
        } text ("Fully qualified name of the MediaWiki page table to use.")

        opt[String]("categorylinks-table") required() valueName ("<table_name>") action { (x, p) =>
            p.copy(categorylinksTable = x)
        } text ("Fully qualified name of the MediaWiki categorylinks table to use.")

        opt[String]("category-allow-list-url") required() valueName ("<url>") action { (x, p) =>
            p.copy(categoryAllowListUrl = x)
        } text ("URL of the Commons category allow-list file to use (i.e. GitLab raw URL).")

        opt[String]("mediawiki-snapshot") required() valueName ("<YYYY-MM>") action { (x, p) =>
            p.copy(mediawikiSnapshot = x)
        } text ("MediaWiki snapshot for which to build the category graph.")

        opt[String]("output-table") required() valueName ("<table_name>") action { (x, p) =>
            p.copy(outputTable = x)
        } text ("Fully qualified name of the Iceberg table to write to.")

        opt[Int]("intermediate-partitions") optional() valueName ("<integer>") action { (x, p) =>
            p.copy(intermediatePartitions = x)
        } text ("Used to repartition the source data once filtered. Default: 1024.")

        opt[Int]("output-partitions") optional() valueName ("<integer>") action { (x, p) =>
            p.copy(outputPartitions = x)
        } text ("Used to repartition the output data into files. Default: 128.")

        opt[Int]("max-distance-to-primary") optional() valueName ("<integer>") action { (x, p) =>
            p.copy(maxDistanceToPrimary = x)
        } text ("Category graph nodes farther away from this distance will be collapsed. Default: 10.")

        opt[String]("checkpoint-directory") required() valueName ("<path>") action { (x, p) =>
            p.copy(checkpointDirectory = x)
        } text ("HDFS directory path where to store Pregel's checkpoints.")
    }

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName(s"CommonsCategoryGraphBuilder")
        val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
        val params = argsParser.parse(args, Params()).getOrElse(sys.exit(1))
        spark.sparkContext.setCheckpointDir(params.checkpointDirectory)
        import spark.implicits._

        // Get the list of allow-listed top-level category IDs.
        val commonsCategoryDf = spark.sql(s"""
            SELECT
                page_id,
                page_title
            FROM ${params.pageTable}
            WHERE
                snapshot = '${params.mediawikiSnapshot}' AND
                wiki_db = 'commonswiki' AND
                page_namespace = 14
        """)
        val allowListedCategoryTitlesDf = Source
            .fromURL(params.categoryAllowListUrl, "UTF-8")
            .mkString.split("\n").toList.toDF("page_title")
        val categoryAllowList = commonsCategoryDf
            .join(allowListedCategoryTitlesDf, "page_title")
            .select(commonsCategoryDf("page_id"))
            .map(row => row.getLong(0))
            .collect()

        // Function called by the Pregel algorithm to merges all messages for a given vertex.
        def mergeMessage (msg1: Message, msg2: Message): Message = {
            Message(
                // All category lists are the unioned.
                parentCategories = msg1.parentCategories ++ msg2.parentCategories,
                primaryCategories = msg1.primaryCategories ++ msg2.primaryCategories,
                ancestorCategories = msg1.ancestorCategories ++ msg2.ancestorCategories,
                // The distance to primary is considered to be the minimum of both messages.
                // We don't need to check whether any of the distances are defined,
                // because mergeMessage will only be called for messages coming from
                // initialized vertices, which always have a distanceToPrimary defined.
                distanceToPrimary = Some(min(msg1.distanceToPrimary.get, msg2.distanceToPrimary.get)),
            )
        }

        // Function called by the Pregel algorithm to assign a new vertex state given the received message.
        def setMessage (id: VertexId, value: Message, message: Message): Message = {
            if (message.primaryCategories == Set[Long]()) {
                // Default message received by all vertices.
                val isPrimaryCategory = categoryAllowList.contains(id)
                Message(
                    parentCategories = Set[Long](),
                    primaryCategories = if (isPrimaryCategory) Set(id) else Set[Long](),
                    ancestorCategories = Set[Long](),
                    distanceToPrimary = if (isPrimaryCategory) Some(0) else None,
                )
            } else { // Regular message during graph traversal.
                Message(
                    // The parent categories are unioned.
                    parentCategories = value.parentCategories ++ message.parentCategories,
                    // We remove the current category id from the primary categories list, just in case
                    // the current category was an allow-listed category that just received a message.
                    // This can happen if the allow-list contains two categories that are related.
                    // Also, we remove all the primary categories that appear as ancestors of the message.
                    // To finally add the message's primary categories.
                    primaryCategories = value.primaryCategories - id -- message.ancestorCategories ++ message.primaryCategories,
                    // The ancestor categories are unioned.
                    ancestorCategories = value.ancestorCategories ++ message.ancestorCategories,
                    // Once the distance is set, it stays like that, since the first message to
                    // reach a vertex carries the shortest distance to a primary category.
                    distanceToPrimary = value.distanceToPrimary.orElse(message.distanceToPrimary),
                )
            }
        }

        // Function called by the Pregel algorithm to determine which messages to send
        // given the origin vertex, the destination vertex and the edge in between them.
        def sendMessage (triplet: EdgeTriplet[Message, String]): Iterator[(VertexId, Message)] = {
            val origin = triplet.srcAttr
            val originId = triplet.srcId
            val destination = triplet.dstAttr
            val destinationId = triplet.dstId

            // Only continue if the origin primary category is in the allow-list.
            if (origin.primaryCategories.isEmpty) return Iterator.empty

            // Only continue if the destination does not create a cycle.
            if (originId == destinationId || origin.ancestorCategories.contains(destinationId)) return Iterator.empty

            // Only continue if the distance to primary is smaller than the maximum.
            if (origin.distanceToPrimary.get >= params.maxDistanceToPrimary) return Iterator.empty

            // Build the message to send to the destination vertex.
            Iterator((destinationId, Message(
                // The parent category of this message is the category that sends it.
                parentCategories = Set(originId),
                // Primary categories of the child are the same as the parent.
                primaryCategories = origin.primaryCategories,
                // Add this category to the ancestor categories.
                ancestorCategories = origin.ancestorCategories + originId,
                // Increment the distance to primary by one.
                distanceToPrimary = Some(origin.distanceToPrimary.get + 1),
            )))
        }

        // Get all commons category links potentially involved.
        val categoryLinksDf = spark
            .sql(s"""
                SELECT
                    cl.cl_from,
                    pg.page_id AS cl_to,
                    cl.cl_type
                FROM ${params.categorylinksTable} cl
                    INNER JOIN ${params.pageTable} pg
                    ON (cl.cl_to = pg.page_title)
                WHERE
                    cl.wiki_db = 'commonswiki' AND
                    cl.snapshot = '${params.mediawikiSnapshot}' AND
                    pg.wiki_db = 'commonswiki' AND
                    pg.snapshot = '${params.mediawikiSnapshot}' AND
                    pg.page_namespace = 14
            """)
            .repartition(params.intermediatePartitions)
            .map(r => CategoryLink(r.getLong(0), r.getLong(1), r.getString(2)))

        // Get a graph of the commons categories (filtering out media files).
        val categoryEdgesRdd = categoryLinksDf
            .filter(r => r.clType == "subcat")
            // In the categorylinks table the subcategory points up to the parent category.
            // But here we want to point from parent category (clTo) to subcategory (clFrom).
            .map(r => Edge(r.clTo, r.clFrom, ""))
            .rdd

        // Execute the Pregel algorithm to get the subgraph of allow-listed category trees.
        // https://spark.apache.org/docs/latest/graphx-programming-guide.html#pregel-api
        val defaultMessage = Message(Set[Long](), Set[Long](), Set[Long](), None)
        val categoryGraphRdd = Graph
            .fromEdges(categoryEdgesRdd, defaultMessage)
            .pregel(defaultMessage, 100, EdgeDirection.Out)(setMessage, sendMessage, mergeMessage)
            // Keep only categories that belong to allow-listed category trees.
            .vertices.filter(v => v._2.distanceToPrimary.isDefined)
            .map(v => (v._1.toLong, v._2)) // (category page id, message)

        // Get the media file categorylinks ready for joining with the category graph.
        val mediaFilesRdd = categoryLinksDf
            .filter(cl => cl.clType == "file")
            // Key by the category id, so we can join with the category graph.
            .map(cl => (cl.clTo, cl.clFrom))
            .rdd

        // Join category graph and media files and update media file information.
        val decoratedMediaFilesRdd = categoryGraphRdd
            .join(mediaFilesRdd)
            .map{case (categoryId, (categoryInfo, mediaFileId)) => (
                mediaFileId, // Key by media file id to group by media file.
                Message(
                    // Calculate the media file properties from the parent category.
                    Set(categoryId),
                    categoryInfo.primaryCategories,
                    categoryInfo.ancestorCategories + categoryId,
                    Some(categoryInfo.distanceToPrimary.get + 1),
                )
            )}
            .groupByKey()
            .map{case (pageId: Long, categoryInfo: Iterable[Message]) =>
                // Merge the information of all of the media file's paren categories.
                val mergedMessage = categoryInfo.fold(defaultMessage)((acc, m) => Message(
                    parentCategories = acc.parentCategories ++ m.parentCategories,
                    primaryCategories = acc.primaryCategories ++ m.primaryCategories,
                    ancestorCategories = acc.ancestorCategories ++ m.ancestorCategories,
                    distanceToPrimary = if (acc.distanceToPrimary.isDefined)
                        Some(min(acc.distanceToPrimary.get, m.distanceToPrimary.get))
                    else m.distanceToPrimary,
                ))
                // Transform the result into an output record.
                OutputRecord(
                    page_id = pageId,
                    page_type = "file",
                    parent_categories = mergedMessage.parentCategories.toList,
                    primary_categories = mergedMessage.primaryCategories.toList,
                    ancestor_categories = mergedMessage.ancestorCategories.toList,
                )
            }

        // Final transformations and write the results.
        categoryGraphRdd
            .map{ case (pageId: Long, message: Message) => OutputRecord(
                page_id = pageId,
                page_type = "subcat",
                parent_categories = message.parentCategories.toList,
                primary_categories = message.primaryCategories.toList,
                ancestor_categories = message.ancestorCategories.toList,
            )}
            .union(decoratedMediaFilesRdd)
            .toDF
            .coalesce(params.outputPartitions)
            .write.saveAsTable(params.outputTable)
    }
}
