package org.wikimedia.analytics.refinery.job.mediawikidumper

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

/**
 * Defines the concept of a partition of pages.
 * Should not be confused with Spark internal partitions.
 *
 * @param size Size of a partition in bytes. Sum of all revisions sizes, plus overhead.
 * @param startPageId Id of the first page of the partition.
 * @param endPageId Id of the last page of the partition.
 * @param sparkPartitionId Id of the partition used internally by Spark to order the pages. It's optional is the Id is
 *                         determined only at the end of the process.
 */
case class PagesPartition(
                             size: Long,
                             startPageId: Long,
                             endPageId: Long,
                             sparkPartitionId: Option[Int] = None
) {

    // Helper method to create a new PagesPartition with updated size and partitionStartPageId
    def sizeAndStartUpdate(newSize: Long, newPartitionStartPageId: Long): PagesPartition = {
        copy(size=newSize, startPageId=newPartitionStartPageId)
    }
}

object PagesPartition {

    val emptyPagesPartitionColumn = {
        lit(null).cast(StructType(Array(
            StructField("size", LongType),
            StructField("startPageId", LongType),
            StructField("endPageId", LongType),
            StructField("sparkPartitionId", IntegerType)
        )))
    }
}

/**
 * Defines the concept of a partition key of pages.
 *
 * Used by the ByPagesPartitionKeyPartitioner partitioner.
 *
 * The sparkPartitionId is used to determine in which partition the revision is sent to.
 * Whereas the pageId and revisionId is used to order the pages within a partition.
 *
 * @param sparkPartitionId
 * @param pageId
 * @param revisionId
 */
case class PagesPartitionKey(
    sparkPartitionId: Int,
    pageId: Long,
    revisionId: Long
) extends Ordered[PagesPartitionKey] {

    def compare(other: PagesPartitionKey): Int = {
        if (pageId == other.pageId) revisionId.compare(other.revisionId) else pageId.compare(other.pageId)
    }
}