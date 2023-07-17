package org.wikimedia.analytics.refinery.job.mediawikidumper

import org.apache.spark.Partitioner

/**
 * Custom Spark partitioner to group elements into partitions defined by a PagesPartitionKey.
 * eg:
 *  - (pagePartitionKey1(sparkPartitionId=1, ...), element1) => partition 1
 *  - (pagePartitionKey1(sparkPartitionId=1, ...), element2) => partition 1
 *  - (pagePartitionKey1(sparkPartitionId=2, ...), element3) => partition 2
 *  - (pagePartitionKey1(sparkPartitionId=3, ...), element4) => partition 3
 *  - (pagePartitionKey1(sparkPartitionId=3, ...), element5) => partition 3
 *
 * As of 2023-09, it is used in pair with the PagesPartitionsDefiner which is in charge of defining the partitioning.
 *
 * @param storedNumPartitions the number of partitions
 */
class ByPagesPartitionKeyPartitioner(storedNumPartitions: Int) extends Partitioner {

    override def numPartitions: Int = storedNumPartitions

    def getPartition(key: Any): Int = key.asInstanceOf[PagesPartitionKey].sparkPartitionId
}
