package org.wikimedia.analytics.refinery.job.mediawikidumper

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.scalatest.{FlatSpec, Matchers}

class ByPagesPartitionKeyPartitionerSpec
    extends FlatSpec
    with DataFrameSuiteBase
    with Matchers {

    val data: Seq[(PagesPartitionKey, String)] = Seq(
        (PagesPartitionKey(0, 0L, 0L), "element1"),
        (PagesPartitionKey(0, 0L, 0L), "element2"),
        (PagesPartitionKey(1, 0L, 0L), "element3"),
        (PagesPartitionKey(2, 0L, 0L), "element4"),
        (PagesPartitionKey(2, 0L, 0L), "element5")
    )

    "ByPagesPartitionKeyPartitioner" should "partition by the key" in {
        val baseRDD: RDD[(PagesPartitionKey, String)] = spark.sparkContext.parallelize(data)
        val partitioner: ByPagesPartitionKeyPartitioner = new ByPagesPartitionKeyPartitioner(3)

        val partitionedRDD = baseRDD.repartitionAndSortWithinPartitions(partitioner)
        partitionedRDD.getNumPartitions should equal(3)

        val broadcastData: Broadcast[Seq[(PagesPartitionKey, String)]] = spark.sparkContext.broadcast(data)
        partitionedRDD.mapPartitions(partition => {
            val partitionList: List[(PagesPartitionKey, String)] = partition.toList
            val partitionId: Int = partitionList.head._1.sparkPartitionId
            val elementsFromData: Seq[String] = broadcastData.value
                .filter(t => t._1.sparkPartitionId == partitionId).map(_._2)
            if (partitionList.map(_._2) == elementsFromData) Iterator(0) else Iterator(1)
        }).sum should equal(0)
    }
}
