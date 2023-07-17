package org.wikimedia.analytics.refinery.job.mediawikidumper

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{FlatSpec, Matchers}

class PagesPartitionsDefinerSpec
    extends FlatSpec
        with Matchers
        with DataFrameSuiteBase {

    import spark.implicits._

    val baseDFColumns: Seq[String] = Seq("pageId", "size")

    val data: Seq[(Int, Int)] = Seq(
        (1001, 5),
        (1002, 6),
        (1003, 7),
        (1003, 8),
        (1004, 0)
    )

     def definer(): PagesPartitionsDefiner = {
         val baseDF: org.apache.spark.sql.DataFrame = spark.sparkContext.parallelize(data).toDF(baseDFColumns: _*)
         new PagesPartitionsDefiner(spark, baseDF, 1, 10, 1)
     }

    "pagesPartitionsSourceRDD" should "compute page size with overhead" in {
        definer().pagesPartitionsSourceRDD.collect().map(_.size) should contain theSameElementsAs Seq(1029, 1030, 2*1024+7+8, 1024)
    }

    it should "fill the parameters of the PagesPartition" in {
        definer().pagesPartitionsSourceRDD.take(1)(0) should equal(PagesPartition(1029, 1001, 1001, None))
    }

    it should "handle pages without contents" in {
        definer().pagesPartitionsSourceRDD.collect().takeRight(1)(0).size should equal(1024)
    }

    val maxPartitionSize = 1  // 1 MB

    "mergeSequentiallyBySize" should "handle an empty RDD" in {
        val emptyData = Iterator[PagesPartition]()
        definer().mergeSequentiallyBySize(emptyData, maxPartitionSize) should be(empty)
    }

    it should "handle partitions exceeding the limit" in {
        val partition = PagesPartition(2*1024*1024, 1, 1)  // 2MB
        val data = Iterator[PagesPartition](partition)
        definer().mergeSequentiallyBySize(data, maxPartitionSize).toList.head should equal(partition)
    }

    it should "merge partitions into 2 groups" in {
        val data = Iterator[PagesPartition](
            PagesPartition(400 * 1024, 1, 1), // 400kB
            PagesPartition(400 * 1024, 2, 2), // 400kB
            PagesPartition(400 * 1024, 3, 3), // 400kB
            PagesPartition(400 * 1024, 4, 4), // 400kB
            PagesPartition(400 * 1024, 5, 5), // 400kB
        )
        val result = definer().mergeSequentiallyBySize(data, maxPartitionSize).toList
        result.map(_.endPageId) should equal(List(3,5))
        result.map(_.startPageId) should equal(List(1,4))
        result.map(_.size) should equal(List(1200*1024, 800*1024))
    }

    it should "handle partitions of exactly the max size" in {
        val data = Iterator[PagesPartition](
            PagesPartition(512 * 1024, 1, 1), // 500kB
            PagesPartition(512 * 1024, 2, 2), // 500kB
            PagesPartition(200 * 1024, 3, 3), // 200kB
        )
        val result = definer().mergeSequentiallyBySize(data, maxPartitionSize).toList
        result.map(_.startPageId) should equal(List(1, 3))
        result.map(_.endPageId) should equal(List(2, 3))
        result.map(_.size) should equal(List(2 * 512 * 1024, 200 * 1024))  // 1MB, 200kB
    }

    it should "handle large pages" in {
        val data = Iterator[PagesPartition](
            PagesPartition(400 * 1024, 1, 1), // 400kB
            PagesPartition(400 * 1024, 2, 2), // 400kB
            PagesPartition(10 * 1024 *1024, 3, 3), // 10MB
            PagesPartition(200 * 1024, 4, 4), // 200kB
        )
        val result = definer().mergeSequentiallyBySize(data, maxPartitionSize).toList
        result.map(_.startPageId) should equal(List(1, 4))
        result.map(_.endPageId) should equal(List(3, 4))
        result.map(_.size) should equal(List(2*400*1024 + 10*1024*1024, 200*1024)) // 10,8MB, 200kB
    }

    val data2: Seq[(Int, Int)] = Seq(
        (101, 600 * 1024), // 600kB
        (102, 600 * 1024), // 600kB
        (103, 800 * 1024), // 800kB
        (103, 800 * 1024), // 800kB
        (104, 8) // >1kB
    )
    def baseDF2(): org.apache.spark.sql.DataFrame = {
         spark.sparkContext.parallelize(data2).toDF(baseDFColumns: _*)
    }

     def definer2(): PagesPartitionsDefiner = {
         new PagesPartitionsDefiner(spark, baseDF2(), 1, 1, 1)
     }

    "definePartitions" should "add the partitionIds" in {
        definer2().pagesPartitions.map(_.startPageId) should equal(List(101, 103, 104))
        definer2().pagesPartitions.map(_.endPageId) should equal(List(102, 103, 104))
        definer2().pagesPartitions.map(_.sparkPartitionId).map(_.get) should equal(List(0,1,2))
    }

    it should "find more partitions if the calculation is more parallel" in {
        val baseDF3 = baseDF2().repartition(10)
        new PagesPartitionsDefiner(
            spark, baseDF3, 1, 1, 1,
            internalTargetNumberOfElementsInSparkPartitions = 1
        ).pagesPartitions.map(_.sparkPartitionId).map(_.get) should equal(List(0, 1, 2, 3))
    }

    "partitionEndPageIds" should "output the partition ends" in {
        definer2().partitionsEndPageIds should equal(List(102, 103, 104))
    }

    "getPagesPartition" should "retrieve the first partition if the first pageId is presented" in {
        definer2().getPagesPartition(101L).sparkPartitionId.get should equal(0)
    }

    it should "retrieve a page closing a partition" in {
        definer2().getPagesPartition(103L).sparkPartitionId.get should equal(1)
    }

    it should "retrieve the last partition if the last pageId is presented" in {
        definer2().getPagesPartition(104L).sparkPartitionId.get should equal(2)
    }


    it should "raise errors when the provided pageId is out of range" in {
        assertThrows[AssertionError] { definer2().getPagesPartition(100L) }
        assertThrows[AssertionError] { definer2().getPagesPartition(105L) }
    }
}
