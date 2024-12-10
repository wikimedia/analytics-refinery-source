package org.wikimedia.analytics.refinery.job.mediawikidumper

import scala.collection.Searching._
import scala.collection.immutable.TreeMap

import org.apache.spark.Partitioner

class RangeLookupPartitioner[K, R](
    rangeBounds: List[R],
    extractRangeValue: K => R,
    offset: Int = 0
)(implicit ord: Ordering[R])
    extends Partitioner
    with Serializable {

    override def numPartitions: Int = rangeBounds.size

    override def getPartition(key: Any): Int = {
        key match {
            case k: K =>
                getPartitionForK(k).get
        }
    }

    def getPartitionForK(k: K): Option[Int] = indexOfRange(extractRangeValue(k))

    def indexOfRange(key: R): Option[Int] = {
        rangeBounds.search(key) match {
            case Found(index) =>
                Some(index + offset)
            case InsertionPoint(index) if index < rangeBounds.size =>
                Some(index + offset)
            case _ =>
                None
        }
    }
}

class SubRangeLookupPartitioner[K, R](
    subRangeBounds: Map[R, RangeLookupPartitioner[K, R]],
    rangeBounds: List[R],
    extractRangeValue: K => R,
    offset: Int = 0
)(implicit ord: Ordering[R])
    extends RangeLookupPartitioner[K, R](
      rangeBounds,
      extractRangeValue,
      offset
    ) {

    private val aggregatedNumberOfPartitions: Int = {
        subRangeBounds.values.map(_.numPartitions).sum + rangeBounds.size
    }

    override def numPartitions: Int = aggregatedNumberOfPartitions

    override def getPartitionForK(k: K): Option[Int] = {
        val delegateKey = extractRangeValue(k)
        subRangeBounds
            .get(delegateKey)
            .flatMap(_.getPartitionForK(k))
            .orElse(indexOfRange(delegateKey))
    }
}

object RangeLookupPartitioner {
    def apply[K, R](rangeBounds: Iterable[R], rangeKeyExtractor: K => R)(
        implicit ord: Ordering[R]
    ): RangeLookupPartitioner[K, R] = {
        new RangeLookupPartitioner[K, R](
          rangeBounds.toList.sorted,
          rangeKeyExtractor
        )
    }

    def apply[K, R](
        subRangesByRangeValue: Map[R, Iterable[R]],
        subRangeValueExtractor: K => R,
        rangeBounds: Iterable[R],
        rangeValueExtractor: K => R
    )(implicit ord: Ordering[R]): RangeLookupPartitioner[K, R] = {
        val (subRanges, offset) = {
            subRangesByRangeValue.foldLeft(
              (TreeMap.newBuilder[R, RangeLookupPartitioner[K, R]], 0)
            ) { case ((map, offset), (rangeKey, rangeBounds)) =>
                val partitioner = {
                    new RangeLookupPartitioner(
                      rangeBounds.toList.sorted,
                      subRangeValueExtractor,
                      offset
                    )
                }
                map += (rangeKey -> partitioner)
                (map, offset + partitioner.numPartitions)
            }
        }
        new SubRangeLookupPartitioner[K, R](
          subRanges.result(),
          rangeBounds.toList.sorted,
          rangeValueExtractor,
          offset
        )
    }
}
