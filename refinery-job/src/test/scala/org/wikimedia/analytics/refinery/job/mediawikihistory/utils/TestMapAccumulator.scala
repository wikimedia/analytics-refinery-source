package org.wikimedia.analytics.refinery.job.mediawikihistory.utils

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers}

class TestMapAccumulator extends FlatSpec with Matchers with SharedSparkContext {

  "A MapAccumulator" should "accumulate simple values" in {
    implicit def sumLong = (a: Long, b: Long) => a + b
    val acc = new MapAccumulator[String, Long]
    sc.register(acc, "Test Map Accumulator")
    val rdd = sc.parallelize(0 until 10)

    val rddWithCounter = rdd.map(i => {
      if (i % 2 == 0) acc.add(("even", 1L))
      else acc.add(("odd", 1L))
    })
    // Force computation materialization
    rddWithCounter.count should equal(10)

    val map = acc.value
    map.size() should equal(2)
    map.get("even") should equal(5)
    map.get("odd") should equal(5)
  }

  "A MapAccumulator" should "accumulate complex values" in {
    implicit def sumLongPairs = (a: (Long, Long), b: (Long, Long)) => (a._1 + b._1, a._2 + b._2)
    val acc = new MapAccumulator[String, (Long, Long)]
    sc.register(acc, "Test Map Accumulator")
    val rdd = sc.parallelize(0 until 10)

    val rddWithCounter = rdd.map(i => {
      if (i % 2 == 0)
        if (i / 2 % 2 == 0)
          acc.add(("even", (1L, 0L)))
        else
          acc.add(("even", (0L, 1L)))
      else acc.add(("odd", (1L, 0L)))
    })
    // Force computation materialization
    rddWithCounter.count should equal(10)

    val map = acc.value
    map.size() should equal(2)
    map.get("even") should equal((3, 2))
    map.get("odd") should equal((5, 0))
  }

}
