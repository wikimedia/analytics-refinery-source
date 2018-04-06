package org.wikimedia.analytics.refinery.spark.utils

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.util.AccumulatorV2

/**
  * This class allows to use a Map in an accumulator.
  * The Map key type is [[K]], and it's value is [[V]].
  *
  * The accumulator input type is (K, V), meaning the
  * accumulator.add function is to be given a (key, value)
  * pair. The key is the map entry to update, and the value
  * is either set as is if the key was not already existing
  * in the map, either summed with the existing one using the
  * implicit sumValues function.
  *
  * @param sumValues implicit function defining how to sum values
  * @tparam K The map key type
  * @tparam V The map value type
  */

class MapAccumulator[K, V](implicit val sumValues: (V, V) => V) extends AccumulatorV2[(K, V), ConcurrentHashMap[K, V]] {

  /**
    * As explained in the scala API
    * https://spark.apache.org/docs/2.2.1/api/scala/index.html#org.apache.spark.util.AccumulatorV2
    * The accumulator OUT type needs to be thread-safe, therefore the ConcurrentHashMap choice
    */
  private val map = new ConcurrentHashMap[K, V]

  /**
    * This method is used to add a (key, value) pair to the map.
    * If the key doesn't exist in the map, it is set, with value
    * as original value. If the key is already present in the map,
    * it's associated value is updated to sumValues(map(key), value).
    *
    * @param kv the key-value pair to add to the map
    */
  override def add(kv: (K, V)): Unit = {
    if (map.containsKey(kv._1)) {
      map.put(kv._1, sumValues(map.get(kv._1), kv._2))
    } else {
      map.put(kv._1, kv._2)
    }
  }


  /**
    * This method is used in the internal of spark to merge distributed
    * versions of the Accumulator into the final one to be used in the
    * master.
    * Merging is done by looping over the `other` map entries and adding them
    * (in the sense of the [[add]] function above) to the current one.
    *
    * @param other the MapAccumlator to merge in
    */
  override def merge(other: AccumulatorV2[(K, V), ConcurrentHashMap[K, V]]): Unit = {
    other match {
      case o: MapAccumulator[K, V] =>
        val iter = o.map.entrySet().iterator()
        while (iter.hasNext) {
          val kv = iter.next()
          val k = kv.getKey
          if (map.containsKey(k)) {
            map.put(k, sumValues(map.get(k), kv.getValue))
          } else {
            map.put(k, kv.getValue)
          }
        }
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }
  }

  /**
    * This method creates a copy of a MapAccumulator, duplicating
    * the inner-map stored by the accumulator
    */
  override def copy(): AccumulatorV2[(K, V), ConcurrentHashMap[K, V]] = {
    val copy = new MapAccumulator[K, V]
    val iter = map.entrySet().iterator()
    while (iter.hasNext) {
      val kv = iter.next()
      copy.map.put(kv.getKey, kv.getValue)
    }
    copy
  }

  /**
    * The value returned by the MapAccumulator is the inner ConcurrentHashMap
    * @return the inner ConcurrentHashMap
    */
  override def value: ConcurrentHashMap[K, V] = map

  /**
    * The accumulator value is zero if the inner map has no key set yet
    */
  override def isZero: Boolean = map.isEmpty

  /**
   * Resetting the accumulator means clearing the inner-map from its entries
   */
  override def reset(): Unit = map.clear()
}
