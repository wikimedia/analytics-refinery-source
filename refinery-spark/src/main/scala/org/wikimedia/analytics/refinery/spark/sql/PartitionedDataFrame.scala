package org.wikimedia.analytics.refinery.spark.sql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.wikimedia.analytics.refinery.core.HivePartition

import scala.util.control.Exception._

/**
  * Class wrapping together a dataframe and a HivePartition.
  * Choice has been made to keep that class minimal, and use the
  * sub-objects and copy patterns explcitely instead of trying to
  * wrap any dataframe existing function.
  *
  * @param df            The dataframe handling data
  *
  * @param partition     HivePartition.  This helper class contains
  *                      database and table name, as well as external location
  *                      and partition keys and values.
  */
case class PartitionedDataFrame(df: DataFrame, partition: HivePartition) {

    /**
      * Returns a new PartitionedDataFrame with constant Hive partitions added as columns.  If any
      * column values are convertable to Ints, they will be added as an Int, otherwise String.
      *
      * @return
      */
    def applyPartitions: PartitionedDataFrame = {
      val df = this.df
      val partition = this.partition
      this.copy(df = partition.keys.foldLeft(df) {
        case (currentDf, (key: String)) =>
          val value = partition.get(key).get
          // If the partition value looks like an Int, convert it,
          // else just use as a String.  lit() will convert the Scala
          // value (Int or String here) into a Spark Column type.
          currentDf.withColumn(key, lit(allCatch.opt(value.toLong).getOrElse(value)))
      })
    }

}
