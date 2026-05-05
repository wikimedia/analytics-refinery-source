package org.wikimedia.analytics.refinery.job.mwhistory

import org.apache.spark.sql.SparkSession

/** Stub — verifies this module compiles against Spark 3.5 + Iceberg 1.10.1. */
object MWHistoryIncrementalWriter {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    spark.stop()
  }
}
