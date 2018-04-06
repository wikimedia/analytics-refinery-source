package org.wikimedia.analytics.refinery.spark.utils

trait StatsHelper {

  import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
  import org.apache.spark.sql.{DataFrame, Row, SparkSession}

  import collection.JavaConverters._

  val spark: SparkSession

  implicit def sumLongs(a: Long, b: Long) = a + b

  // Metrics are defined as follow: wikiDb.metricName
  // For success/failures, we use wikiDb.metricsOK / wikiDb.metricsKO
  val statsAccumulator = new MapAccumulator[String, Long]
  spark.sparkContext.register(statsAccumulator, "statistics")

  val statsSchema = StructType(Seq(
    StructField("wiki_db", StringType, nullable = false),
    StructField("statistic", StringType, nullable = false),
    StructField("value", LongType, nullable = false)
  ))

  def statsDataframe: DataFrame = {
    // Split accumulator value using first dot
    val statsRows = statsAccumulator.value.asScala.toSeq.map{ case (wikiAndMetric, value) =>
      val idx = wikiAndMetric.indexOf(".")
      assert(idx > 0)
      val wikiDb = wikiAndMetric.substring(0, idx)
      val metric = wikiAndMetric.substring(idx+1, wikiAndMetric.length)
      Row(wikiDb, metric, value)
    }
    val statsRdd = spark.sparkContext.parallelize(statsRows.toSeq)
    spark.createDataFrame(statsRdd, statsSchema)
  }

}
