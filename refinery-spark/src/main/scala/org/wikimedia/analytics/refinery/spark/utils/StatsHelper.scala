package org.wikimedia.analytics.refinery.spark.utils

trait StatsHelper {

  import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
  import org.apache.spark.sql.{DataFrame, Row, SparkSession}

  import collection.JavaConverters._

  implicit def sumLongs(a: Long, b: Long) = a + b

  @transient val spark: SparkSession
  @transient val statsAccumulator: Option[MapAccumulator[String, Long]]

  val statsSchema = StructType(Seq(
    StructField("wiki_db", StringType, nullable = false),
    StructField("statistic", StringType, nullable = false),
    StructField("value", LongType, nullable = false)
  ))

  def addOptionalStat(key: String, value: Long): Unit = {
    statsAccumulator.foreach(_.add(key, value))
  }

  def statsDataframe: Option[DataFrame] = {
    statsAccumulator.map(statAcc => {
      // Split accumulator value using first dot
      val statsRows = statAcc.value.asScala.toSeq.map { case (wikiAndMetric, value) =>
        val idx = wikiAndMetric.indexOf(".")
        assert(idx > 0)
        val wikiDb = wikiAndMetric.substring(0, idx)
        val metric = wikiAndMetric.substring(idx + 1, wikiAndMetric.length)
        Row(wikiDb, metric, value)
      }
      val statsRdd = spark.sparkContext.parallelize(statsRows.toSeq)
      spark.createDataFrame(statsRdd, statsSchema)
    })
  }

}
