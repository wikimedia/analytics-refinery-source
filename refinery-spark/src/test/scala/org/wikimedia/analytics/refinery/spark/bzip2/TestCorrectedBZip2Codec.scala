package org.wikimedia.analytics.refinery.spark.bzip2

import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{FlatSpec, Matchers}

class TestCorrectedBZip2Codec
  extends FlatSpec
  with Matchers {

  val testFileURI = getClass.getResource("/lbzip2_32767.bz2")

  "BZip2Codec" should "fail to read the test file" in {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test_bzip_fail")
      .set("spark.driver.allowMultipleContexts", "true")

    val sparkContext = new SparkContext(conf)

    an [Throwable] should be thrownBy sparkContext.textFile(testFileURI.toString).take(1)
  }

  "CorrectedBZip2Codec" should "successfully read the test file" in {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test_bzip")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.hadoop.io.compression.codecs", "org.wikimedia.analytics.refinery.spark.bzip2.CorrectedBZip2Codec")

    val sparkContext = new SparkContext(conf)
    sparkContext.textFile(testFileURI.toString).take(1).head should equal("TEST")
  }
}