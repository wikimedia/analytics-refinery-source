/*
 * This is a simple test app that should be removed once we have a real job in this directory.
 * I am leaving it here mainly because git won't let me commit a directory tree without a file.
 */

/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Test Spark Refinery")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val webrequests = sqlContext.parquetFile("/wmf/data/wmf/webrequest/webrequest_source=mobile/year=2015/month=3/day=15/hour=0/000000_0")
    println("It has %s records".format(webrequests.count()))
  }
}
