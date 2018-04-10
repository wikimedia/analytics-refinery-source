package org.wikimedia.analytics.refinery.job

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.wikimedia.analytics.refinery.job.connectors.{GraphiteMessage, GraphiteClient}
import scopt.OptionParser

/**
 * Reports metrics for API (rest and action) to graphite
 *
 * Usage with spark-submit:
 * spark-submit \
 * --class org.wikimedia.analytics.refinery.job.APIsVarnishRequests
 * /path/to/refinery-job.jar
 * -y <year> -m <month> -d <day> -h <hour>
 * [-r <restbaseNamespace> -a <mediawikiApiNamesapce> -w <webrequest-base-path> -g <graphite-host> -p <graphite-port>]
 */
object APIsVarnishRequests {


  /**
   * Config class for CLI argument parser using scopt
   */
  case class Params(webrequestBasePath: String = "hdfs://analytics-hadoop/wmf/data/wmf/webrequest",
                    graphiteHost: String = "localhost",
                    graphitePort: Int = 2003,
                    restbaseNamespace: String = "restbase.requests",
                    mwAPINamespace: String = "analytics.mw_api",
                    year: Int = 0, month: Int = 0, day: Int = 0, hour: Int = 0)

  /**
   * Define the command line options parser
   */
  val argsParser = new OptionParser[Params]("APIs Varnish Requests") {
    head("APIs Varnish Requests", "")
    note("This job reports RESTBase and MW_API traffic to graphite hourly")
    help("help") text ("Prints this usage text")

    opt[String]('w', "webrequest-base-path") optional() valueName ("<path>") action { (x, p) =>
      p.copy(webrequestBasePath = if (x.endsWith("/")) x.dropRight(1) else x)
    } text ("Base path to webrequest data on hadoop. Defaults to hdfs://analytics-hadoop/wmf/data/wmf/webrequest")

    opt[String]('g', "graphite-host") optional() valueName ("<path>") action { (x, p) =>
      p.copy(graphiteHost = x)
    } text ("Graphite host. Defaults to localhost")

    opt[Int]('p', "graphite-port") optional() valueName ("<path>") action { (x, p) =>
      p.copy(graphitePort = x)
    } text ("Graphite port. Defaults to 2003")

    opt[String]('r', "restbaseNamespace") optional() valueName ("<path>") action { (x, p) =>
      p.copy(restbaseNamespace = x)
    } text ("Restbase Namespace/prefix for graphite metric. Defaults to restbase.requests")

    opt[String]('a', "mwAPINamespace") optional() valueName ("<path>") action { (x, p) =>
      p.copy(mwAPINamespace = x)
    } text ("Mediawiki Namespace/prefix for graphite metric. Defaults to analytics.mw_api")


    opt[Int]('y', "year") required() action { (x, p) =>
      p.copy(year = x)
    } text ("Year as an integer")

    opt[Int]('m', "month") required() action { (x, p) =>
      p.copy(month = x)
    } validate { x => if (x > 0 & x <= 12) success else failure("Invalid month")
    } text ("Month as an integer")

    opt[Int]('d', "day") required() action { (x, p) =>
      p.copy(day = x)
    } validate { x => if (x > 0 & x <= 31) success else failure("Invalid day")
    } text ("Day of month as an integer")

    opt[Int]('h', "hour") required() action { (x, p) =>
      p.copy(hour = x)
    } validate { x => if (x >= 0 & x < 24) success else failure("Invalid hour")
    } text ("Hour of day as an integer (0-23)")

  }

  def countAPIsURIs(parquetData: DataFrame, sqlContext: SQLContext): (Long, Long) = {

    parquetData.registerTempTable("tmp_apis")
    val row = sqlContext.sql(
      """
        |SELECT
        |  SUM(CASE WHEN uri_path like '/api/rest_v1%' THEN 1 ELSE 0 END) as restbase_requests,
        |  SUM(CASE WHEN uri_path like '/w/api.php%' THEN 1 ELSE 0 END) as mwapi_requests
        |FROM tmp_apis
      """.stripMargin).collect().head
    (row.getLong(0), row.getLong(1))
  }

  def main(args: Array[String]): Unit = {
    argsParser.parse(args, Params()) match {
      case Some(params) => {
        // Initial Spark setup
        val conf = new SparkConf().setAppName("APIsVarnishRequests")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

        // Define the path to load data in Parquet format
        val parquetDataPath = "%s/webrequest_source=text/year=%d/month=%d/day=%d/hour=%d"
          .format(params.webrequestBasePath, params.year, params.month, params.day, params.hour)

        // Define time, metric, Compute request count
        val graphiteTimestamp = new DateTime(params.year, params.month, params.day, params.hour, 0).getMillis / 1000
        val restbaseMetric = "%s.varnish_requests".format(params.restbaseNamespace)
        val mwAPIMetric = "%s.varnish_requests".format(params.mwAPINamespace)
        val (restbaseRequests, mwAPIRequests) = countAPIsURIs(sqlContext.read.parquet(parquetDataPath), sqlContext)

        // Send to graphite
        val graphite = new GraphiteClient(params.graphiteHost, params.graphitePort)
        graphite.sendMany(Seq(
          GraphiteMessage(restbaseMetric, restbaseRequests, graphiteTimestamp),
          GraphiteMessage(mwAPIMetric, mwAPIRequests, graphiteTimestamp)
        ))
      }
      case None => sys.exit(1)
    }
  }

}
