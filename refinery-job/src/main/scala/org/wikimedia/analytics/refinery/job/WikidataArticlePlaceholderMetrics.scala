package org.wikimedia.analytics.refinery.job

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.wikimedia.analytics.refinery.core.GraphiteClient
import scopt.OptionParser
import scala.collection.mutable.Map;

/**
  * Reports metrics for the ArticlePlaceholder extension to graphite
  *
  * Usage with spark-submit:
  * spark-submit \
  * --class org.wikimedia.analytics.refinery.job.WikidataArticlePlaceholderMetrics
  * /path/to/refinery-job.jar
  * -y <year> -m <month> -d <day>
  * [-n <namespace> -t <webrequest-table> -g <graphite-host> -p <graphite-port>]
  */
object WikidataArticlePlaceholderMetrics {

  /**
    * Config class for CLI argument parser using scopt
    */
  case class Params(webrequestTable: String = "wmf.webrequest",
                    graphiteHost: String = "localhost",
                    graphitePort: Int = 2003,
                    graphiteNamespace: String = "daily.wikidata.articleplaceholder",
                    year: Int = 0, month: Int = 0, day: Int = 0)

  /**
    * Define the command line options parser
    */
  val argsParser = new OptionParser[Params]("Wikidata ArticlePlaceholder Metrics") {
    head("Wikidata ArticlePlaceholder Metrics", "")
    note("This job reports ArticlePlaceholder extension traffic to graphite daily")
    help("help") text ("Prints this usage text")

    opt[String]('t', "webrequest-table") optional() valueName ("<table>") action { (x, p) =>
      p.copy(webrequestTable = x)
    } text ("Hive webrequest table to use. Defaults to wmf.webrequest")

    opt[String]('g', "graphite-host") optional() valueName ("<path>") action { (x, p) =>
      p.copy(graphiteHost = x)
    } text ("Graphite host. Defaults to localhost")

    opt[Int]('p', "graphite-port") optional() valueName ("<port>") action { (x, p) =>
      p.copy(graphitePort = x)
    } text ("Graphite port. Defaults to 2003")

    opt[String]('n', "graphite-namespace") optional() valueName ("<graphite.namespace>") action { (x, p) =>
      p.copy(graphiteNamespace = x)
    } text ("graphite metric namespace/prefix. Defaults to daily.wikidata.articleplaceholder")

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

  }

  def main(args: Array[String]): Unit = {
    argsParser.parse(args, Params()) match {
      case Some(params) => {
        // Initial Spark setup
        val conf = new SparkConf().setAppName("WikidataArticlePlaceholderMetrics-%d-%d-%d".format(
          params.year, params.month, params.day))
        val sc = new SparkContext(conf)
        val hiveContext = new HiveContext(sc)

        // Currently limited to wikipedia as ArticlePlaceholder is only deployed to wikipedias
        val sql = """
  SELECT
    pageview_info["project"],
    agent_type,
    (referer rlike '^.*search=.*$' AND referer rlike '^.*\.wikipedia\.org.*$') as from_search,
    COUNT(1)
  FROM %s
  WHERE year = %d
    AND month = %d
    AND day = %d
    AND webrequest_source = 'text'
    AND is_pageview = TRUE
    AND x_analytics_map["ns"] = '-1'
    AND x_analytics_map["special"] = 'AboutTopic'
    AND normalized_host.project_class = 'wikipedia'
  GROUP BY
    pageview_info["project"],
    agent_type,
    (referer rlike '^.*search=.*$' AND referer rlike '^.*\.wikipedia\.org.*$')
                  """.format(params.webrequestTable, params.year, params.month, params.day)

        val queryData = hiveContext.sql(sql).collect().map(r => (r.getString(0), r.getString(1), r.getBoolean(2), r.getLong(3)))
        val time = new DateTime(params.year, params.month, params.day, 0, 0)
        val graphite = new GraphiteClient(params.graphiteHost, params.graphitePort)
        val data = Map[String, Long]()

        queryData.foreach{ case (project, agentType, fromSearch, count) => {
          val metric = "%s.varnish_requests.abouttopic.%s.%s".format(params.graphiteNamespace, agentType, project.replace('.','_'))
          data += data.get(metric).map(x => metric -> (x + count)).getOrElse(metric -> count)
          if(fromSearch) {
            val searchMetric = "%s.varnish_requests.abouttopic.search_referral.%s".format(params.graphiteNamespace, project.replace('.','_'))
            data += data.get(searchMetric).map(x => searchMetric -> (x + count)).getOrElse(searchMetric -> count)
          }
        }}

        data.foreach{ case (metric, count) => {
          graphite.sendOnce(metric, count, time.getMillis / 1000)
        }}

      }
      case None => sys.exit(1)
    }
  }

}
