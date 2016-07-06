package org.wikimedia.analytics.refinery.job

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.wikimedia.analytics.refinery.core.GraphiteClient
import scopt.OptionParser

/**
  * Reports metrics for the ArticlePlaceholder extension to graphite
  *
  * Usage with spark-submit:
  * spark-submit \
  * --class org.wikimedia.analytics.refinery.job.WikidataArticlePlaceholderMetrics
  * /path/to/refinery-job.jar
  * -y <year> -m <month> -d <day>
  * [-n <namespace> -w <data-base-path> -g <graphite-host> -p <graphite-port>]
  */
object WikidataArticlePlaceholderMetrics {


  /**
    * Config class for CLI argument parser using scopt
    */
  case class Params(pageviewTable: String = "wmf.pageview_hourly",
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

    opt[String]('t', "pageview-table") optional() valueName ("<table>") action { (x, p) =>
      p.copy(pageviewTable = x)
    } text ("Hive pageview table to use. Defaults to wmf.pageview_hourly")

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

        val sql = """
  SELECT
    project,
    SUM(view_count)
  FROM %s
  WHERE year = %d
    AND month = %d
    AND day = %d
    AND page_title LIKE 'Special:AboutTopic%%'
    AND agent_type = 'user'
  GROUP BY project
                  """.format(params.pageviewTable, params.year, params.month, params.day)

        val data = hiveContext.sql(sql).collect().map(r => (r.getString(0), r.getLong(1)))
        val time = new DateTime(params.year, params.month, params.day, 0, 0)
        val graphite = new GraphiteClient(params.graphiteHost, params.graphitePort)

        data.foreach{ case (project, count) => {
          val metric = "%s.varnish_requests.abouttopic.user.%s".format(params.graphiteNamespace, project.replace('.','_'))
          graphite.sendOnce(metric, count, time.getMillis / 1000)
        }}

      }
      case None => sys.exit(1)
    }
  }

}
