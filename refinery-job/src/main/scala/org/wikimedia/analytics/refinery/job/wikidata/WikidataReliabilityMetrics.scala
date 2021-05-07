package org.wikimedia.analytics.refinery.job.wikidata

import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.wikimedia.analytics.refinery.core.GraphiteClient
import scopt.OptionParser

import scala.collection.mutable.Map

/**
  * Reports reliability metrics for Wikidata.
  *
  * Including median page weight, median loading time, edge cache hit ratio.
  * Usage with spark-submit:
  * spark-submit \
  * --class org.wikimedia.analytics.refinery.job.WikidataReliabilityMetrics
  * /path/to/refinery-job.jar
  * -y <year> -m <month> -d <day>
  * [-n <namespace> -t <webrequest-table> -g <graphite-host> -p <graphite-port>]
  */
object WikidataReliabilityMetrics {

  /**
    * Config class for CLI argument parser using scopt
    */
  case class Params(webrequestTable: String = "wmf.webrequest",
                    graphiteHost: String = "localhost",
                    graphitePort: Int = 2003,
                    graphiteNamespace: String = "daily.wikidata.reliability_metrics",
                    year: Int = 0, month: Int = 0, day: Int = 0)

  /**
    * Define the command line options parser
    */
  val argsParser = new OptionParser[Params]("Wikidata Reliability Metrics") {
    head("Wikidata Reliability Metrics", "")
    note("This job reports reliability metrics to graphite daily")
    help("help") text ("Prints this usage text")

    opt[String]('t', "webrequest-table") optional() valueName ("<table>") action { (x, p) =>
      p.copy(webrequestTable = x)
    } text ("The webrequest table to query. Defaults to wmf.webrequest")

    opt[String]('g', "graphite-host") optional() valueName ("<host>") action { (x, p) =>
      p.copy(graphiteHost = x)
    } text ("Graphite host. Defaults to localhost")

    opt[Int]('p', "graphite-port") optional() valueName ("<port>") action { (x, p) =>
      p.copy(graphitePort = x)
    } text ("Graphite port. Defaults to 2003")

    opt[String]('n', "graphite-namespace") optional() valueName ("<graphite.namespace>") action { (x, p) =>
      p.copy(graphiteNamespace = x)
    } text ("graphite metric namespace/prefix. Defaults to daily.wikidata.reliability_metrics")

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
        val appName = s"WikidataReliabilityMetrics-${params.year}-${params.month}-${params.day}"
        val spark = SparkSession.builder().appName(appName).enableHiveSupport().getOrCreate()
        val time = new DateTime(params.year, params.month, params.day, 0, 0)
        val graphite = new GraphiteClient(params.graphiteHost, params.graphitePort)
        // Prepare data into temporary table and cache it
        spark.sql(s"""
SELECT
  uri_host,
  uri_path,
  namespace_id,
  http_status,
  is_pageview,
  response_size,
  time_firstbyte,
  cache_status
FROM ${params.webrequestTable}
WHERE
webrequest_source = 'text'
  AND year = ${params.year}
  AND month = ${params.month}
  AND day = ${params.day}
  AND uri_host IN ('www.wikidata.org', 'query.wikidata.org', 'm.wikidata.org')
  AND (is_pageview = TRUE OR uri_path LIKE '/w/api.php%' OR uri_path LIKE '%/sparql')
""").createOrReplaceTempView("wd_reliability_metrics")
        spark.table("wd_reliability_metrics").cache()

        var sql = """
SELECT
  percentile_approx(response_size, 0.5) as median_payload
FROM
  wd_reliability_metrics
WHERE
  http_status = 200
  AND uri_host IN ('www.wikidata.org', 'm.wikidata.org')
  AND namespace_id IN (0,120,146)
  AND is_pageview = TRUE"""

        val queryDataPayload = spark.sql(sql).collect().map(r => (r.getLong(0)))
        val dataPayload = Map[String, Long]()
        queryDataPayload.foreach{ case (median_payload) =>
          val metric = "%s.median_payload".format(params.graphiteNamespace)
          dataPayload += dataPayload.get(metric).map(x => metric -> (x + median_payload)).getOrElse(metric -> median_payload)
        }

        dataPayload.foreach{ case (metric, value) =>
          graphite.sendOnce(metric, value, time.getMillis / 1000)
        }

        sql = """
SELECT
  percentile_approx(time_firstbyte, 0.5) as median_time
FROM
  wd_reliability_metrics
WHERE
  uri_host IN ('www.wikidata.org', 'm.wikidata.org')
  AND namespace_id IN (0,120)
  AND http_status = 200
  AND is_pageview = TRUE
  AND cache_status = 'miss'"""

        val queryDataTime = spark.sql(sql).collect().map(r => (r.getDouble(0)))
        val dataTime = Map[String, Double]()
        queryDataTime.foreach{ case (median_time) =>
          val metric = "%s.median_time".format(params.graphiteNamespace)
          dataTime += dataTime.get(metric).map(x => metric -> (x + median_time)).getOrElse(metric -> median_time)
        }

        dataTime.foreach{ case (metric, value) =>
          graphite.sendOnce(metric, (value * 1000).toLong, time.getMillis / 1000)
        }

        sql = """
SELECT
 COUNT(*) as count,
 CASE WHEN is_pageview = 1 THEN 'entity_page'
 ELSE
  CASE WHEN uri_path LIKE '/w/api.php%' THEN 'api'
  ELSE CASE WHEN uri_host = 'query.wikidata.org' AND uri_path LIKE '%/sparql' THEN 'wdqs'
       ELSE 'other'
       END
  END
 END AS request_category,
 CASE WHEN cache_status IN ('pass','miss') THEN 'miss' ELSE 'hit' END AS is_cached
FROM
  wd_reliability_metrics
GROUP BY
 (CASE WHEN is_pageview = 1 THEN 'entity_page'
 ELSE
  CASE WHEN uri_path LIKE '/w/api.php%' THEN 'api'
  ELSE CASE WHEN uri_host = 'query.wikidata.org' AND uri_path LIKE '%/sparql' THEN 'wdqs'
       ELSE 'other'
       END
  END
 END),
  (CASE WHEN cache_status IN ('pass','miss') THEN 'miss' ELSE 'hit' END)
"""

        val queryDataCache = spark.sql(sql).collect().map(r => (r.getString(1), r.getString(2), r.getLong(0)))
        val dataCache = Map[String, Long]()
        queryDataCache.foreach{ case (request_category, is_cached, count) =>
          val metric = "%s.request_count.%s.%s".format(params.graphiteNamespace, request_category, is_cached)
          dataCache += dataCache.get(metric).map(x => metric -> (x + count)).getOrElse(metric -> count)
        }

        dataCache.foreach{ case (metric, count) =>
          graphite.sendOnce(metric, count, time.getMillis / 1000)
        }

      }
      case None => sys.exit(1)
    }
  }

}
