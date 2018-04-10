package org.wikimedia.analytics.refinery.job

import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.wikimedia.analytics.refinery.job.connectors.GraphiteClient
import scopt.OptionParser

/**
  * Reports metrics for the wikidata Special:EntityData page to graphite
  *
  * Usage with spark-submit:
  * spark-submit \
  * --class org.wikimedia.analytics.refinery.job.WikidataSpecialEntityDataMetrics
  * /path/to/refinery-job.jar
  * -y <year> -m <month> -d <day>
  * [-n <namespace> -t <webrequest-table> -g <graphite-host> -p <graphite-port>]
  */
object WikidataSpecialEntityDataMetrics {

  /**
    * Config class for CLI argument parser using scopt
    */
  case class Params(webrequestBasePath: String = "hdfs://analytics-hadoop/wmf/data/wmf/webrequest",
                    graphiteHost: String = "localhost",
                    graphitePort: Int = 2003,
                    graphiteNamespace: String = "daily.wikidata.entitydata",
                    year: Int = 0, month: Int = 0, day: Int = 0)

  /**
    * Define the command line options parser
    */
  val argsParser = new OptionParser[Params]("Wikidata Special:EntityData Metrics") {
    head("Wikidata Special:EntityData Metrics", "")
    note("This job reports use of the wikidata Special:EntityData page to graphite daily")
    help("help") text ("Prints this usage text")

    opt[String]('t', "webrequest-base-path") optional() valueName ("<path>") action { (x, p) =>
      p.copy(webrequestBasePath = if (x.endsWith("/")) x.dropRight(1) else x)
    } text ("Path to the webrequest data in HDFS. Defaults to hdfs://analytics-hadoop/wmf/data/wmf/webrequest")

    opt[String]('g', "graphite-host") optional() valueName ("<path>") action { (x, p) =>
      p.copy(graphiteHost = x)
    } text ("Graphite host. Defaults to localhost")

    opt[Int]('p', "graphite-port") optional() valueName ("<port>") action { (x, p) =>
      p.copy(graphitePort = x)
    } text ("Graphite port. Defaults to 2003")

    opt[String]('n', "graphite-namespace") optional() valueName ("<graphite.namespace>") action { (x, p) =>
      p.copy(graphiteNamespace = x)
    } text ("graphite metric namespace/prefix. Defaults to daily.wikidata.entitydata")

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
        val appName = s"WikidataSpecialEntityDataMetrics-${params.year}-${params.month}-${params.day}"
        val spark = SparkSession.builder().appName(appName).getOrCreate()

        val webrequestTextPath = params.webrequestBasePath + "/webrequest_source=text"
        val parquetPath = "%s/year=%d/month=%d/day=%d/*".format(webrequestTextPath, params.year, params.month, params.day)
        val temporaryTable = "temporaryTable"

        spark.read.parquet(parquetPath).createOrReplaceTempView(temporaryTable)

        val sql = """
  SELECT
    COUNT(1) AS count,
    agent_type,
    content_type
  FROM %s
  WHERE http_status = 200
    AND normalized_host.project_class = 'wikidata'
    AND uri_path rlike '^/wiki/Special:EntityData/.*$'
    GROUP BY
      agent_type,
      content_type
                  """.format(temporaryTable)

        val data = spark.sql(sql).collect().map(r => (r.getLong(0), r.getString(1), r.getString(2)))

        val metrics = data.foldLeft(Map.empty[String, Long])((acc, v) => {
          v match {
            case (0L, _, _) => acc
            case (count, agentType, contentType) =>
              val formatKey = "format." + normalizeFormat( contentType )
              val agentTypeKey = "agent_types." + agentType
              acc +
                (formatKey -> (acc.withDefaultValue(0L)(formatKey) + count)) +
                (agentTypeKey -> (acc.withDefaultValue(0L)(agentTypeKey) + count))
          }
        })

        val graphite = new GraphiteClient(params.graphiteHost, params.graphitePort)
        val time = new DateTime(params.year, params.month, params.day, 0, 0)

        metrics.foreach { case (metricName, count) =>
          val metric = "%s.%s".format(params.graphiteNamespace, metricName)
          graphite.sendOnce(metric, count, time.getMillis / 1000)
        }

      }
      case None => sys.exit(1)
    }
  }

  val rdfPat = ".*(/rdf\\+xml).*".r
  val phpPat = ".*(/vnd\\.php).*".r
  val ntPat = ".*(/n\\-triples).*".r
  val n3Pat = ".*(/n3).*".r
  val jsonPat = ".*(/json).*".r
  val ttlPat = ".*(/turtle).*".r
  val htmlPat = ".*(/html).*".r

  def normalizeFormat (contentType:String): String = {
    contentType match {
      case rdfPat(_) => "rdf"
      case phpPat(_) => "php"
      case ntPat(_) => "nt"
      case n3Pat(_) => "n3"
      case jsonPat(_) => "json"
      case ttlPat(_) => "ttl"
      case htmlPat(_) => "html"
      case _ => "unknown"
    }
  }

}
