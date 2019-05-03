package org.wikimedia.analytics.refinery.job.wikidata

import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.wikimedia.analytics.refinery.core.GraphiteClient
import scopt.OptionParser

import scala.collection.mutable.Map

/**
  * Reports metrics for Wikidata co-editors to graphite
  *
  */
object CoEditors {
    /**
      * Config class for CLI argument parser using scopt
      */
    case class Params(
        mwProjectNamespaceMapTable: String = "wmf_raw.mediawiki_project_namespace_map",
        mwHistoryTable: String = "wmf.mediawiki_history",
        graphiteHost: String = "localhost",
        graphitePort: Int = 2003,
        graphiteNamespace: String = "monthly.wikidata.coeditors",
        year: Int = 0, month: Int = 0
    )

    val argsParser = new OptionParser[Params]("Wikidata Editors Metrics") {
        head("Wikidata Co-Editors Metrics", "")
        note("This job reports Wikidata co-editors data to graphite monthly")
        help("help") text ("Prints this usage text")

        opt[String]('s', "namespace-table") optional() valueName ("<namespace_table>") action { (x, p) =>
            p.copy(mwProjectNamespaceMapTable = x)
        } text ("The namespace map table to query. Defaults to wmf_raw.mediawiki_project_namespace_map")

        opt[String]('h', "history-table") optional() valueName ("<history_table>") action { (x, p) =>
            p.copy(mwHistoryTable = x)
        } text ("The history table to query. Defaults to wmf.mediawiki_history")

        opt[String]('g', "graphite-host") optional() valueName ("<host>") action { (x, p) =>
            p.copy(graphiteHost = x)
        } text ("Graphite host. Defaults to localhost")

        opt[Int]('p', "graphite-port") optional() valueName ("<port>") action { (x, p) =>
            p.copy(graphitePort = x)
        } text ("Graphite port. Defaults to 2003")

        opt[String]('n', "graphite-namespace") optional() valueName ("<graphite.namespace>") action { (x, p) =>
            p.copy(graphiteNamespace = x)
        } text ("graphite metric namespace/prefix. Defaults to daily.wikidata.coeditors")

        opt[Int]('y', "year") required() action { (x, p) =>
            p.copy(year = x)
        } text ("Year as an integer")

        opt[Int]('m', "month") required() action { (x, p) =>
            p.copy(month = x)
        } validate { x => if (x > 0 & x <= 12) success else failure("Invalid month")
        } text ("Month as an integer")
    }

    def main(args: Array[String]): Unit = {
      argsParser.parse(args, Params()) match {
        case Some(params) => {
          // Initial Spark setup
          val appName = s"WikidataEditorsMetrics-${params.year}-${params.month}"
          val spark = SparkSession.builder().appName(appName).enableHiveSupport().getOrCreate()

          val sql = s"""
          WITH
          wikipedias AS (
            SELECT
              DISTINCT dbname
            FROM ${params.mwProjectNamespaceMapTable}
            WHERE snapshot = '${params.year}-${"%02d".format(params.month)}'
              AND hostname LIKE '%wikipedia.org'
          ),

          wikidata_editors AS (
            SELECT
              DISTINCT event_user_text
            FROM ${params.mwHistoryTable}
            WHERE snapshot = '${params.year}-${"%02d".format(params.month)}'
              AND event_entity = 'revision'
              AND event_type = 'create'
              AND wiki_db = 'wikidatawiki'
              AND NOT revision_is_deleted_by_page_deletion
              AND NOT event_user_is_anonymous
              AND NOT ARRAY_CONTAINS(event_user_groups, 'bot')
              AND event_timestamp RLIKE '^${params.year}-${"%02d".format(params.month)}.*'
          )

          SELECT
            mwh.wiki_db,
            COUNT(DISTINCT mwh.event_user_text) as wikidata_coeditors
          FROM ${params.mwHistoryTable} mwh
            JOIN wikipedias w ON (mwh.wiki_db = w.dbname)
            JOIN wikidata_editors wde ON (mwh.event_user_text = wde.event_user_text)
          WHERE snapshot = '${params.year}-${"%02d".format(params.month)}'
            AND event_entity = 'revision'
            AND event_type = 'create'
            AND NOT revision_is_deleted_by_page_deletion
            AND NOT mwh.event_user_is_anonymous
            AND NOT ARRAY_CONTAINS(mwh.event_user_groups, 'bot')
            AND mwh.event_timestamp RLIKE '^${params.year}-${"%02d".format(params.month)}.*'
          GROUP BY
            mwh.wiki_db
          ORDER BY wikidata_coeditors DESC
          LIMIT 1200"""

          val queryData = spark.sql(sql).collect().map(r => (r.getString(0), r.getLong(1)))
          val time = new DateTime(params.year, params.month, 1, 0, 0)
          val graphite = new GraphiteClient(params.graphiteHost, params.graphitePort)
          val data = Map[String, Long]()

          queryData.foreach{ case (project, count) =>
            val metric = "%s.%s".format(params.graphiteNamespace, project)
            data += data.get(metric).map(x => metric -> (x + count)).getOrElse(metric -> count)
          }

          data.foreach{ case (metric, count) =>
            graphite.sendOnce(metric, count, time.getMillis / 1000)
          }

        }
        case None => sys.exit(1)
      }
    }
}
