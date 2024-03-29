package org.wikimedia.analytics.refinery.job.wikidata

import org.apache.spark.sql.{SaveMode, SparkSession}
import scopt.OptionParser

/**
 * FIXME Deprecated by analytics-refinery/hql/wikidata/item_page_link/weekly
 * Generate a wikidata_item_page_link snapshot from a wikidata entity table.
 * Data is saved in parquet-format in a partition-folder. Partition addition
 * to the related table is done in oozie.
 *
 * Page-item links are links between a wikidata item and its related wikipedia pages
 * in various languages.
 *
 * Example usage:
 *
 * sudo -u analytics spark2-submit \
 *     --master yarn \
 *     --deploy-mode cluster \
 *     --executor-memory 16G \
 *     --driver-memory 8G \
 *     --executor-cores 4 \
 *     --conf spark.dynamicAllocation.maxExecutors=64 \
 *     --conf spark.executor.memoryOverhead=4096 \
 *     --class org.wikimedia.analytics.refinery.job.wikidata.WikidataPageItemLink \
 *     /srv/deployments/analytics/refinery/artifacts/refinery-job.jar \
 *     --wikidata-snapshot 2020-01-13 \
 *     --history-snapshot 2020-01 \
 *     --output-path /tmp/test_item_page_link \
 *
 */
object WikidataPageItemLink {

    /**
     * Config class for CLI argument parser using scopt
     */
    case class Params(
        wikidataEntityTable: String = "wmf.wikidata_entity",
        mwProjectNamespaceMapTable: String = "wmf_raw.mediawiki_project_namespace_map",
        mwPageHistoryTable: String = "wmf.mediawiki_page_history",
        eventPageMoveTable: String = "event.mediawiki_page_move",
        numWorkPartitions: Int = 512,
        numOutputPartitions: Int = 64,
        wikidataSnapshot: String = "",
        historySnapshot: String = "",
        outputPath: String = ""
    )

    val argsParser = new OptionParser[Params]("Wikidata Item-Page Links") {
        note("Generate wikidata page-item links from a wikidata-entity table")
        help("help") text ("Prints this usage text")

        opt[String]('w', "wikidata-entity-table") optional() valueName ("<wikidata_entity_table>") action { (x, p) =>
            p.copy(wikidataEntityTable = x)
        } text ("The wikidata-entity table to query. Defaults to wmf.wikidata_entity")

        opt[String]('n', "namespace-table") optional() valueName ("<namespace_table>") action { (x, p) =>
            p.copy(mwProjectNamespaceMapTable = x)
        } text ("The namespace map table to query. Defaults to wmf_raw.mediawiki_project_namespace_map")

        opt[String]('p', "page-history-table") optional() valueName ("<page_history_table>") action { (x, p) =>
            p.copy(mwPageHistoryTable = x)
        } text ("The page-history table to query. Defaults to wmf.mediawiki_page_history")

        opt[String]('m', "page-move-table") optional() valueName ("<page_move_table>") action { (x, p) =>
            p.copy(eventPageMoveTable = x)
        } text ("Table containing page move events. Defaults to event.mediawiki_page_move")

        opt[Int]('k', "num-work-partitions") optional() action { (x, p) =>
            p.copy(numWorkPartitions = x)
        } text ("Number of work partitions (computation parallelism) for the job. Defaults to 256")

        opt[Int]('t', "num-output-partitions") optional() action { (x, p) =>
            p.copy(numOutputPartitions = x)
        } text ("Number of output partitions (output files) for the job. Defaults to 64")

        opt[String]('s', "wikidata-snapshot") required() valueName ("<wikidata-snapshot>") action { (x, p) =>
            p.copy(wikidataSnapshot = x)
        } text ("Snapshot of the wikidata-entity table to query. Usually in format YYYY-MM-DD")

        opt[String]('h', "history-snapshot") required() valueName ("<history-snapshot>") action { (x, p) =>
            p.copy(historySnapshot = x)
        } text ("Snapshot of the tables to query as an integer")

        opt[String]('o', "output-path") required() valueName ("<path>") action { (x, p) =>
            p.copy(outputPath = if (x.endsWith("/")) x.dropRight(1) else x)
        } text ("Where to write the computed parquet files.")
    }

    def main(args: Array[String]): Unit = {
        argsParser.parse(args, Params()) match {
            case None => sys.exit(1)
            case Some(params) =>
                // Initial Spark setup
                val appName = s"WikidataPageItemLink-${params.wikidataSnapshot}"
                val spark = SparkSession.builder().appName(appName).enableHiveSupport().getOrCreate()

                spark.sql(s"SET spark.sql.shuffle.partitions=${params.numWorkPartitions}")

                // Below, we join to the eventPageMoveTable to get page titles since
                // the last history snapshot, because these change quite rapidly.  This
                // improves the ability to link to wikidata items, but it does not make it
                // perfect.  One problem that remains is pages that have been deleted since
                // the history snapshot will remain and effectively duplicate links to wikidata
                // items.  This could be remedied by joining to mediawiki_page_delete but then
                // we'd have to also join to page_restore and it gets complicated.  Instead,
                // we propose to wait until we have a more comprehensive incremental update
                // of mediawiki history.
                val sql = s"""
WITH

snapshot_page_titles AS (

  SELECT DISTINCT
    wiki_db,
    page_id,
    first_value(page_title) OVER w AS page_title,
    first_value(page_namespace) OVER w AS page_namespace
  FROM ${params.mwPageHistoryTable}
  WHERE snapshot = '${params.historySnapshot}'
    AND page_id IS NOT NULL AND page_id > 0
    AND page_title IS NOT NULL and LENGTH(page_title) > 0
  WINDOW w AS (
    PARTITION BY
      wiki_db,
      page_id
    ORDER BY
      start_timestamp DESC, -- If events have the same timestamp
      source_log_id DESC,   -- Use biggest source_log_id. If same source_log_id
      caused_by_event_type  -- then use create instead of delete.
  )
),

event_page_titles AS (
  SELECT DISTINCT
    `database` AS wiki_db,
    page_id,
    first_value(page_title) OVER w AS page_title,
    first_value(page_namespace) OVER w AS page_namespace
  FROM ${params.eventPageMoveTable}
  WHERE page_id IS NOT NULL AND page_id > 0
    AND page_title IS NOT NULL and LENGTH(page_title) > 0
    AND concat(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0'))
        between
            -- Since history snapshot includes that month we start looking 1 month later
            TO_DATE(concat('${params.historySnapshot}', '-01')) + INTERVAL 1 MONTH
            and
            -- wikidata snapshot includes that week, so look 1 week later
            TO_DATE('${params.wikidataSnapshot}') + INTERVAL 7 DAYS

  WINDOW w AS (
    PARTITION BY
      `database`,
      page_id
    ORDER BY
      meta.dt DESC
  )
),

current_page_titles AS (
  SELECT s.wiki_db,
    s.page_id,
    coalesce(u.page_title, s.page_title) as page_title,
    coalesce(u.page_namespace, s.page_namespace) as page_namespace
  FROM snapshot_page_titles s
    LEFT JOIN event_page_titles u
      ON (
        s.wiki_db = u.wiki_db
        AND s.page_id = u.page_id
      )
),

localized_namespace_titles AS (

  SELECT
    wiki_db,
    page_id,
    page_title,
    page_namespace,
    CASE WHEN (LENGTH(namespace_localized_name) > 0)
      THEN CONCAT(namespace_localized_name, ':', page_title)
      ELSE page_title
    END AS page_title_localized_namespace
  FROM current_page_titles cpt
    INNER JOIN ${params.mwProjectNamespaceMapTable} nsm
      ON (
        cpt.wiki_db = nsm.dbname
        AND cpt.page_namespace = nsm.namespace
        AND nsm.snapshot = '${params.historySnapshot}'
      )
),

wikidata_sitelinks AS (
  SELECT
    id as item_id,
    EXPLODE(siteLinks) AS sitelink
  FROM ${params.wikidataEntityTable}
  WHERE snapshot = '${params.wikidataSnapshot}'
    AND size(siteLinks) > 0
)

SELECT
  item_id,
  wiki_db,
  page_id,
  page_title,
  page_namespace,
  page_title_localized_namespace
FROM wikidata_sitelinks ws
  INNER JOIN localized_namespace_titles lnt
    ON (
      ws.sitelink.site = lnt.wiki_db
      AND REPLACE(ws.sitelink.title, ' ', '_') = page_title_localized_namespace
    )
"""
                spark.sql(sql).repartition(params.numOutputPartitions)
                    .write.mode(SaveMode.Overwrite).parquet(params.outputPath)
        }
    }
}
