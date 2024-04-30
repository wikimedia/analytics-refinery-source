package org.wikimedia.analytics.refinery.job

import org.apache.spark.sql.functions.{col, sum, desc}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.wikimedia.analytics.refinery.tools.LogHelper
import org.wikimedia.analytics.refinery.tools.config._

import java.net.URLDecoder
import scala.collection.immutable.ListMap

object ClickstreamBuilder extends LogHelper with ConfigHelper {

    case class PageInfo(
        wikiDb: String,
        pageId: Long,
        pageTitle: String,
        pageNamespace: Int,
        pageIsRedirect: Boolean
    )

    case class Redirect(
        wikiDb: String,
        fromPageId: Long,
        toPageId: Long
    )

    case class PageLink(
        wikiDb: String,
        fromPageId: Long,
        toPageId: Long
    )

    case class ClickStream(
        wikiDb: String,
        fromPageTitle: String,
        toPageTitle: String,
        typ: String,
        count: Long
    )

    /**
      * Steps:
      *  - prepare raw tables:
      *    - page
      *      Keep only wiki_db, page_id, page_namespace, page_is_redirect, page_title
      *      Insert fake values used later
      *    - redirect
      *      Join with page (from and to) to clean and denormalize
      *    - pagelinks
      *      Join with page (from and to) to clean and denormalize
      *      Join with redirect to resolve directs
      *      Take distinct instances
      *  - Get data from pageview_actor:
      *    - (prev[referer/referer_class parsing], curr[page_title], type, count)
      *      - Joint with pages (namespace 0 only)
      *      - Joint with redirect (resolved)
      *      - Annotated (type)
      *    - Save (title_prev, title_to, type, count)
      *      - Joint with page for titles (+ no redirects, only namespace 0, and no from = to)
      */

    def listToSQLInCondition(list: Seq[String]): String = list.map(w => s"'$w'").mkString(", ")

    /**
      * Prepare a map linking project hostnames to project dbnames.
      * @return A map[hostname -> dbname]
      */
    def prepareProjectToWikiMap(
        spark: SparkSession,
        projectNamespaceTable: String,
        snapshot: String,
        wikiList: Seq[String]
    ): Map[String, String] = {

        spark.sql(
            s"""
            |SELECT DISTINCT
            |  hostname,
            |  dbname
            |FROM $projectNamespaceTable
            |WHERE snapshot = '$snapshot'
            |  AND dbname IN (${listToSQLInCondition(wikiList)})
            """.stripMargin
        ).collect.map(r => r.getString(0) -> r.getString(1)).toMap
    }

    /**
      * Prepare the pages dataset to be reused
      * @return A Dataset of PageInfo data augmented with fake pages for each wiki
      */
    def preparePages(
        spark: SparkSession,
        pageTable: String,
        snapshot: String,
        wikiList: Seq[String]
    ): Dataset[PageInfo] = {

        import spark.implicits._
        spark.sql(
            s"""
            |SELECT
            |    wiki_db AS wikiDb,
            |    page_id AS pageId,
            |    page_title AS pageTitle,
            |    page_namespace AS pageNamespace,
            |    page_is_redirect AS pageIsRedirect
            |FROM $pageTable
            |WHERE snapshot = '$snapshot'
            |    AND wiki_db IN (${listToSQLInCondition(wikiList)})
            |    AND page_id IS NOT NULL
            |    AND page_id > 0
            |GROUP BY
            |    wiki_db,
            |    page_id,
            |    page_title,
            |    page_namespace,
            |    page_is_redirect
            """.stripMargin
        ).as[PageInfo].union(
            // insert rows for our special prev pages this will let us work with ids
            // instead of titles later, which is much less error prone
            wikiList.flatMap(wiki => Seq(
                PageInfo(wiki, -1L, "other-empty", 0, pageIsRedirect = false),
                PageInfo(wiki, -2L, "other-internal",0, pageIsRedirect = false),
                PageInfo(wiki, -3L, "other-external",0, pageIsRedirect = false),
                PageInfo(wiki, -4L, "other-search",0, pageIsRedirect = false),
                PageInfo(wiki, -5L, "other-other",0, pageIsRedirect = false))
            ).toDS()
        )
    }

    /**
      * Prepare the redirects dataset to be reused
      * @return A Dataset of redirect data
      */
    def prepareRedirects(
        spark: SparkSession,
        redirectTable: String,
        snapshot: String,
        wikiList: Seq[String],
        pages: Dataset[PageInfo]
    ): Dataset[Redirect] = {

        import spark.implicits._
        spark.sql(
            s"""
            |SELECT
            |    wiki_db,
            |    rd_from,
            |    rd_title,
            |    rd_namespace
            |FROM $redirectTable
            |WHERE snapshot = '$snapshot'
            |    AND wiki_db IN (${listToSQLInCondition(wikiList)})
            |    AND rd_from IS NOT NULL
            |    AND rd_from > 0
            |    AND rd_title IS NOT NULL
            |GROUP BY
            |    wiki_db,
            |    rd_from,
            |    rd_title,
            |    rd_namespace
            """.stripMargin
        )
            // ensure fromPageId exists in the page table
            .join(pages, col("wiki_db") === pages("wikiDb") && col("rd_from") === pages("pageId"), "left_semi")
            // Get toPageId from page-tile and page page-namespace
            .join(
                pages,
                col("wiki_db") === pages("wikiDb") &&
                    col("rd_title") === pages("pageTitle") &&
                    col("rd_namespace") === pages("pageNamespace"))
            .selectExpr("wikiDb", "rd_from AS fromPageId", "pageId as toPageId")
            .distinct().as[Redirect]
    }


    /**
      * Prepare the pagelinks dataset to be reused
      * Note: links that end in a redirect are 'resolved' in this dataset.
      *       This means that if A links to B, and B redirects to C, we replace the link (A,B) with (A,C).
      *       This lets us properly annotate link types after resolving redirects in the clickstream, since
      *       a user will experience following A as if it linked to C.
      * IMPORTANT: The 'distinct' at the end ensures that each link only occurs once
      * @return A Dataset of pagelinks data.
      */
    def preparePagelinks(
        spark: SparkSession,
        pagelinksTable: String,
        linktargetTable: String,
        snapshot: String,
        wikiList: Seq[String],
        pages: Dataset[PageInfo],
        redirects: Dataset[Redirect]
    ): Dataset[PageLink] = {

        import spark.implicits._
        spark.sql(
            s"""
            |WITH linktargets AS (
            |    SELECT
            |        wiki_db,
            |        lt_id,
            |        lt_namespace,
            |        lt_title
            |    FROM $linktargetTable
            |    WHERE
            |        snapshot = '$snapshot'
            |        AND wiki_db IN (${listToSQLInCondition(wikiList)})
            |),
            |pagelinks AS (
            |    SELECT
            |        wiki_db,
            |        pl_from,
            |        pl_target_id
            |    FROM $pagelinksTable
            |    WHERE
            |        snapshot = '$snapshot'
            |        AND wiki_db IN (${listToSQLInCondition(wikiList)})
            |    AND pl_from IS NOT NULL
            |    AND pl_from > 0
            |)
            |
            |SELECT
            |    pl.wiki_db AS wiki_db,
            |    pl.pl_from AS pl_from,
            |    lt.lt_namespace AS lt_namespace,
            |    lt.lt_title AS lt_title
            |FROM pagelinks pl
            |    JOIN linktargets lt
            |        ON pl.wiki_db = lt.wiki_db
            |            AND pl.pl_target_id = lt.lt_id
            """.stripMargin
        )
            // ensure fromPageId exists in the page table
            .join(pages, col("wiki_db") === pages("wikiDb") && col("pl_from") === pages("pageId"), "left_semi")
            // Get toPageId from page-tile and page page-namespace
            .join(
                pages,
                col("wiki_db") === pages("wikiDb") &&
                    col("lt_title") === pages("pageTitle") &&
                    col("lt_namespace") === pages("pageNamespace"))
            .selectExpr("wiki_db", "pl_from", "pageId AS noRedirectToPageId")
            // Join with redirects adding one level of redirect
            .join(
                redirects,
                col("wiki_db") === redirects("wikiDb") &&
                    col("noRedirectToPageId") === redirects("fromPageId"),
                "left_outer")
            .selectExpr("wiki_db as wikiDb", "pl_from AS fromPageId", "COALESCE(toPageId, noRedirectToPageId) AS toPageId")
            .distinct().as[PageLink]
    }

    /**
      * Prepare the actual clickstream dataset
      * @return A RDD of clickstream data.
      */
    def prepareClickstream(
        spark: SparkSession,
        pageviewActorTable: String,
        year: Int,
        month: Int,
        projectToWikiMap: Map[String, String],
        pages: Dataset[PageInfo],
        redirects: Dataset[Redirect],
        pageLinks: Dataset[PageLink],
        minCount: Int
    ): Dataset[ClickStream] = {

        import spark.implicits._
        // Prepare wiki-project -> domain lists (one without suffix, one with suffix and mobile
        val domainList = projectToWikiMap.keys.toList
        val projectList = domainList.map(_.stripSuffix(".org"))
        val domainAndMobileList = domainList.flatMap(p => {
            val firstDotIndex = p.indexOf('.')
            Seq(p, p.substring(0, firstDotIndex) + ".m" + p.substring(firstDotIndex))
        })

        // Prepare 2 udfs needed for data preparation
        spark.udf.register("project_to_wiki", (project: String) => projectToWikiMap(project))
        spark.udf.register("url_decode", (path: String) => URLDecoder.decode(path, "UTF-8"))

        spark.sql(
            s"""
            |SELECT
            |    project_to_wiki(CONCAT(pageview_info['project'], '.org')) AS wiki_db,
            |    CASE
            |        -- empty or malformed referer
            |        WHEN referer IS NULL THEN 'other-empty'
            |        WHEN referer == '' THEN 'other-empty'
            |        WHEN referer == '-' THEN 'other-empty'
            |        WHEN parse_url(referer,'HOST') is NULL THEN 'other-empty'
            |        -- internal referer from the same wikipedia
            |        WHEN parse_url(referer,'HOST') in (${listToSQLInCondition(domainAndMobileList)})
            |                AND LENGTH(REGEXP_EXTRACT(parse_url(referer,'PATH'), '/wiki/(.*)', 1)) > 1
            |            THEN REPLACE(url_decode(REGEXP_EXTRACT(parse_url(referer,'PATH'), '/wiki/(.*)', 1)), ' ', '_')
            |        -- other referers
            |        WHEN referer_class = 'internal' THEN 'other-internal'
            |        WHEN referer_class = 'external' THEN 'other-external'
            |        WHEN referer_class = 'external (search engine)' THEN 'other-search'
            |        ELSE 'other-other'
            |    END as fromTitle,
            |    pageview_info['page_title'] as toTitle,
            |    COUNT(1) as count
            |FROM $pageviewActorTable
            |WHERE year = $year AND month = $month
            |    AND pageview_info['project'] IN (${listToSQLInCondition(projectList)})
            |    AND is_pageview
            |    AND agent_type = 'user'
            |GROUP BY
            |    project_to_wiki(CONCAT(pageview_info['project'], '.org')),
            |    CASE
            |        -- empty or malformed referer
            |        WHEN referer IS NULL THEN 'other-empty'
            |        WHEN referer == '' THEN 'other-empty'
            |        WHEN referer == '-' THEN 'other-empty'
            |        WHEN parse_url(referer,'HOST') is NULL THEN 'other-empty'
            |        -- internal referer from the same wikipedia
            |        WHEN parse_url(referer,'HOST') in (${listToSQLInCondition(domainAndMobileList)})
            |                AND LENGTH(REGEXP_EXTRACT(parse_url(referer,'PATH'), '/wiki/(.*)', 1)) > 1
            |            THEN REPLACE(url_decode(REGEXP_EXTRACT(parse_url(referer,'PATH'), '/wiki/(.*)', 1)), ' ', '_')
            |        -- other referers
            |        WHEN referer_class = 'internal' THEN 'other-internal'
            |        WHEN referer_class = 'external' THEN 'other-external'
            |        WHEN referer_class = 'external (search engine)' THEN 'other-search'
            |        ELSE 'other-other'
            |    END,
            |    pageview_info['page_title']
            """.stripMargin
        )

            // Get fromPageId instead of fromPageTitle
            .join(pages, col("wiki_db") === pages("wikiDb") && col("fromTitle") === pages("pageTitle"))
            .where("pageNamespace = 0")
            .selectExpr("wiki_db", "pageId AS from_page_id", "toTitle", "count")

            // Get toPageId instead of toPageTitle
            .join(pages, col("wiki_db") === pages("wikiDb") && col("toTitle") === pages("pageTitle"))
            .where("pageNamespace = 0")
            .selectExpr("wiki_db", "from_page_id", "pageId AS to_page_id", "count")

            // Resolve one level of redirect (to_page_id --> to_redirect_pageId)
            .join(redirects, col("wiki_db") === redirects("wikiDb") && col("to_page_id") === redirects("fromPageId"), "left_outer")
            .selectExpr("wiki_db", "from_page_id", "COALESCE(toPageId, to_page_id) AS to_page_id", "count")

            // Re-aggregate after redirect resolution
            // filter not frequent-enough pairs and source = dest
            .groupBy("wiki_db", "from_page_id", "to_page_id")
            .agg(sum("count").as("count"))
            .where(s"count >= $minCount AND from_page_id <> to_page_id")

            // Join with pagelinks to add link-typ
            .join(
                pageLinks,
                col("wiki_db") === pageLinks("wikiDb") &&
                    col("from_page_id") === pageLinks("fromPageId") &&
                    col("to_page_id") === pageLinks("toPageId"),
                "left_outer")
            .selectExpr("wiki_db", "from_page_id", "to_page_id",
                """|CASE
                   |    WHEN from_page_id < 0 THEN 'external'
                   |    WHEN fromPageId IS NOT NULL THEN 'link'
                   |    ELSE 'other'
                   |END AS typ""".stripMargin, "count")

            // Join back to pages to present titles instead of Ids
            // Filter redirect pages and namespace not zero
            .join(pages, col("wiki_db") === pages("wikiDb") && col("from_page_id") === pages("pageId"))
            .where("pageNamespace = 0 AND NOT pageIsRedirect")
            .selectExpr("wiki_db", "pageTitle AS fromPageTitle", "to_page_id", "typ", "count")
            .join(pages, col("wiki_db") === pages("wikiDb") && col("to_page_id") === pages("pageId"))
            .where("pageNamespace = 0 AND NOT pageIsRedirect")
            .selectExpr("wiki_db AS wikiDb", "fromPageTitle", "pageTitle AS toPageTitle", "typ", "count")
            .as[ClickStream]
    }

    /**
      * Config class for CLI argument parser using scopt
      */
    case class Config(
        year: Int,
        month: Int,
        output_base_path: String,
        wiki_list: Seq[String],

        project_namespace_table: String = "wmf_raw.mediawiki_project_namespace_map",
        page_table: String = "wmf_raw.mediawiki_page",
        redirect_table: String = "wmf_raw.mediawiki_redirect",
        pagelinks_table: String = "wmf_raw.mediawiki_pagelinks",
        linktarget_table: String = "wmf_raw.mediawiki_private_linktarget",
        pageview_actor_table: String = "wmf.pageview_actor",
        spark_partitions: Int = 1024,
        output_files_parts: Int = 1,
        minimun_link_count: Int = 10,
    )

    object Config {
        // This is just used to ease generating help message with default values.
        // Required configs are set to dummy values so that validate() will succeed.
        val default: Config = new Config(
            year = 0,
            month = 0,
            output_base_path = "dummy/path",
            wiki_list = Seq.empty[String]
        )

        val propertiesDoc: ListMap[String, String] = ListMap(
            "config_file <file1.properties,files2.properties>" ->
                """Comma separated list of paths to properties files. These files parsed for
                  |for matching config parameters as defined here.""",
            "year" -> "The year to use for pageview data gathering and snapshot definition.",
            "month" -> "The year to use for pageview data gathering and snapshot definition.",
            "output_base_path" ->
                """The folder Where to store the computed datasets (one subfolder per wiki)""",
            "wiki_list" ->
                """wiki dbs to compute clickstream for.""",
            "project_namespace_table" ->
                s"Fully qualified name of the project-namespace table. Default: ${default.project_namespace_table}",
            "page_table" ->
                s"Fully qualified name of the mediawiki page table. Default: ${default.page_table}",
            "redirect_table" ->
                s"Fully qualified name of the mediawiki redirect table. Default: ${default.redirect_table}",
            "pagelinks_table" ->
                s"Fully qualified name of the mediawiki pagelinks table. Default: ${default.pagelinks_table}",
            "linktarget_table" ->
                s"Fully qualified name of the mediawiki linktarget table. Default: ${default.linktarget_table}",
            "pageview_actor_table" ->
                s"Fully qualified name of the pageview_actor table. Default: ${default.pageview_actor_table}",
            "spark_partitions" ->
                s"Number of partitions Spark uses to distribute work. Default: ${default.spark_partitions}",
            "output_files_parts" ->
                s"Number of file parts to output in hdfs per project. Default: ${default.output_files_parts}",
            "minimun_link_count" ->
                s"The minimum count for a link to appear in the dataset. Default to ${default.minimun_link_count}"
        )

        val usage: String =
            """
              |This job computes a clickstream dataset from one or more wiki(s).
              |It creates per-wiki folders to store the results.
              |
              |Example:
              |  spark3-submit --class org.wikimedia.analytics.refinery.job.ClickstreamBuilder refinery-job.jar \
              |  # read configs out of this file
              |   --config_file                     /etc/refinery/clickstream.properties \
              |   # Override and/or set other configs on the CLI
              |   --year                      2024 \
              |   --month                     4 \
              |   --output_base_path          /wmf/tmp/analytics/clickstream \
              |   --wiki_list                 enwiki,ruwiki,dewiki,eswiki
              |""".stripMargin

        /**
          * Loads Config from args
          */
        def apply(args: Array[String]): Config = {
            val config = try {
                configureArgs[Config](args)
            } catch {
                case e: ConfigHelperException => {
                    log.fatal(e.getMessage + ". Aborting.")
                    sys.exit(1)
                }
            }
            log.info("Loaded Clickstream config:\n" + prettyPrint(config))
            config
        }
    }


    def main(args: Array[String]): Unit = {
        if (args.contains("--help")) {
            println(help(Config.usage, Config.propertiesDoc))
            sys.exit(0)
        }

        val spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()

        adjustLoggingLevelToSparkMaster(spark)

        val config = Config(args)

        apply(spark)(config)
    }

    def apply(spark: SparkSession)(config: Config): Unit = {
        // Set number of partitions
        spark.sql(s"SET spark.sql.shuffle.partitions = ${config.spark_partitions}")

        val snapshot = f"${config.year}%04d-${config.month}%02d"

        val pages = preparePages(spark, config.page_table, snapshot, config.wiki_list).cache()
        val redirects = prepareRedirects(spark, config.redirect_table, snapshot, config.wiki_list, pages).cache()
        val pageLinks = preparePagelinks(spark, config.pagelinks_table, config.linktarget_table, snapshot, config.wiki_list, pages, redirects)
        val projectToWikiMap = prepareProjectToWikiMap(spark, config.project_namespace_table, snapshot, config.wiki_list)

        prepareClickstream(
            spark,
            config.pageview_actor_table,
            config.year,
            config.month,
            projectToWikiMap,
            pages,
            redirects,
            pageLinks,
            config.minimun_link_count
        )
            .repartition(col("wikiDb"))
            .sortWithinPartitions(desc("count"), col("fromPageTitle"), col("toPageTitle"))
            .write
            .mode(SaveMode.Overwrite)
            .partitionBy("wikiDb")
            .options(Map(
                "sep" -> "\t",
                "header" -> "false",
                "compression" -> "gzip",
                "emptyValue" -> ""
            ))
            .csv(config.output_base_path)
    }
}
