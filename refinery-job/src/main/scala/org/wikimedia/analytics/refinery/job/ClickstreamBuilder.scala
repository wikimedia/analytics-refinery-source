package org.wikimedia.analytics.refinery.job

object ClickstreamBuilder {

  import org.apache.spark.sql.hive.HiveContext
  import org.apache.spark.SparkConf
  import org.apache.spark.sql.SaveMode
  import org.apache.spark.SparkContext
  import scopt.OptionParser
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.SQLContext

  case class PageInfo(
                       wikiDb: String,
                       pageId: Long,
                       pageTitle: String,
                       pageNamespace: Long,
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
                        ) {
    def toTSVLine = s"$fromPageTitle\t$toPageTitle\t$typ\t$count"
  }

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
    *  - Get data from Webrequest:
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
                               sqlContext: SQLContext,
                               projectNamespaceTable: String,
                               snapshot: String,
                               wikiList: Seq[String]
                             ): Map[String, String] = {
    sqlContext.sql(
      s"""
         |SELECT DISTINCT
         |  hostname,
         |  dbname
         |FROM $projectNamespaceTable
         |WHERE snapshot = '$snapshot'
         |  AND dbname IN (${listToSQLInCondition(wikiList)})
        """.stripMargin).
      rdd.
      collect.
      map(r => r.getString(0) -> r.getString(1)).toMap
  }

  /**
    * Prepare the pages dataset to be reused
    * @return A RDD of PageInfo data augmented with fake pages for each wiki
    */
  def preparePages(
                    sqlContext: SQLContext,
                    pageTable: String,
                    snapshot: String,
                    wikiList: Seq[String]
                  ): RDD[PageInfo] = {
    sqlContext.sql(s"""
                      |SELECT
                      |  wiki_db,
                      |  page_id,
                      |  page_title,
                      |  page_namespace,
                      |  page_is_redirect
                      |FROM $pageTable
                      |WHERE snapshot = '$snapshot'
                      |  AND wiki_db IN (${listToSQLInCondition(wikiList)})
                      |  AND page_id IS NOT NULL
                      |  AND page_id > 0
                      |GROUP BY
                      |  wiki_db,
                      |  page_id,
                      |  page_title,
                      |  page_namespace,
                      |  page_is_redirect
        """.stripMargin).
      rdd.
      map(r => {
        PageInfo(r.getString(0), r.getLong(1), r.getString(2), r.getLong(3), r.getBoolean(4))
      }).
      // insert rows for our special prev pages this will let us work with ids
      // instead of titles later, which is much less error prone
      union(
        sqlContext.sparkContext.parallelize(wikiList.flatMap(wiki => Seq(
          PageInfo(wiki, -1L, "other-empty", 0, pageIsRedirect = false),
          PageInfo(wiki, -2L, "other-internal",0, pageIsRedirect = false),
          PageInfo(wiki, -3L, "other-external",0, pageIsRedirect = false),
          PageInfo(wiki, -4L, "other-search",0, pageIsRedirect = false),
          PageInfo(wiki, -5L, "other-other",0, pageIsRedirect = false))))
      )
  }

  /**
    * Prepare the redirects dataset to be reused
    * @return A RDD of redirect data
    */
  def prepareRedirects(
                        sqlContext: SQLContext,
                        redirectTable: String,
                        snapshot: String,
                        wikiList: Seq[String],
                        pagesPerPageId: RDD[((String, Long), String)],
                        perTitleAndNamespacePages: RDD[((String, String, Long), Long)]
                      ): RDD[Redirect] = {
    sqlContext.sql(
      s"""
         |SELECT
         |  wiki_db,
         |  rd_from,
         |  rd_title,
         |  rd_namespace
         |FROM $redirectTable
         |WHERE snapshot = '$snapshot'
         |  AND wiki_db IN (${listToSQLInCondition(wikiList)})
         |  AND rd_from IS NOT NULL
         |  AND rd_from > 0
         |GROUP BY
         |  wiki_db,
         |  rd_from,
         |  rd_title,
         |  rd_namespace
          """.stripMargin).
      rdd.
      map(r => {
        ((r.getString(0), r.getLong(1)), (r.getString(2), r.getLong(3)))
      }).
      filter(t => Option(t._2._1).isDefined). // Remove null toPageTitle
      join(pagesPerPageId). // Ensure fromPageId exists in page table
      map { case ((wiki, fromPageId), ((toPageTitle, toPageNamespace), _)) =>
        ((wiki, toPageTitle, toPageNamespace), fromPageId) }.
      join(perTitleAndNamespacePages). // Get toPageId from page table
      map { case ((wiki, _ , _), (fromPageId, toPageId)) =>
        Redirect(wiki, fromPageId, toPageId)}.
      distinct // prevent any duplicate (data corruption)
  }


  /**
    * Prepare the pagelinks dataset to be reused
    * Note: links that end in a redirect are 'resolved' in this dataset.
    *       This means that if A links to B, and B redirects to C, we replace the link (A,B) with (A,C).
    *       This lets us properly annotate link types after resolving redirects in the clickstream, since
    *       a user will experience following A as if it linked to C.
    * IMPORTANT: The 'distinct' at the end ensures that each link only occurs once
    * @return A RDD of pagelinks data.
    */
  def preparePagelinks(
                        sqlContext: SQLContext,
                        pagelinksTable: String,
                        snapshot: String,
                        wikiList: Seq[String],
                        pagesPerPageId: RDD[((String, Long), String)],
                        pagesPerTitleAndNamespace: RDD[((String, String, Long), Long)],
                        redirects: RDD[Redirect]
                      ): RDD[PageLink] = {
    sqlContext.sql(
      s"""
         |SELECT
         |  wiki_db,
         |  pl_from,
         |  pl_title,
         |  pl_namespace
         |FROM $pagelinksTable
         |WHERE snapshot = '$snapshot'
         |  AND wiki_db IN (${listToSQLInCondition(wikiList)})
         |  AND pl_from IS NOT NULL
         |  AND pl_from > 0
         |GROUP BY
         |  wiki_db,
         |  pl_from,
         |  pl_title,
         |  pl_namespace
              """.stripMargin).
      rdd.
      map(r => {
        ((r.getString(0), r.getLong(1)), (r.getString(2), r.getLong(3)))
      }).
      filter(t => Option(t._2._1).isDefined). // Remove null toPageTitle
      join(pagesPerPageId).  // Ensure fromPageId exists in page table
      map { case ((wiki, fromPageId), ((toPageTitle, toPageNamespace), _)) =>
        ((wiki, toPageTitle, toPageNamespace), fromPageId) }.
      join(pagesPerTitleAndNamespace).  // get ToPageId from page table
      map { case ((wiki, _, _), (fromPageId, toPageId)) => ((wiki, toPageId), fromPageId)}.
      leftOuterJoin(redirects.map(r => ((r.wikiDb, r.fromPageId), r.toPageId))).
      map { case ((wiki, toPageId), (fromPageId, optionalRedirectToPageId)) =>
        PageLink(wiki, fromPageId, optionalRedirectToPageId.getOrElse(toPageId))}.
      distinct  // prevent any duplicate (data corruption or redirect induced)
  }

  /**
    * Prepare the actual clickstream dataset
    * @return A RDD of clickstream data.
    */
  def prepareClickstream(
                          sqlContext: SQLContext,
                          webrequestTable: String,
                          year: Int,
                          month: Int,
                          day: Option[Int],
                          hour: Option[Int],
                          domainAndMobileList: Seq[String],
                          projectList: Seq[String],
                          projectToWikiMap: Map[String, String],
                          pages: RDD[PageInfo],
                          pagesPerTitleAndNamespace: RDD[((String, String, Long), Long)],
                          redirects: RDD[Redirect],
                          pageLinks: RDD[PageLink],
                          minCount: Int = 10
                        ): RDD[ClickStream] = {
    val pagesPerTitles = pagesPerTitleAndNamespace
      .map(p => ((p._1._1, p._1._2), (p._1._3, p._2))) // ((wiki, pageTitle), (pageNamespace, pageId))
      .cache()
    val pagesPerPageId = pages.keyBy(p => (p.wikiDb, p.pageId)).cache()

    sqlContext.sql(
      s"""
         |SELECT
         |  CONCAT(pageview_info['project'], '.org') AS project,
         |  CASE
         |    -- empty or malformed referer
         |    WHEN referer IS NULL THEN 'other-empty'
         |    WHEN referer == '' THEN 'other-empty'
         |    WHEN referer == '-' THEN 'other-empty'
         |    WHEN parse_url(referer,'HOST') is NULL THEN 'other-empty'
         |    -- internal referer from the same wikipedia
         |    WHEN
         |        parse_url(referer,'HOST') in (${listToSQLInCondition(domainAndMobileList)})
         |        AND LENGTH(REGEXP_EXTRACT(parse_url(referer,'PATH'), '/wiki/(.*)', 1)) > 1
         |    THEN REGEXP_EXTRACT(parse_url(referer,'PATH'), '/wiki/(.*)', 1)
         |    -- other referers
         |    WHEN referer_class = 'internal' THEN 'other-internal'
         |    WHEN referer_class = 'external' THEN 'other-external'
         |    WHEN referer_class = 'external (search engine)' THEN 'other-search'
         |    ELSE 'other-other'
         |  END as fromTitle,
         |  pageview_info['page_title'] as toTitle,
         |  COUNT(1) as count
         |FROM $webrequestTable
         |WHERE webrequest_source = 'text'
         |  AND year = $year AND month = $month
         |  ${day.map(d => s"AND day = $d").getOrElse("")}
         |  ${hour.map(h => s"AND hour = $h").getOrElse("")}
         |  AND pageview_info['project'] IN (${listToSQLInCondition(projectList)})
         |  AND is_pageview
         |  AND agent_type = 'user'
         |GROUP BY
         |  CONCAT(pageview_info['project'], '.org'),
         |  CASE
         |    -- empty or malformed referer
         |    WHEN referer IS NULL THEN 'other-empty'
         |    WHEN referer == '' THEN 'other-empty'
         |    WHEN referer == '-' THEN 'other-empty'
         |    WHEN parse_url(referer,'HOST') is NULL THEN 'other-empty'
         |    -- internal referer from the same wikipedia
         |    WHEN
         |        parse_url(referer,'HOST') in (${listToSQLInCondition(domainAndMobileList)})
         |        AND LENGTH(REGEXP_EXTRACT(parse_url(referer,'PATH'), '/wiki/(.*)', 1)) > 1
         |    THEN REGEXP_EXTRACT(parse_url(referer,'PATH'), '/wiki/(.*)', 1)
         |    -- other referers
         |    WHEN referer_class = 'internal' THEN 'other-internal'
         |    WHEN referer_class = 'external' THEN 'other-external'
         |    WHEN referer_class = 'external (search engine)' THEN 'other-search'
         |    ELSE 'other-other'
         |  END,
         |  pageview_info['page_title']
          """.stripMargin)
      .rdd
      // Raw clickstream ((wiki, fromTitle), (toTitle, count)) out of webrequest
      .map(r => {
        val project = r.getString(0)
        val wiki = projectToWikiMap(project)
        ((wiki, r.getString(1)), (r.getString(2), r.getLong(3))) //
      }).
      // Get fromPageId instead of fromPageTitle and keep only fromNamespace == 0
      join(pagesPerTitles).
      filter { case (_, (_, (fromPageNamespace, _))) => fromPageNamespace == 0}.
      map { case ((wiki, _), ((toTitle, count), (_, fromPageId))) => ((wiki, toTitle), (fromPageId, count)) }.
      // Get toPageId instead of toPageTitle and keep only toNamespace == 0
      join(pagesPerTitles).
      filter { case (_, (_, (toPageNamespace, _))) => toPageNamespace == 0}.
      map { case ((wiki, _), ((fromPageId, count), (_, toPageId))) => ((wiki, toPageId), (fromPageId, count)) }.
      // Resolve one step of toPageId redirect
      // Note that from redirects are not correct, and therefore filtered at the end
      leftOuterJoin(redirects.map(r => ((r.wikiDb, r.fromPageId), r.toPageId))).
      map { case ((wiki, toPageId), ((fromPageId, count), optionalRedirectToPageId)) =>
        ((wiki, fromPageId, optionalRedirectToPageId.getOrElse(toPageId)), count)}.
      // Re-Aggregate after the redirects-reduction, and remove infrequent pairs
      reduceByKey(_ + _).
      filter(_._2 >= minCount).
      //
      leftOuterJoin(pageLinks.map(l => ((l.wikiDb, l.fromPageId, l.toPageId), true))).
      map { case ((wiki, fromPageId, toPageId), (count, optionalPageLink)) =>
          val typ = {
            if (fromPageId < 0) "external"
            else if (optionalPageLink.isDefined) "link"
            else "other"
          }
          ((wiki, fromPageId), (toPageId, typ, count)) }.
      filter {case ((_, fromPageId), (toPageId, _, _)) => fromPageId != toPageId}.
      join(pagesPerPageId).
      filter { case (_, (_, fromPage)) => fromPage.pageNamespace == 0 && ! fromPage.pageIsRedirect}.
      map { case ((wiki, _), ((toPageId, typ, count), fromPage)) =>
        ((wiki, toPageId), (fromPage.pageTitle, typ, count)) }.
      join(pagesPerPageId).
      filter { case (_, (_, toPage)) => toPage.pageNamespace == 0 && ! toPage.pageIsRedirect}.
      map { case ((wiki, _), ((fromPageTitle, typ, count), toPage)) =>
        ClickStream(wiki, fromPageTitle, toPage.pageTitle, typ, count) }
  }


  /**
    * Config class for CLI argument parser using scopt
    */
  case class Params(
                     outputBasePath: String = "/wmf/data/archive/clickstream",
                     projectNamespaceTable: String = "wmf_raw.mediawiki_project_namespace_map",
                     pageTable: String = "wmf_raw.mediawiki_page",
                     redirectTable: String = "wmf_raw.mediawiki_redirect",
                     pagelinksTable: String = "wmf_raw.mediawiki_pagelinks",
                     webrequestTable: String = "wmf.webrequest",
                     wikiList: Seq[String] = Seq("enwiki"),
                     outputFilesParts: Int = 1,
                     snapshot: String = "", // Parameter required, never used as is
                     year: Int = 0, // Parameter required, never used as is
                     month: Int = 0, // Parameter required, never used as is
                     day: Option[Int] = None,
                     hour: Option[Int] = None,
                     minCount: Int = 0 // Parameter required, never used as is
                   )

  /**
    * Define the command line options parser
    */
  val argsParser = new OptionParser[Params]("Clickstream dataset builder") {
    head("Clickstream dataset builder", "")
    note(
      """
        |This job computes a clickstream dataset from one or more wiki(s).
        |It creates a date folder, and per-wiki folders to store the results.
      """.stripMargin)
    help("help") text ("Prints this usage text")

    opt[String]('s', "snapshot") required() valueName ("<snapshot>") action { (x, p) =>
      p.copy(snapshot = x)
    } text ("The mediawiki hadoop snapshot to use for page, links and redirects (usually YYYY-MM)")

    opt[Int]('y', "year") required() valueName ("<year>") action { (x, p) =>
      p.copy(year = x)
    } text ("The year to use for webrequest data gathering.")

    opt[Int]('m', "month") required() valueName ("<month>") action { (x, p) =>
      p.copy(month = x)
    } validate { x => if (x > 0 & x <= 12) success else failure("Invalid month")
    } text ("The month to use for webrequest data gathering.")

    opt[Int]('d', "day") optional() valueName ("<day>") action { (x, p) =>
      p.copy(day = Some(x))
    }  validate { x => if (x > 0 & x <= 31) success else failure("Invalid day")
    } text ("The day to use for webrequest data gathering (default to empty, for monthly computation).")

    opt[Int]('h', "hour") optional() valueName ("<hour>") action { (x, p) =>
      p.copy(hour = Some(x))
    } validate { x => if (x >= 0 & x < 24 ) success else failure("Invalid hour")
    } text ("The hour to use for webrequest data gathering (default to empty, for daily or monthly computation).")

    opt[Int]('c', "minimum-count") required() valueName ("<min-count>") action { (x, p) =>
      p.copy(minCount = x)
    } validate { x => if (x >= 0 ) success else failure("Invalid minimum count (must be at least 0)")
    } text ("The minimum count for a link to appear in the dataset. Default to 10.")

    opt[String]('o', "output-base-path") optional() valueName ("<path>") action { (x, p) =>
      p.copy(outputBasePath = if (x.endsWith("/")) x.dropRight(1) else x)
    } text ("Where on HDFS to store the computed dataset (date folder created for you). Defaults to hdfs://analytics-hadoop/wmf/data/archive/clickstream")

    opt[String]('w', "wikis") optional() valueName "<wiki_db_1>,<wiki_db_2>..." action { (x, p) =>
      p.copy(wikiList = x.split(",").map(_.toLowerCase))
    } validate { x =>
      val dbs = x.split(",").map(_.toLowerCase)
      if (dbs.filter(db => db.isEmpty || (! db.contains("wik"))).length > 0)
        failure("Invalid wikis list")
      else
        success
    } text "wiki dbs to compute. Defaults to enwiki"

    opt[Int]('p', "output-files-parts") optional() valueName ("<partitions>") action { (x, p) =>
      p.copy(outputFilesParts = x)
    } text ("Number of file parts to output in hdfs. Defaults to 1")

    opt[String]("project-namespace-table") optional() valueName ("<table>") action { (x, p) =>
      p.copy(projectNamespaceTable = x)
    } text ("Fully qualified name of the project-namespace table on Hive. Default to wmf_raw.mediawiki_project_namespace_map")

    opt[String]("page-table") optional() valueName ("<table>") action { (x, p) =>
      p.copy(pageTable = x)
    } text ("Fully qualified name of the page table on Hive. Default to wmf_raw.mediawiki_page")

    opt[String]("redirect-table") optional() valueName ("<table>") action { (x, p) =>
      p.copy(redirectTable = x)
    } text ("Fully qualified name of the redirect table on Hive. Default to wmf_raw.mediawiki_redirect")

    opt[String]("pagelinks-table") optional() valueName ("<table>") action { (x, p) =>
      p.copy(pagelinksTable = x)
    } text ("Fully qualified name of the pagelinks table on Hive. Default to wmf_raw.mediawiki_pagelinks")

    opt[String]("webrequest-table") optional() valueName ("<table>") action { (x, p) =>
      p.copy(webrequestTable = x)
    } text ("Fully qualified name of the webrequest table on Hive. Default to wmf.webrequest")

  }


  def main(args: Array[String]): Unit = {
    val params = args.headOption match {
      // Case when our job options are given as a single string.  Split them
      // and pass them to argsParser.
      case Some("--options") =>
        argsParser.parse(args(1).split("\\s+"), Params()).getOrElse(sys.exit(1))
      // Else the normal usage, each CLI opts can be parsed as a job option.
      case _ =>
        argsParser.parse(args, Params()).getOrElse(sys.exit(1))
    }

    // Exit non-zero if if any refinements failed.
    apply(params)
  }

  def apply(params: Params): Unit = {

      val conf = new SparkConf()
        .setAppName(s"ClickStreamBuilder")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .registerKryoClasses(Array(
          classOf[PageInfo],
          classOf[Redirect],
          classOf[PageLink],
          classOf[ClickStream]))
        .set("spark.hadoop.mapred.output.compress", "true")
        .set("spark.hadoop.mapred.output.compression.codec", "true")
        .set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
        .set("spark.hadoop.mapred.output.compression.type", "BLOCK")

      val sqlContext = new HiveContext(new SparkContext(conf))

      import sqlContext.implicits._

      val projectToWikiMap = prepareProjectToWikiMap(sqlContext, params.projectNamespaceTable, params.snapshot, params.wikiList)
      val domainList = projectToWikiMap.keys.toList
      val domainAndMobileList = domainList.flatMap(p => {
        val firstDotIndex = p.indexOf('.')
        Seq(p, p.substring(0, firstDotIndex) + ".m" + p.substring(firstDotIndex))
      })
      val projectList = domainList.map(_.stripSuffix(".org"))

      val outputFolder = "/user/joal/wmf/data/archive/clickstream/" +
        f"${params.year}%04d-${params.month}%02d${params.day.map(d => f"-$d%02d").getOrElse("")}${params.hour.map(h => f"-$h%02d").getOrElse("")}"

      // Reused RDDs
      val pages = preparePages(sqlContext, params.pageTable, params.snapshot, params.wikiList).cache()
      val pagesPerPageId = pages.map(p => ((p.wikiDb, p.pageId), p.pageTitle)).cache()
      val pagesPerTitleAndNamespace = pages.map(p => ((p.wikiDb, p.pageTitle, p.pageNamespace), p.pageId)).cache()

      val redirects = prepareRedirects(sqlContext, params.redirectTable, params.snapshot, params.wikiList, pagesPerPageId, pagesPerTitleAndNamespace).cache()
      val pageLinks = preparePagelinks(sqlContext, params.pagelinksTable, params.snapshot, params.wikiList, pagesPerPageId, pagesPerTitleAndNamespace, redirects).cache()

      prepareClickstream(sqlContext, params.webrequestTable, params.year, params.month, params.day, params.hour,
        domainAndMobileList, projectList, projectToWikiMap, pages, pagesPerTitleAndNamespace, redirects, pageLinks).
        map(c => (c.wikiDb, c.toTSVLine)).
        repartition(params.outputFilesParts).
        toDF.
        withColumnRenamed("_1", "wiki_db").
        write.
        mode(SaveMode.Overwrite).
        partitionBy("wiki_db").
        text(outputFolder)
  }
}

