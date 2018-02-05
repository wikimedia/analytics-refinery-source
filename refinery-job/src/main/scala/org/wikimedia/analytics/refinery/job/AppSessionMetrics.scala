package org.wikimedia.analytics.refinery.job

import com.github.nscala_time.time.Imports.{LocalDate, Period}
import com.twitter.algebird.{QTree, QTreeSemigroup}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import scala.collection.immutable.HashMap

/**
 * This job computes the following session-related metrics for app pageviews:
 * Number of sessions per user
 * Number of pageviews per session
 * Session length (gap between first and last pageview, in milliseconds)
 *
 * For each metric, the following stats are computed:
 * Minima
 * Maxima
 * Quantiles List(.1, .5, .9, .99)
 *
 * Usage with spark-submit
 * spark-submit \
 * --class org.wikimedia.analytics.refinery.job.AppSessionMetrics
 * /path/to/refinery-job.jar
 * -o <output-dir> -y <year> -m <month> -d <day> [-p <period-days> -w <webrequest-base-path> -n <num-partitions>]
 *
 * spark-submit \
 * --class org.wikimedia.analytics.refinery.job.AppSessionMetrics
 * --num-executors=16 --executor-cores=1   --executor-memory=2g
 * /path/to/refinery-job.jar -h hdfs://analytics-hadoop -o /tmp/mobile-apps-sessions
 * -y 2015 -m 3 -d 30 [-p 30 -n 8]
 *
 * The metrics are stored in output_directory/session_metrics.tsv, and also exposed through a
 * hive external table at wmf.mobile_apps_session_metrics
 *
 */

object AppSessionMetrics {

  /**
   * Quantiles
   *
   * @param nums RDD of values.
   * @param qs   List of q values to calculate quantiles for.
   *
   * @return A list containing the quantile (lowBound, highBound) for each q.
   */
  def quantiles(nums: RDD[Long], qs: List[Double]): List[(Double, Double)] = {
    val qtSemigroup = new QTreeSemigroup[Long](8)


    /** see https://github.com/twitter/algebird/issues/517
      * and note that QTree constructor has changed since then,
      * If we upgrade algebird we need to upgrade this workarround
      https://github.com/twitter/algebird/blob/e8f869bc266997190a1fc6ed376dc415cee8e250/algebird-core/src/main/scala/com/twitter/algebird/QTree.scala#L159

     */

    // aiming for 4 digits of precision, percentiles should be reported like:
    // SessionsPerUser,Android,Map(percentile_50 -> (1.0,1.0625)

    val sum = nums.map(x =>QTree(x*16,-4,1,x,None,None) ).reduce(qtSemigroup.plus)

    qs.map(sum.quantileBounds(_))
  }

  /**
   * Session logic
   *
   * @param sessions  List of sessions. Each session is represented
   *                  as an ordered list of pageview timestamps.
   *                  Like: List(List(ts1, ts2), List(ts3, ts4), List(ts5))
   *                  This would be empty on 1st pass for a uuid
   *
   * @param timestamp The pageview timestamp to be merged to the
   *                  session list. It is assumed to be greater than
   *                  all the previous timestamps in the session list.
   *                  So the list we are folding needs to be order max to min
   *                  If gap among two pageviews is bigger than 30 mins (1800 secs)
                      is a new session
   *
   * @return The list of sessions including the new pageview timestamp.
   *         Depending on the time passed since last pageview,
   *         The timestamp will be allocated as part of the last session
   *         or in a new session.
   */
  def sessionize(sessions: List[List[Long]], timestamp: Long): List[List[Long]] = {

    val sessionGap = 1800
    if (sessions.isEmpty) List(List(timestamp))
    else {
      if (timestamp <= sessions.last.last + sessionGap) {
        sessions.init :+ (sessions.last :+ timestamp)
      } else sessions :+ List(timestamp)
    }
  }

  /**
   * Computes statistics for a metric
   *
   * @param nums RDD of values.
   * @return Hashmap of different stats computed for a given metric
   */
  def statsPerMetric(nums: RDD[Long]) = {
    val qs = List(.1, .5, .9, .99)
    val percentiles = quantiles(nums, qs)

    Map(
      "count" -> nums.count,
      "min" -> nums.min,
      "max" -> nums.max,
      "percentile_1" -> percentiles(0),
      "percentile_50" -> percentiles(1),
      "percentile_90" -> percentiles(2),
      "percentile_99" -> percentiles(3)
    )
  }

  /**
   * Compute stats for the different session metrics given all user sessions data
   * @param userSessions  UserSessions is of this form:
   *                      [((String, String), List[List[Long]])] = (
   *                        ((Android, 3e92cbf4-d204-4a59-a12d-dde6e337902d), List(List(1426809630), List(1426812577))),
   *                        ((iOS, bf78563d-9173-415c-ba4f-6848ab46afc3), List(List(1426810256, 1426810264))),
   *                        ((Android, 896c399c-0dc1-4835-aa66-6d619b83a76b), List(List(1426810619, 1426811555))),
   *                        ((iOS, 53d91d28-158f-4dcf-8561-5a3d6e90e18e), List(List(1426810894, 1426810915)))
   *                      )
   * @param osFilter      [Optional] If present, it will consider only the uuids
   *                      that belong to the given os_family. It will also output
   *                      this parameter as the second value of the output's tuples.
   *                      If not present (default), it will consider all uuids and
   *                      output null as second value in the output's tuples.
   * @return List of tuples in the form of [(SessionMetricName, osFamily, SessionMetricStats), ... ]
   */
  def allSessionMetricsStats(
    userSessions: RDD[((String, String), List[List[Long]])],
    osFilter: String = null
  ): List[(String, String, Map[String, Any])] = {

    // Filter the wmfuuids that belong to the given os family, if necessary.
    val filteredSessions = if (osFilter == null) {
      userSessions
    } else {
      userSessions.filter(_._1._1 == osFilter)
    }

    // calculate number of sessions per user
    val sessionsPerUser = statsPerMetric(filteredSessions.map(r => r._2.length.toLong))

    // flatten: (key, List(session1, session2, ...) -> session*
    // RDD[List[Long]]] =
    // List(1426810256, 1426810264),
    // List(1426811100), List(1426810253)
    // List(1426810619, 1426811555), List(1426810894, 1426810915),
    val sessions = filteredSessions.flatMap(_._2)

    // calculate number of pageviews per session
    val pageviewsPerSession = statsPerMetric(sessions.map(r => r.length.toLong))

    // calculate session length
    // sessions with only one pageview are not counted
    val sessionLength = statsPerMetric(sessions
      //Remove keys with just 1 timestamp
      .filter(s => !s.isEmpty && !s.tail.isEmpty)
      .map(r => r.last - r.head))

    List(
      ("SessionsPerUser", osFilter, sessionsPerUser),
      ("PageviewsPerSession", osFilter, pageviewsPerSession),
      ("SessionLength", osFilter, sessionLength)
    )
  }

  /**
   * Given stats map returns stats ready to be printed
   *
   * @return A tab separated string for given metric, looks like
   *         2015	6	2	2015-6-1 -- 2015-6-1	SessionsPerUser	1259304	1	15	(1.0,2.0)	(1.0,2.0)	(2.0,3.0)	(5.0,6.0)
   *         If the data is broken down by os family, adds a corresponing column at the end.
   */
  def statsToString(
    stats: Map[String, Any],
    statsType: String,
    osFamily: String,
    datesInfo: Map[String, Int]
  ): String = {
    val reportDateRange = dateRangeToString(datesInfo)
    val osString = if (osFamily == null) "" else "\t" + osFamily
    val outputStats = "%d\t%d\t%d\t%s\t%s%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"
      .format(datesInfo("year"), datesInfo("month"), datesInfo("day"), reportDateRange,
        statsType, osString, stats.apply("count"), stats.apply("min"), stats.apply("max"),
        stats.apply("percentile_1"), stats.apply("percentile_50"), stats.apply("percentile_90"),
        stats.apply("percentile_99"))
    outputStats
  }

  /**
   * Makes a printable string with all the stats
   *
   * @param sessionStatsData List of tuples in form of
   *                         [(SessionMetricName, osFamily, SessionMetricStats), ...]
   * @param datesInfo Hashmap with report date related info
   * @return String with 1 line for every metric, separated by newline
   */
  def printableStats(
    sessionStatsData: List[(String, String, Map[String, Any])],
    datesInfo: Map[String, Int]
  ): String = {
    sessionStatsData.map(statsData => statsToString(
      statsData._3,
      statsData._1,
      statsData._2,
      datesInfo
    )).mkString("\n")
  }


  /**
   * Make a report date range string given the date of report run and the number of days
   * for which the report is being run for
   * @param datesInfo Hashmap with report date related info in the form of
   *                  {"year"->2015, "month"->5, "day"->30, "periodDays"->30}
   * @return String in the form of 'yyyy-mm-dd -- yyyy-mm-dd'
   */
  def dateRangeToString(datesInfo: Map[String, Int]): String = {
    //Start Date is the date of report run
    val start = new LocalDate(datesInfo("year"), datesInfo("month"), datesInfo("day"))
    //End date is the end of the report run period
    val end = start.plusDays(datesInfo("periodDays") - 1)
    val dateRange = "%d-%d-%d -- %d-%d-%d".format(start.getYear, start.getMonthOfYear, start.getDayOfMonth,
      end.getYear, end.getMonthOfYear, end.getDayOfMonth)
    dateRange
  }

  /**
   * Generate list of Parquet file paths over a range of dates
   * @param webrequestTextPath Base path to webrequest text parquet data
   * @param datesInfo Hashmap with report date related info
   * @return List of path strings like [".../day=1", ".../day=2"]
   */
  def dateRangeToPathList(webrequestTextPath: String, datesInfo: Map[String, Int]): List[String] = {
    //Custom iterator for stepping through LocalDate objects
    def makeDateRange(from: LocalDate, to: LocalDate, step: Period): Iterator[LocalDate] =
      Iterator.iterate(from)(_.plus(step)).takeWhile(_.isBefore(to))

    val dateStart = new LocalDate(datesInfo("year"), datesInfo("month"), datesInfo("day"))
    val dateEnd = dateStart.plusDays(datesInfo("periodDays"))
    val dateRange = makeDateRange(dateStart, dateEnd, new Period().withDays(1))
    dateRange.toList.map(dt => "%s/year=%d/month=%d/day=%d/*".format(webrequestTextPath, dt.getYear, dt.getMonthOfYear, dt.getDayOfMonth))
  }

  /**
   * Fetch select columns from given list of Parquet files
   * @param paths List of parquet file paths to load,
   *              Like: ["hdfs://../year=2015/month=5/day=5",
   *              "hdfs://../year=2015/month=5/day=6",...]
   * @param sqlContext SQL Context
   * @return DataFrame with 3 columns: os_family, wmfuuid and timestamp
   */
  def pathListToDataframe(paths: List[String], sqlContext: SQLContext): DataFrame = {
    sqlContext.read.parquet(paths: _*)
      .filter("""
        is_pageview
        and access_method = 'mobile app'
        and x_analytics_map['wmfuuid'] is not null
        and x_analytics_map['wmfuuid'] != ''
        and ts is not null
      """)
      .selectExpr(
        "user_agent_map['os_family'] as os_family",
        "x_analytics_map['wmfuuid'] as wmfuuid",
        "CAST(ts AS int) as ts"
      )
  }

  /**
   * Compute sessions by user
   * @param userSessionsAll DataFrame with os_family, wmfuuid and timestamp colums
   * @param numPartitions Number of partitions for the output RDD
   * @return userSessions RDD in the form of
   *                      ((os_family, wmfuuid), List(session1, session2, ...))
   */
  def userSessions(userSessionsAll: DataFrame, numPartitions: Int): RDD[((String, String), List[List[Long]])] = {
    userSessionsAll
      .rdd
      // format the line as: key=(os_family, wmfuuid), val=timestamp
      .map(r => ((r.getString(0), r.getString(1)), r.getInt(2).toLong))
      // aggregate key to list of sorted timestamps (key, timestamp)* -> (key, List(ts1, ts2, ts3, ...))
      .combineByKey(
        (t: Long) => List(t),
        (l: List[Long], t: Long) => (t +: l),
        // sort timestamps max to min
        (l1: List[Long], l2: List[Long]) => (l1 ++ l2),
        numPartitions = numPartitions)
      // map: (key, List(ts1, ts2, ts3, ...)) -> sort(List(ts1, ts2, ts3, ...)) -> (key, List(List(ts1, ts2), List(ts3), ...)
      .map { case (key, listOfTimestampLists) => key -> listOfTimestampLists.sorted.foldLeft(emptySessions)(sessionize) }
      .cache()
  }

  /**
   * Save output stats to HDFS
   */
  def saveStats(sc: SparkContext, outputFile: String, currentStats: String, reportDateRange: String) = {
    @transient
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = new Path(outputFile).getFileSystem(hadoopConf)

    // Convert current stats to string
    val path = new Path(outputFile)

    if (hdfs.exists(path)) {
      // Read data file and delete records from this date range if any
      // and convert the filtered data into a string.
      val pastStats = sc.textFile(outputFile)
        .filter(record => !record.contains(reportDateRange))
        .collect.mkString("\n")

      // Concatenate with current output stats
      val currentAndPastData = "%s\n%s".format(pastStats, currentStats)

      // 3. delete old file in hadoop
      try {
        hdfs.delete(path, true)
      } catch {
        case _: Throwable => {}
      }
      // Save the combined data
      hdfs.create(path).write(currentAndPastData.getBytes)
    } else {
      //Save only current data
      hdfs.create(path).write(currentStats.getBytes)
    }
  }

  /**
   * Empty list of sessions
   * To be used as zero value for the sessionize function.
   */
  val emptySessions = List.empty[List[Long]]

  /**
   * List of os families to breakdown.
   */
  val osFamilies: List[String] = List("Android", "iOS")

  /**
   * Config class for CLI argument parser using scopt
   */
  case class Params(webrequestBasePath: String = "hdfs://analytics-hadoop/wmf/data/wmf/webrequest",
                    outputDir: String = "/wmf/data/wmf/mobile-sessions/",
                    year: Int = 0, month: Int = 0, day: Int = 0, periodDays: Int = 30,
                    splitByOs: Boolean = false, numPartitions: Int = 16)

  /**
   * Define the command line options parser
   */
  val argsParser = new OptionParser[Params]("App Session Metrics") {
    head("Mobile App Session Metrics", "")
    note(
      """This job computes the following session-related metrics for app pageviews:
        |  Number of sessions per user
        |  Number of pageviews per session
        |  Session length (gap between first and last pageview, in milliseconds)
        |
        |  For each metric, the following stats are computed:
        |  Minima
        |  Maxima
        |  Quantiles List(.1, .5, .9, .99)""".format().stripMargin)
    help("help") text ("Prints this usage text")

    opt[String]('w', "webrequest-base-path") optional() valueName ("<path>") action { (x, p) =>
      p.copy(webrequestBasePath = if (x.endsWith("/")) x.dropRight(1) else x)
    } text ("Base path to webrequest data on hadoop. Defaults to hdfs://analytics-hadoop/wmf/data/wmf/webrequest")

    opt[String]('o', "output-dir") required() valueName ("<path>") action { (x, p) =>
      p.copy(outputDir = if (x.endsWith("/")) x else x + "/")
    } text ("Path to output directory")

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
    } text ("Day as an integer")

    opt[Int]('p', "period-days") optional() action { (x, p) =>
      p.copy(periodDays = x)
    } validate { x => if (x > 0 & x <= 31) success else failure("Too many days")
    } text ("Period in days to run report for. Defaults to 30 days")

    opt[Boolean]('s', "split-by-os") optional() action { (x, p) =>
      p.copy(splitByOs = x)
    } text("Calculate metrics broken down by os family. Default: false")

    opt[Int]('n', "num-partitions") optional() action { (x, p) =>
      p.copy(numPartitions = x)
    } text ("Number of hash-paritions for User Sessions RDD. Defaults to 16. Specify less/more partitions based on job")
  }

  def main(args: Array[String]) {
    argsParser.parse(args, Params()) match {
      case Some(params) => {
        // Initial setup - Spark, SQLContext
        val conf = new SparkConf().setAppName("AppSessionMetrics")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.kryoserializer.buffer.mb", "24")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

        // Generate a list of all parquet file paths to read given the webrequest base path,
        // and all dates related information.  NOTE: As of January 2016,
        // mobile web caches have been merged with text, so webrequest_source=text.
        val webrequestTextPath = params.webrequestBasePath + "/webrequest_source=text"
        // Helper hashmap with all date related information to avoid passing around lots of params
        val datesInfo = HashMap("year" -> params.year, "month" -> params.month, "day" -> params.day, "periodDays" -> params.periodDays)
        // List of path strings like [".../day=1", ".../day=2"]
        val webrequestPaths = dateRangeToPathList(webrequestTextPath, datesInfo)

        // Get sessions data for all users, calculate stats for different metrics,
        // and get the stats in a printable string format to output
        val userSessionsData = userSessions(pathListToDataframe(webrequestPaths, sqlContext), params.numPartitions).cache()
        val allMetricsStats = if (params.splitByOs) {
          osFamilies.map(allSessionMetricsStats(userSessionsData, _)).fold(List.empty)(_++_)
        } else {
          allSessionMetricsStats(userSessionsData)
        }
        val outputStats = printableStats(allMetricsStats, datesInfo)

        //Save output to file
        val outputFile = params.outputDir + "/session_metrics.tsv"
        saveStats(sc, outputFile, outputStats, dateRangeToString(datesInfo))
      }
      case None => sys.exit(1)
    }
  }
}
