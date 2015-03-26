package org.wikimedia.analytics.refinery.job

import java.text.SimpleDateFormat

import com.twitter.algebird.{QTree, QTreeSemigroup}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive._
import org.apache.spark.{SparkConf, SparkContext}
import org.wikimedia.analytics.refinery.core.{PageviewDefinition, Webrequest}

import scala.util.control.NonFatal

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
 * Usage with spark-submit (please note that 'day' is an optional argument)
 * spark-submit \
 * --class org.wikimedia.analytics.refinery.job.AppSessionMetrics
 * /path/to/refinery-job.jar <output-directory> <year> <month> [<day>]
 *
 * spark-submit \
 * --class org.wikimedia.analytics.refinery.job.AppSessionMetrics
 *  --num-executors=6 --executor-cores=2   --executor-memory=2g
 * /path/to/refinery-job.jar hdfs://analytics-hadoop/tmp/mobile-apps-sessions 2015 03 [01]
 *
 *
 * Note that there is 1 file created per date as it is the only way to make sure
 * reruns update the right file.
 *
 * TODO: oozification
 * TODO: mobile need to document their uuid, in x-analytics we see wmfuuid
 * https://wikitech.wikimedia.org/wiki/X-Analytics
 *
 */
object AppSessionMetrics {

  /**
   * Transforms a date yyyy-MM-dd'T'HH:mm:ss into a timestamp
   * Will not be needed once timestamp column is filled by hive
   * @param s
   * @return
   */
  def getTimeFromDate(s: String): Long = {
    try {
      //SimpleDateFormat is not thread safe
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
      dateFormat.parse(s).getTime() / 1000
    } catch {
      // do not break app if we cannot parse date, maybe overly cautious?
      case NonFatal(t) => {
        return 0
      }
    }
  }


  /**
   * Quantiles
   *
   * @param nums RDD of values.
   * @param qs   List of q values to calculate quantiles for.
   *
   * @return A list containing the quantile (lowBound, highBound) for each q.
   */
  def quantiles(nums: RDD[Long], qs: List[Double]): List[(Double, Double)] = {
    val qtSemigroup = new QTreeSemigroup[Long](16)
    val sum = nums.map(QTree(_)).reduce(qtSemigroup.plus)
    qs.map(sum.quantileBounds(_))
  }

  /**
   * Computes statistics for a metric
   */
  def stats(nums: RDD[Long]) = {
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
   * Given stats map returns stats ready to be printed
   * @param stats
   * @param statsType
   * @param date
   * @return String
   */
  def statsToString(stats: Map[String, Any], statsType: String, date: String): String = {

    val outputStats = """ %s, %s, %s, %s, %s, %s, %s, %s, %s """.format(date, statsType, stats.apply("count"), stats.apply("min"), stats.apply("max"),
      stats.apply("percentile_1"), stats.apply("percentile_50"), stats.apply("percentile_90"), stats.apply("percentile_99"))

    outputStats
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
   *                  If gap amoung two pageviews is bigger than 30 mins (1800 secs)
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
   * Returns false if data on row is empty or null for any of the fields
   * Have in mind that even query string is always full for apps pageviews
   * looks something like: "?action=mobileview&format=json&page=Belgium&"
   *
   * @param r
   * @return
   */
  def filterBadData(r: Row): Boolean = {
    for (i <- 0 to r.length - 1) {
      if (r.isNullAt(i) || r.getString(i).trim().isEmpty())
        return false
    }
    return true
  }

  /**
   * Builds "where" clause like:
   * where webrequest_source='mobile' and year=2015 and month=03 [and day=20]"
   * Note that months and days have a leading 0
   *
   * Returns date for later reporting
   *
   * @param args
   *
   * @return (String: predicate, String: date)
   */
  def buildPredicate(args: Array[String]): (String, String) = {
    //arguments need to be positional
    //mandatory
    val year = args(1)
    val month = args(2)
    val predicate = "where webrequest_source='mobile' and year="
    if (args.length == 4) {
      var day = args(3)
      return (predicate + year + " and month=" + month + " and day=" + day, year + "-" + month + "-" + day)
    } else {
      return (predicate + year + " and month=" + month, year + "-" + month)
    }
  }


  /**
   * Empty list of sessions
   * To be used as zero value for the sessionize function.
   */
  val emptySessions = List.empty[List[Long]]


  def main(args: Array[String]) {
    // get spark context
    val conf = new SparkConf().setAppName("AppSessionMetrics")
    val sc = new SparkContext(conf)


    @transient
    val hc = new HiveContext(sc)
    val (predicate, date) = buildPredicate(args)
    val outputDirectory = args(0)

    val sql = "SELECT uri_path, uri_query, content_type, user_agent, x_analytics, dt from wmf.webrequest " + predicate
    val data = hc.sql(sql)

    // compute sessions by user: (uuid, List(session1, session2, ...)
    val userSessionsAll = data
      //filter records with empty entries that we cannot use
      .filter(r => filterBadData(r))
      // filter app pageviews
      .filter(r => PageviewDefinition.getInstance.isAppPageview(r.getString(0), r.getString(1), r.getString(2), r.getString(3)))
      //consider only records where uuid is on x-analytics field, those are not all of them, for some uuid comes on the url
      .filter(pv => Webrequest.getInstance.getXAnalyticsValue(pv.getString(4), "wmfuuid").trim.nonEmpty && pv.getString(5).trim.nonEmpty)
      // map: pageview -> (uuid, timestamp)
      .map(pv => (Webrequest.getInstance.getXAnalyticsValue(pv.getString(4), "wmfuuid"), getTimeFromDate(pv.getString(5))))


    // lowering in 1 order of magnitude the number of partitions for this job
    // logs list 1500 partitions for the original dataset
    val userSessions = userSessionsAll.coalesce(100)
      // aggregate uuid to list of sorted timestamps (uuid, timestamp)* -> (uuid, List(ts1, ts2, ts3, ...))
      // sorting is max to min, careful here: if values are found in the same partition they need to be sorted too
      .combineByKey(
        List(_),
        (l: List[Long], t: Long) => (t +: l).sorted,
        // sort timestamps max to min
        (l1: List[Long], l2: List[Long]) => (l1 ++ l2).sorted
      )
      // map: (uuid, List(ts1, ts2, ts3, ...)) -> (uuid, List(List(ts1, ts2), List(ts3), ...)
      .map { case (uuid, listOfTimestampLists) => uuid -> listOfTimestampLists.foldLeft(emptySessions)(sessionize) }

    /**
     * UserSessions is of this form:
     *
     * [(String, List[List[Long]])] =
     * ((3e92cbf4-d204-4a59-a12d-dde6e337902d,List(List(1426809630),List(1426812577))),
     * (bf78563d-9173-415c-ba4f-6848ab46afc3,List(List(1426810256, 1426810264))),
     * (896c399c-0dc1-4835-aa66-6d619b83a76b,List(List(1426810619, 1426811555))),
     * (53d91d28-158f-4dcf-8561-5a3d6e90e18e,List(List(1426810894, 1426810915))),
     */


    // flatten: (uuid, List(session1, session2, ...) -> session*
    // RDD[List[Long]]] =
    // List(1426810256, 1426810264),
    // List(1426811100), List(1426810253)
    // List(1426810619, 1426811555), List(1426810894, 1426810915),
    val sessions = userSessions.flatMap(_._2)

    // calculate number of sessions per user
    val sessionsPerUser = userSessions.map(r => r._2.length.toLong)

    // calculate number of pageviews per session
    val pageviewsPerSession = sessions.map(r => r.length.toLong)

    // calculate session length
    // sessions with only one pageview are not counted
    val sessionLength = sessions
      //Remove uuids with just  1 timestamp
      .filter(s => !s.isEmpty && !s.tail.isEmpty)
      .map(r => r.last - r.head)

    val statsSessionsPerUser = statsToString(stats(sessionsPerUser), "sessionsPerUser", date)
    val statsPageviewsPerSession = statsToString(stats(pageviewsPerSession), "pageviewsPerSession", date)
    val statsSessionLength = statsToString(stats(sessionLength), "sessionLength", date)

    //println(statsSessionsPerUser)
    val header =
      """Date, Type, count, max, min, p_1, p_50, p_90, p99
      """.stripMargin
    val outputStats = """ %s,
%s,
%s, """.format(statsSessionsPerUser, statsPageviewsPerSession, statsSessionLength)

    ////////////////////////////     Save Output       ///////////////////////////////
    // note that spark doesn't know how to "override" a file:
    // http://apache-spark-user-list.1001560.n3.nabble.com/How-can-I-make-Spark-1-0-saveAsTextFile-to-overwrite-existing-file-td6696.html

    val uri = new java.net.URI(outputDirectory)

    @transient
    val hadoopConf = new org.apache.hadoop.conf.Configuration()

    //parse hdfs://analytics-hadoop from hdfs://analytics-hadoop/blah/blah
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(uri.getScheme().toString + "://" + uri.getHost()), hadoopConf)


    if (hdfs.exists(new org.apache.hadoop.fs.Path(outputDirectory))) {
      // 1. read data file and delete records from this date if any
      val pastSessionData = sc.textFile(outputDirectory).filter(record => !(record.contains(date) || record.contains("Date")))

      // 2. union with records just calculated
      val currentAndPastData = pastSessionData.union(sc.parallelize(Seq(header + outputStats))).coalesce(1, true)

      // 3. delete old file in hadoop
      try {
        hdfs.delete(new org.apache.hadoop.fs.Path(outputDirectory), true)
      } catch {
        case _: Throwable => {}
      }
      // 4. save file
      currentAndPastData.saveAsTextFile(outputDirectory, classOf[GzipCodec])

    } else {
      sc.parallelize(Seq(header + outputStats)).coalesce(1, true).saveAsTextFile(outputDirectory, classOf[GzipCodec])
    }

  }
}

