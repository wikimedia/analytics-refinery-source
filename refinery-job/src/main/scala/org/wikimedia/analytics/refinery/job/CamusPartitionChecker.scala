package org.wikimedia.analytics.refinery.job

import java.io.FileInputStream
import java.util.Properties

import com.github.nscala_time.time.Imports._
import com.linkedin.camus.etl.kafka.CamusJob
import com.linkedin.camus.etl.kafka.common.EtlKey
import com.linkedin.camus.etl.kafka.mapred.{EtlInputFormat, EtlMultiOutputFormat}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.joda.time.{DateTime, Hours}
import org.wikimedia.analytics.refinery.camus.CamusStatusReader
import scopt.OptionParser

/**
 * Created by jo on 9/25/15.
 */
object CamusPartitionChecker {

  val BLACKLIST_TOPICS = EtlInputFormat.KAFKA_BLACKLIST_TOPIC
  val WHITELIST_TOPICS = EtlInputFormat.KAFKA_WHITELIST_TOPIC
  val PARTITION_BASE_PATH = EtlMultiOutputFormat.ETL_DESTINATION_PATH

  // Only using HDFS, no need for proper config
  val fs = FileSystem.get(new Configuration)
  val camusReader: CamusStatusReader = new CamusStatusReader
  val props: Properties = new Properties
  val log: Logger = Logger.getLogger(CamusPartitionChecker.getClass)

  /**
   * Computes calendar hours happening between two timestamps. For instance
   * if  t1 =  = 2015-09-25 03:28:12
   * and t2 =  = 2015-09-25 06:55:32
   * Returned result is
   *     [(2015, 9, 25, 4), (2015, 9, 25, 5),(2015, 9, 25, 6)]
   * @param t1 The first timestamp (oldest)
   * @param t2 The second timestamp (youngest)
   * @return the hours having happened between t1 and t2 in format (year, month, day, hour)
   */
  def hoursInBetween(t1: Long, t2: Long): Seq[(Int, Int, Int, Int)] = {
    val oldestNextHour = new DateTime(t1 , DateTimeZone.UTC).hourOfDay.roundCeilingCopy
    val youngestPreviousHour = new DateTime(t2, DateTimeZone.UTC).hourOfDay.roundFloorCopy
    for (h <- 0 to Hours.hoursBetween(oldestNextHour, youngestPreviousHour).getHours ) yield {
      val fullHour = oldestNextHour + h.hours
      (fullHour.year.get, fullHour.monthOfYear.get, fullHour.dayOfMonth.get, fullHour.hourOfDay.get)
    }
  }

  def partitionDirectory(base: String, topic: String, year: Int, month: Int, day: Int, hour: Int): String = {
    if ((! StringUtils.isEmpty(base)) && (! StringUtils.isEmpty(topic)))
      f"${base}%s/${topic}%s/hourly/${year}%04d/${month}%02d/${day}%02d/${hour}%02d"
    else
      throw new IllegalArgumentException("Can't make partition directory with empty base or topic.")
  }

  /** Compute complete hours imported on a camus run by topic. Throws an IllegalStateException if
    * the camus run state is not correct (missing topics or import-time not moving)
    * @param camusRunPath the camus run Path folder to use
    * @return a map of topic -> Seq[(year, month, day, hour)]
    */
  def getTopicsAndHoursToFlag(camusRunPath: Path): Map[String, Seq[(Int, Int, Int, Int)]] = {
    // Empty Whitelist means all --> default to .* regexp
    val topicsWhitelist = "(" + props.getProperty(WHITELIST_TOPICS, ".*").replaceAll(" *, *", "|") + ")"
    // Empty Blacklist means no blacklist --> Default to empty string
    val topicsBlacklist = "(" + props.getProperty(BLACKLIST_TOPICS, "").replaceAll(" *, *", "|") + ")"

    val currentOffsets: Seq[EtlKey] = camusReader.readEtlKeys(camusReader.offsetsFiles(camusRunPath))
    val previousOffsets: Seq[EtlKey] = camusReader.readEtlKeys(camusReader.previousOffsetsFiles(camusRunPath))

    val currentTopicsAndOldestTimes = camusReader.topicsAndOldestTimes(currentOffsets)
    val previousTopicsAndOldestTimes = camusReader.topicsAndOldestTimes(previousOffsets)

    previousTopicsAndOldestTimes.foldLeft(
      Map.empty[String, Seq[(Int, Int, Int, Int)]])((map, previousTopicAndTime) => {
        val (previousTopic, previousTime) = previousTopicAndTime
        if ((! previousTopic.matches(topicsBlacklist)) &&
            (previousTopic.matches(topicsWhitelist))) {
          if ((currentTopicsAndOldestTimes.contains(previousTopic)) &&
            (currentTopicsAndOldestTimes.get(previousTopic).get > previousTime)) {
            val hours = hoursInBetween(previousTime, currentTopicsAndOldestTimes.get(previousTopic).get)
            map + (previousTopic -> hours)
          } else
            throw new IllegalStateException(
              s"Error on topic ${previousTopic} - New offset time is either missing, either not after the previous one")
        } else map
      })
  }

  def flagFullyImportedPartitions(flag: String,
                                  topicsAndHours: Map[String, Seq[(Int, Int, Int, Int)]]): Unit = {
    for ((topic, hours) <- topicsAndHours) {
      for ((year, month, day, hour) <- hours) {
        val dir = partitionDirectory(
          props.getProperty(PARTITION_BASE_PATH), topic, year, month, day, hour)
        val partitionPath: Path = new Path(dir)
        if (fs.exists(partitionPath) && fs.isDirectory(partitionPath)) {
          val flagPath = new Path(s"${dir}/${flag}")
          fs.create(flagPath)
        } else
          throw new IllegalStateException(
            s"Error on topic ${topic} - Partition folder ${partitionPath} is missing, can't be flagged.")
      }
    }
  }

  case class Params(camusPropertiesFilePath: String = "",
                     flag: String = "_IMPORTED")

  val argsParser = new OptionParser[Params]("Camus Checker") {
    head("Camus partition checker", "")
    note(
      "This job checked for most recent camus run correctness and flag hour partitions when fully imported.")
    help("help") text ("Prints this usage text")

    opt[String]('c', "camus-properties-file") required() valueName ("<path>") action { (x, p) =>
      p.copy(camusPropertiesFilePath = x)
    } text ("Camus configuration properties file path.")

    opt[String]('f', "flag") optional() action { (x, p) =>
      p.copy(flag = x)
    } validate { f =>
      if ((! f.isEmpty) && (f.matches("_[a-zA-Z0-9-_]+"))) success else failure("Incorrect flag file name")
    } text ("Flag file to be used (defaults to '_IMPORTED'.")
  }

  def main(args: Array[String]): Unit = {
    argsParser.parse(args, Params()) match {
      case Some (params) => {
        try {
          log.info("Loading camus properties file.")
          props.load(new FileInputStream(params.camusPropertiesFilePath))

          log.info("Getting camus most recent run from history folder.")
          val mostRecentCamusRun = camusReader.mostRecentRun (
            new Path (props.getProperty(CamusJob.ETL_EXECUTION_HISTORY_PATH)))

          log.info("Checking job correctness and computing partitions to flag as imported.")
          val topicsAndHours = getTopicsAndHoursToFlag(mostRecentCamusRun)

          log.info("Flag imported partitions.")
          flagFullyImportedPartitions(params.flag, topicsAndHours)

          log.info("Done.")
        } catch {
          case e: Exception => {
            log.error("An error occured during execution.", e)
            sys.exit(1)
          }
        }
      }
      case None => {
        log.error("No parameter passed. Please run with --help to see options.")
        sys.exit(1)
      }
    }

  }


}
