package org.wikimedia.analytics.refinery.camus



import java.io.FileInputStream
import java.util.Properties
import javax.mail.{Message, MessagingException, Session, Transport}
import javax.mail.internet.{InternetAddress, MimeMessage}

import com.github.nscala_time.time.Imports._
import com.linkedin.camus.etl.kafka.CamusJob
import com.linkedin.camus.etl.kafka.common.EtlKey
import com.linkedin.camus.etl.kafka.mapred.{EtlInputFormat, EtlMultiOutputFormat}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{LogManager, Logger}
import org.joda.time.{DateTime, Hours}
import scopt.OptionParser
import scala.collection.JavaConverters._

/**
 * Class marking checking camus runs based on a camus.properties file.
 * It flags hdfs imported data for fully imported hours.
 * It logs to console by default.
 *
 * command example (replace [*] with * in classpath - hack to prevent scala comment issue):
 * java -Dlog4j.configuration=file:///home/joal/code/log4j_console.properties \
 *      -cp "/home/joal/code/analytics-refinery-source/refinery-job/target/refinery-job-0.0.21-SNAPSHOT.jar:/usr/lib/spark/lib/[*]:/usr/lib/hadoop/[*]:/usr/lib/hadoop-hdfs/[*]:/usr/lib/hadoop/lib/[*]:/usr/share/java/[*]" \
 *      org.wikimedia.analytics.refinery.job.CamusPartitionChecker -c /home/joal/camus.test.import.properties
 *
 */
object CamusPartitionChecker {

  val BLACKLIST_TOPICS = EtlInputFormat.KAFKA_BLACKLIST_TOPIC
  val WHITELIST_TOPICS = EtlInputFormat.KAFKA_WHITELIST_TOPIC
  val PARTITION_BASE_PATH = EtlMultiOutputFormat.ETL_DESTINATION_PATH


  // Dummy values, to be set with configuration in main
  var fs: FileSystem = FileSystem.get(new Configuration)
  var camusReader: CamusStatusReader = new CamusStatusReader(fs)
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
  def finishedHoursInBetween(t1: Long, t2: Long): Seq[(Int, Int, Int, Int)] = {
    val oldestNextHour = new DateTime(t1 , DateTimeZone.UTC).hourOfDay.roundCeilingCopy
    val youngestPreviousHour = new DateTime(t2, DateTimeZone.UTC).hourOfDay.roundFloorCopy
    for (h <- 0 to Hours.hoursBetween(oldestNextHour, youngestPreviousHour).getHours ) yield {
      val fullHour: DateTime = oldestNextHour + h.hours - 1.hours
      (fullHour.year.get, fullHour.monthOfYear.get, fullHour.dayOfMonth.get, fullHour.hourOfDay.get)
    }
  }

  // Replacing dots by underscores in topic names as per
  // https://github.com/wikimedia/analytics-camus/blob/master/camus-etl-kafka/src/main/java/com/linkedin/camus/etl/kafka/partitioner/DefaultPartitioner.java#L67
  def partitionDirectory(base: String, topic: String, year: Int, month: Int, day: Int, hour: Int): String = {
    if ((! StringUtils.isEmpty(base)) && (! StringUtils.isEmpty(topic)))
      f"${base}%s/${topic.replaceAll("\\.", "_")}%s/hourly/${year}%04d/${month}%02d/${day}%02d/${hour}%02d"
    else
      throw new IllegalArgumentException("Can't make partition directory with empty base or topic.")
  }

  /**
   * Simple container class for passing around topicsAndHours to flag, and any error messages
   * encountered while finding hours to flag.
   * @param topicsAndHours
   * @param errors
   */
  case class CamusPartitionsToFlag(
    topicsAndHours: Map[String, Seq[(Int, Int, Int, Int)]] = Map.empty,
    errors: Seq[String] = Seq.empty
  )

  /** Compute complete hours imported on a camus run by topic. Log errors if
    * the camus run state is not correct (missing topics or import-time not moving),
    * but doesn't prevent other topics to be processed
    * @param camusRunPath the camus run Path folder to use
    * @return CamusPartitionsToFlag containing the partitions to flag and encountered errors
    */
  def getCamusPartitionsToFlag(camusRunPath: Path): CamusPartitionsToFlag = {
    // Empty Whitelist means all --> default to .* regexp
    val topicsWhitelist = "(" + props.getProperty(WHITELIST_TOPICS, ".*").replaceAll(" *, *", "|") + ")"
    // Empty Blacklist means no blacklist --> Default to empty string
    val topicsBlacklist = "(" + props.getProperty(BLACKLIST_TOPICS, "").replaceAll(" *, *", "|") + ")"

    val currentOffsets: Seq[EtlKey] = camusReader.readEtlKeys(camusReader.offsetsFiles(camusRunPath))
    val previousOffsets: Seq[EtlKey] = camusReader.readEtlKeys(camusReader.previousOffsetsFiles(camusRunPath))

    val currentTopicsAndOldestTimes = camusReader.topicsAndOldestTimes(currentOffsets)
    val previousTopicsAndOldestTimes = camusReader.topicsAndOldestTimes(previousOffsets)

    previousTopicsAndOldestTimes.foldLeft(CamusPartitionsToFlag()) {

      case (camusPartitionsToFlag, (previousTopic, previousTime)) => {

        // If this topic should be checked
        if (!previousTopic.matches(topicsBlacklist) && previousTopic.matches(topicsWhitelist)) {

          // If the latest import time is greater than the previous import time,
          // find finished hours in between.
          if (currentTopicsAndOldestTimes.contains(previousTopic) &&
              currentTopicsAndOldestTimes(previousTopic) > previousTime) {
            val hours = finishedHoursInBetween(previousTime, currentTopicsAndOldestTimes(previousTopic))
            camusPartitionsToFlag.copy(
                topicsAndHours = camusPartitionsToFlag.topicsAndHours + (previousTopic -> hours)
            )
          }

          // Else there is probably a problem.
          else {
            val errorMessage = s"Error on topic ${previousTopic} - Latest offset time is either missing or not after the previous run's offset time. Either there has been no new data since the previous run, or Camus is failing to import data."
            log.error(errorMessage)
            camusPartitionsToFlag.copy(
                errors = camusPartitionsToFlag.errors :+ errorMessage
            )
          }
        }

        else
          camusPartitionsToFlag
      }
    }
  }

  def flagFullyImportedPartitions(
    flag: String,
    dryRun: Boolean,
    topicsAndHours: Map[String, Seq[(Int, Int, Int, Int)]]
  ): Unit = {
    for ((topic, hours) <- topicsAndHours) {
      for ((year, month, day, hour) <- hours) {
        val dir = partitionDirectory(
          props.getProperty(PARTITION_BASE_PATH), topic, year, month, day, hour)
        val partitionPath: Path = new Path(dir)
        if (fs.exists(partitionPath) && fs.isDirectory(partitionPath)) {
          val flagPath = new Path(s"${dir}/${flag}")
          if (! dryRun) {
            fs.create(flagPath)
            log.info(s"Flag created: ${dir}/${flag}")
          } else
            log.info(s"DryRun - Flag would have been created: ${dir}/${flag}")
        } else {
          log.error(s"Error on topic ${topic} - Partition folder ${partitionPath} is missing, can't be flagged.")
        }
      }
    }
  }

  case class Params(
    camusPropertiesFilePath: String   = "",
    datetimeToCheck: Option[String]   = None,
    mostRecentRunsToCheck: Int        = 1,
    hadoopCoreSitePath: String        = "/etc/hadoop/conf/core-site.xml",
    hadoopHdfsSitePath: String        = "/etc/hadoop/conf/hdfs-site.xml",
    shouldEmailReport: Boolean        = false,
    smtpURI: String                   = "mx1001.wikimedia.org:25",
    fromEmail: String                 = s"camus_checker@${java.net.InetAddress.getLocalHost.getCanonicalHostName}",
    toEmails: Seq[String]             = Seq("analytics-alerts@wikimedia.org"),
    flag: String                      = "_IMPORTED",
    dryRun: Boolean                   = false
  )

  val argsParser = new OptionParser[Params]("Camus Checker") {
    head("Camus partition checker", "")
    note(
      "This job checked camus runs correctness and flag hour partitions when fully imported.\n" +
        "\tWhen dateTimeToCheck parameter is set, is overrides mostRecentRunsToCheck parameter.")
    help("help") text ("Prints this usage text")

    opt[String]('c', "camus-properties-file") required() valueName ("<path>") action { (x, p) =>
      p.copy(camusPropertiesFilePath = x)
    } text ("Camus configuration properties file path.")

    opt[String]('d', "datetimeToCheck") optional() valueName ("yyyy-mm-dd-HH-MM-SS") action { (x, p) =>
      p.copy(datetimeToCheck = Some(x))
    } text ("Datetime camus run to check (must be present in history folder)")

    opt[Int]('n', "mostRecentRunsToCheck") optional() valueName ("<num>") action { (x, p) =>
      p.copy(mostRecentRunsToCheck = x)
    } validate { x => if (x > 0) success else failure("mostRecentRunsToCheck must be greater than 0")
    } text ("Number of most recent camus runs to check (default to 1, overwritten by datetimeToCheck if set).")

    opt[String]("hadoop-core-site-file") optional() valueName ("<path>") action { (x, p) =>
      p.copy(hadoopCoreSitePath = x)
    } text ("Hadoop core-site.xml file path for configuration.")

    opt[String]("hadoop-hdfs-site-file") optional() valueName ("<path>") action { (x, p) =>
      p.copy(hadoopHdfsSitePath = x)
    } text ("Hadoop hdfs-site.xml file path for configuration.")

    opt[Unit]('E', "send-email-report") optional() action { (_, p) =>
      p.copy(shouldEmailReport = true)
    } text
        "Set this flag if you want an email report of possibly missing hourly data."

    opt[String]('T', "smtp-uri") optional() valueName "<smtp-uri>" action { (x, p) =>
      p.copy(smtpURI = x)
    } text "SMTP server host:port. Default: mx1001.wikimedia.org"

    opt[String]('f', "from-email") optional() valueName "<from-email>" action { (x, p) =>
      p.copy(fromEmail = x)
    } text "Email report from sender email address."

    opt[String]('t', "to-emails") optional() valueName "<to-emails>" action { (x, p) =>
      p.copy(toEmails = x.split(","))
    } text
        "Email report recipient email addresses (comma separated). Default: analytics-alerts@wikimedia.org"

    opt[String]("flag") optional() action { (x, p) =>
      p.copy(flag = x)
    } validate { f =>
      if ((! f.isEmpty) && (f.matches("_[a-zA-Z0-9-_]+"))) success else failure("Incorrect flag file name")
    } text ("Flag file name. Default: _IMPORTED")

    opt[Unit]("dry-run") optional() action { (_, p) =>
      p.copy(dryRun = true)
    } text ("Only print check result and if flag files would have been created.")
  }

  def isLog4JConfigured():Boolean = {
    if (Logger.getRootLogger.getAllAppenders.hasMoreElements)
      return true
    val loggers = LogManager.getCurrentLoggers
    while (loggers.hasMoreElements)
      if (loggers.nextElement.asInstanceOf[Logger].getAllAppenders.hasMoreElements)
        return true
    return false
  }

  def main(args: Array[String]): Unit = {
    if (! isLog4JConfigured)
      org.apache.log4j.BasicConfigurator.configure

    argsParser.parse(args, Params()) match {
      case Some (params) => {
        try {
          log.info("Loading hadoop configuration.")
          val conf: Configuration = new Configuration()
          conf.addResource(new Path(params.hadoopCoreSitePath))
          conf.addResource(new Path(params.hadoopHdfsSitePath))
          fs = FileSystem.get(conf)
          camusReader = new CamusStatusReader(fs)

          log.info("Loading camus properties file.")
          props.load(new FileInputStream(params.camusPropertiesFilePath))
          // Merge any camus property overrides from System properties.
          props.putAll(System.getProperties().asScala.filter(p => props.containsKey(p._1)).asJava)


          val camusPathsToCheck: Seq[Path] = {

            if (params.datetimeToCheck.isDefined) {
              val p = new Path(props.getProperty(CamusJob.ETL_EXECUTION_HISTORY_PATH) + "/" + params.datetimeToCheck.get)
              if (fs.isDirectory(p)) {
                log.info("Set job to given datetime to check.")
                Seq(p)
              } else {
                throw new IllegalArgumentException("The given datetime to check is not a folder in camus history.")
              }
            } else {
              log.info(s"Getting ${params.mostRecentRunsToCheck} camus most recent runs from history folder.")
              val history_folder = props.getProperty(CamusJob.ETL_EXECUTION_HISTORY_PATH)
              camusReader.mostRecentRuns(new Path(history_folder), params.mostRecentRunsToCheck)
            }
          }
          log.info(s"Working ${camusPathsToCheck.size} camus history folders.")
          camusPathsToCheck.foreach(p => {
            log.info(s"Checking ${p.toString}")
            val camusPartitionsToFlag = getCamusPartitionsToFlag(p)
            log.info(s"Flagging imported partitions for ${p.toString}")
            flagFullyImportedPartitions(params.flag, params.dryRun, camusPartitionsToFlag.topicsAndHours)
            log.info(s"Done ${p.toString}.")

            if (params.shouldEmailReport && camusPartitionsToFlag.errors.size > 0) {
              val smtpHost = params.smtpURI.split(":")(0)
              val smtpPort = params.smtpURI.split(":")(1)

              log.info(s"Sending failure email report to ${params.toEmails.mkString(",")}")
              sendEmail(
                smtpHost,
                smtpPort,
                params.fromEmail,
                params.toEmails.toArray,
                s"Camus failure report for ${params.camusPropertiesFilePath}",
                camusPartitionsToFlag.errors.mkString("\n")
              )
            }
          })
        } catch {
          case e: Exception => {
            log.error("A fatal error occurred during execution.", e)
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


  def sendEmail(smtpHost: String, smtpPort: String, fromEmail: String, toEmails: Array[String], subject: String, body: String): Unit = {
    val props  = new Properties
    props.put("mail.smtp.host", smtpHost)
    props.put("mail.smtp.auth", "false")
    props.put("mail.smtp.port", smtpPort)
    val session  = Session.getDefaultInstance(props)
    try {
      val message  = new MimeMessage(session)
      message.setFrom(new InternetAddress(fromEmail))
      for (email <- toEmails) {
        message.addRecipient(Message.RecipientType.TO, new InternetAddress(email))
      }
      message.setSubject(subject)
      message.setText(body)
      Transport.send(message)
    }
    catch {
      case ex: MessagingException =>
        throw new RuntimeException(ex)
    }
  }

}
