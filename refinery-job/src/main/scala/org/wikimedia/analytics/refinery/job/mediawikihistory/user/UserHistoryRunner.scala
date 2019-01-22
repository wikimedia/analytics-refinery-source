package org.wikimedia.analytics.refinery.job.mediawikihistory.user

import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.job.mediawikihistory.sql.SQLHelper
import org.wikimedia.analytics.refinery.spark.utils.{MapAccumulator, StatsHelper}


/**
  * This class defines the functions for the user history reconstruction process.
  * It delegates the reconstruction part of it's process to the
  * [[UserHistoryBuilder]] class.
  *
  * The [[run]] function loads [[UserEvent]] and [[UserState]] RDDs from expected
  * already defined logging and user views using [[UserEventBuilder]] utilities
  * It then calls [[UserHistoryBuilder.run]] to partition the RDDs and rebuild history.
  *
  * It finally writes the resulting [[UserState]] data in parquet format.
  *
  * Note: You can have errors output as well by providing
  * errorsPath to the [[run]] function.
  */
class UserHistoryRunner(
                         val spark: SparkSession,
                         val statsAccumulator: Option[MapAccumulator[String, Long]],
                         val numPartitions: Int
                       ) extends StatsHelper with Serializable {

  import org.apache.spark.sql.{Row, SaveMode}
  import org.apache.log4j.Logger
  import org.apache.spark.sql.types._
  import org.wikimedia.analytics.refinery.core.TimestampHelpers

  @transient
  lazy val log: Logger = Logger.getLogger(this.getClass)

  val METRIC_VALID_LOGS_OK = "userHistory.validLogs.OK"
  val METRIC_VALID_LOGS_KO = "userHistory.validLogs.KO"
  val METRIC_EVENTS_PARSING_OK = "userHistory.eventsParsing.OK"
  val METRIC_EVENTS_PARSING_KO = "userHistory.eventsParsing.KO"
  val METRIC_INITIAL_STATES = "userHistory.initialStates"
  val METRIC_WRITTEN_ROWS = "userHistory.writtenRows"

  /**
    * Extract [[UserEvent]] and [[UserState]] RDDs from expected already
    * defined logging and user views, then launch the reconstruction and
    * writes the results (and potentially the errors).
    *
    * @param outputPath The path to output the reconstructed user history (parquet files)
    * @param errorsPathOption An optional path to output errors (csv files) if defined
    */
  def run(
           outputPath: String,
           errorsPathOption: Option[String]
  ): Unit = {

    log.info(s"User history jobs starting")

    //***********************************
    // Prepare user events and states RDDs
    //***********************************

    // Work with 4 times more partitions that expected for file production
    // during events and states pre stages
    spark.sql("SET spark.sql.shuffle.partitions=" + 4 * numPartitions)

    val parsedUserEvents = spark.sql(
      // data gathered from already defined logging view
      s"""
  SELECT
    log_type,
    log_action,
    log_timestamp,
    log_user,
    log_user_text,
    log_title,
    log_comment,
    log_params,
    wiki_db
  FROM ${SQLHelper.LOGGING_VIEW}
  WHERE log_type IN (
    'renameuser',
    'rights',
    'block',
    'newusers'
  )
        """)
      .rdd
      .filter(row => {
        val wikiDb = row.getString(8)
        val isValid = UserEventBuilder.isValidLogTitle(row.getString(5))
        val metricName = if (isValid) METRIC_VALID_LOGS_OK else METRIC_VALID_LOGS_KO
        addOptionalStat(s"$wikiDb.$metricName", 1)
        isValid
      })
      .map(row => {
        val wikiDb = row.getString(8)
        val userEvent = UserEventBuilder.buildUserEvent(row)
        val metricName = if (userEvent.parsingErrors.isEmpty) METRIC_EVENTS_PARSING_OK else METRIC_EVENTS_PARSING_KO
        addOptionalStat(s"$wikiDb.$metricName", 1)
        userEvent
      })

    val userEvents = parsedUserEvents.filter(_.parsingErrors.isEmpty).cache()

    val userStates = spark.sql(
    // Data gathered from already defined user view
      s"""
SELECT
  wiki_db,
  user_id,
  user_text,
  user_registration,
  user_first_rev_timestamp,
  user_groups
FROM ${SQLHelper.USER_VIEW}
      """)
      .rdd
      .map { row =>
        val wikiDb = row.getString(0)
        addOptionalStat(s"$wikiDb.$METRIC_INITIAL_STATES", 1L)
        new UserState(
            userId = row.getLong(1),
            userTextHistorical = row.getString(2),
            userText = row.getString(2),
            userRegistrationTimestamp = (row.getString(3), row.getString(4)) match {
              // first-revision-timestamp is defined and parsable by SQL restriction
              case (null, null) => None
              case (null, timestamp) => Some(TimestampHelpers.makeMediawikiTimestamp(timestamp))
              case (potentialTimestamp, null) => TimestampHelpers.makeMediawikiTimestampOption(potentialTimestamp)
              // If both registration and first-revision timestamps are defined, use the oldest
              case (potentialTimestamp1, potentialTimestamp2) =>
                val pt1 = TimestampHelpers.makeMediawikiTimestampOption(potentialTimestamp1)
                val t2 = TimestampHelpers.makeMediawikiTimestamp(potentialTimestamp2)
                if (pt1.isDefined) {
                  if (pt1.get.before(t2)) pt1 else Some(t2)
                } else Some(t2)
            },
            userGroupsHistorical = Seq.empty[String],
            userGroups = if (row.isNullAt(5)) Seq.empty[String] else row.getSeq(5),
            userBlocksHistorical = Seq.empty[String],
            causedByEventType = "create",
            wikiDb = wikiDb
        )}
      .cache()


    log.info(s"User history data defined, starting reconstruction")


    //***********************************
    // Reconstruct user history
    //***********************************

    val userHistoryBuilder = new UserHistoryBuilder(
      spark,
      statsAccumulator
    )
    val (userHistoryRdd, unmatchedEvents) = userHistoryBuilder.run(userEvents, userStates)

    // TODO : Compute is_bot_for_other_wikis

    log.info(s"User history reconstruction done, writing results, errors and stats")


    //***********************************
    // Write results
    //***********************************

    // Drop states having empty registration timestamp. Since registration timestamp
    // is MIN(DB-registration-date, first-edit), no registration timestamp means no
    // edit activity - We drop the data instead of trying to approximate it.
    // TODO: Approximate creation dates using user-id/registration-date coherence.
    spark.createDataFrame(
      userHistoryRdd.filter(_.userRegistrationTimestamp.isDefined).map(state => {
          addOptionalStat(s"${state.wikiDb}.$METRIC_WRITTEN_ROWS", 1)
          state.toRow
        }), UserState.schema)
      .repartition(numPartitions)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath)
    log.info(s"User history reconstruction results written")

    //***********************************
    // Optionally write errors
    //***********************************

    errorsPathOption.foreach(errorsPath => {
      val parsingErrorEvents = parsedUserEvents.filter(_.parsingErrors.nonEmpty)
      val errorDf = spark.createDataFrame(
        parsingErrorEvents.map(e => Row(e.wikiDb, "parsing", e.toString))
          .union(unmatchedEvents.map(e => Row(e.wikiDb, "matching", e.toString)))
          .union(
            userHistoryRdd
              .filter(_.userRegistrationTimestamp.isEmpty)
              .map(s => Row(s.wikiDb, "empty-registration", s.toString))
          ),
        StructType(Seq(
          StructField("wiki_db", StringType, nullable = false),
          StructField("error_type", StringType, nullable = false),
          StructField("data", StringType, nullable = false)
        ))
      )
      errorDf.repartition(1)
        .write
        .mode(SaveMode.Overwrite)
        .format("csv")
        .option("sep", "\t")
        .save(errorsPath)
      log.info(s"User history reconstruction errors written")
    })

    log.info(s"User history jobs done")
  }

}
