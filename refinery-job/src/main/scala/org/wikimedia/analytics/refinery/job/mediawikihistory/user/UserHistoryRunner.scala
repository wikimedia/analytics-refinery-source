package org.wikimedia.analytics.refinery.job.mediawikihistory.user

import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.spark.utils.{MapAccumulator, StatsHelper}


/**
  * This class defines the functions for the user history reconstruction process.
  * It delegates the reconstruction part of it's process to the
  * [[UserHistoryBuilder]] class.
  *
  * The [[run]] function loads [[UserEvent]] and [[UserState]] RDDs from raw path
  * using [[UserEventBuilder]] utilities. It then calls
  * [[UserHistoryBuilder.run]] to partition the RDDs and rebuild history.
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

  import com.databricks.spark.avro._
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
    * Extract and clean [[UserEvent]] and [[UserState]] RDDs,
    * then launch the reconstruction and
    * writes the results (and potentially the errors).
    *
    * @param wikiConstraint The wiki database names on which to execute the job (empty for all wikis)
    * @param loggingDataPath The path of the logging data (avro files partitioned by wiki_db)
    * @param userDataPath The path of the user data (avro files partitioned by wiki_db)
    * @param userGroupsDataPath The path of the user_groups data (avro files partitioned by wiki_db)
    * @param revisionDataPath The path of the revision data (avro files partitioned by wiki_db)
    * @param outputPath The path to output the reconstructed user history (parquet files)
    * @param errorsPathOption An optional path to output errors (csv files) if defined
    */
  def run(
           wikiConstraint: Seq[String],
           loggingDataPath: String,
           userDataPath: String,
           userGroupsDataPath: String,
           revisionDataPath: String,
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

    val loggingDf = spark.read.avro(loggingDataPath)
    loggingDf.createOrReplaceTempView("logging")

    val userDf = spark.read.avro(userDataPath)
    userDf.createOrReplaceTempView("user")

    val userGroupsDf = spark.read.avro(userGroupsDataPath)
    userGroupsDf.createOrReplaceTempView("user_groups")

    val revisionDf = spark.read.avro(revisionDataPath)
    revisionDf.createOrReplaceTempView("revision")

    val wikiClause = if (wikiConstraint.isEmpty) "" else {
      "AND wiki_db IN (" + wikiConstraint.map(w => s"'$w'").mkString(", ") + ")\n"
    }

    val parsedUserEvents = spark.sql(
      s"""
  SELECT
    log_type,
    log_action,
    log_timestamp,
    log_user,
    log_title,
    log_comment,
    log_params,
    wiki_db
  FROM logging
  WHERE
    log_type IN (
      'renameuser',
      'rights',
      'block',
      'newusers'
    )
    $wikiClause
  GROUP BY -- Grouping by to enforce expected partitioning
    log_type,
    log_action,
    log_timestamp,
    log_user,
    log_title,
    log_comment,
    log_params,
    wiki_db
        """)
      .rdd
      .filter(row => {
        val wikiDb = row.getString(7)
        val isValid = UserEventBuilder.isValidLog(row)
        val metricName = if (isValid) METRIC_VALID_LOGS_OK else METRIC_VALID_LOGS_KO
        addOptionalStat(s"$wikiDb.$metricName", 1)
        isValid
      })
      .map(row => {
        val wikiDb = row.getString(7)
        val userEvent = UserEventBuilder.buildUserEvent(row)
        val metricName = if (userEvent.parsingErrors.isEmpty) METRIC_EVENTS_PARSING_OK else METRIC_EVENTS_PARSING_KO
        addOptionalStat(s"$wikiDb.$metricName", 1)
        userEvent
      })

    val userEvents = parsedUserEvents.filter(_.parsingErrors.isEmpty).cache()



    /** *********************************************************
      * HACK
      *   - collect_set function is only available in spark 1.6 using HiveContext
      *   - HiveContext doesn't work when using Spark with Oozie
      *   --> Reimplementing the portion of code without collect_set
      *   --> Update coalesce to in map null change (typing issue)
      */

    val userGroupsSchema = StructType(
      Seq(StructField("wiki_db", StringType, nullable = false),
        StructField("ug_user", LongType, nullable = false),
        StructField("user_groups", ArrayType(StringType, containsNull = false), nullable = false)))

    val userGroupsRdd = spark.sql(
      s"""
    SELECT
      wiki_db,
      ug_user,
      ug_group
    FROM user_groups
      WHERE TRUE
      $wikiClause
        """)
      .rdd
      .map(r => ((r.getString(0), r.getLong(1)), Seq(r.getString(2))))
      .reduceByKey(_ ++ _)
      .map(t => Row(t._1._1, t._1._2, t._2.distinct))

      spark.createDataFrame(userGroupsRdd, userGroupsSchema).createOrReplaceTempView("grouped_user_groups")


    val userStates = spark.sql(
      s"""
  SELECT
    user_id,
    user_name,
    user_registration,
    u.wiki_db,
    rev.rev_timestamp as first_rev_timestamp,
    --coalesce(user_groups, emptyStringArray())
    user_groups
  FROM user AS u
    LEFT JOIN (
      SELECT
        rev_user,
        min(rev_timestamp) as rev_timestamp,
        wiki_db
      FROM revision
      WHERE TRUE
        $wikiClause
      GROUP BY
        rev_user,
        wiki_db
    ) rev
    ON user_id = rev_user
    AND u.wiki_db = rev.wiki_db
    LEFT JOIN grouped_user_groups ug
      -- HACK CONTINUATION
      --(
      --SELECT
      --    wiki_db,
      --    ug_user,
      --    collect_set(ug_group) as user_groups
      --FROM user_groups
      --WHERE TRUE
      --  $wikiClause
      --GROUP BY
      --  wiki_db,
      --  ug_user
      --) ug
    ON u.wiki_db = ug.wiki_db
    AND user_id = ug_user
  WHERE user_id IS NOT NULL
    AND user_name IS NOT NULL -- to prevent any NPE when graph partitioning
    ${wikiClause.replace("wiki_db", "u.wiki_db")}
  GROUP BY -- Grouping by to enforce expected partitioning
    user_id,
    user_name,
    user_registration,
    u.wiki_db,
    rev.rev_timestamp,
    --coalesce(user_groups, emptyStringArray())
    user_groups
        """)
      .rdd
      .map { row =>
        val wikiDb = row.getString(3)
        addOptionalStat(s"$wikiDb.$METRIC_INITIAL_STATES", 1L)
        new UserState(
            userId = row.getLong(0),
            userNameHistorical = row.getString(1),
            userName = row.getString(1),
            userRegistrationTimestamp = (row.getString(2), row.getString(4)) match {
              case (null, null) => None
              case (null, potentialTimestamp) => TimestampHelpers.makeMediawikiTimestamp(potentialTimestamp)
              case (potentialTimestamp, _) => TimestampHelpers.makeMediawikiTimestamp(potentialTimestamp)
            },
            userGroupsHistorical = Seq.empty[String],
            userGroups = if (row.isNullAt(5)) Seq.empty[String] else row.getSeq(5),
            userBlocksHistorical = Seq.empty[String],
            causedByEventType = "create",
            wikiDb = wikiDb
        )}
      .cache()


    /** *********************************************************
      * END OF HACK
      */

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

    log.info(s"User history reconstruction done, writing results, errors and statsde")


    //***********************************
    // Write results
    //***********************************

    spark.createDataFrame(userHistoryRdd.map(state => {
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
          .union(unmatchedEvents.map(e => Row(e.wikiDb, "matching", e.toString))
          ),
        StructType(Seq(
          StructField("wiki_db", StringType, nullable = false),
          StructField("error_type", StringType, nullable = false),
          StructField("event", StringType, nullable = false)
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
