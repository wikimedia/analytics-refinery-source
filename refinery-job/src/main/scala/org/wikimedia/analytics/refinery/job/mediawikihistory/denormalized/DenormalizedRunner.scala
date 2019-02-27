package org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized

import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.job.mediawikihistory.sql.SQLHelper
import org.wikimedia.analytics.refinery.spark.utils.{MapAccumulator, StatsHelper}

/**
  * This class defines the functions for revisions-denormalization process.
  * It delegates the revision-enrichment part of it's process to the
  * [[DenormalizedRevisionsBuilder]] object.
  *
  * The [[run]] function loads user, page and revisions data from raw path
  * and does some cleaning (see [[filterStates]] function). It then calls
  * [[DenormalizedRevisionsBuilder.run]] to enrich and join revisions data,
  * then uses the partition-and-sort-within-partition/zip-partitions trick
  * to denormalize revisions with users and pages data, and users and
  * pages data with user data.
  *
  * It finally writes the union of those denormalized data in parquet format.
  */
class DenormalizedRunner(
                          val spark: SparkSession,
                          val statsAccumulator: Option[MapAccumulator[String, Long]],
                          val numPartitions: Int,
                          val wikiConstraint: Seq[String]
                        ) extends StatsHelper with Serializable {

  import org.apache.spark.sql.{SaveMode, Row}
  import org.apache.spark.sql.types.{StringType, StructField, StructType}
  import scala.reflect.ClassTag
  import org.apache.log4j.Logger
  import org.apache.spark.rdd.RDD
  import java.sql.Timestamp
  import org.wikimedia.analytics.refinery.job.mediawikihistory.page.PageState
  import org.wikimedia.analytics.refinery.job.mediawikihistory.user.UserState
  // Implicit needed to sort by timestamps
  import org.wikimedia.analytics.refinery.core.TimestampHelpers
  import TimestampHelpers.orderedTimestamp

  @transient
  lazy val log: Logger = Logger.getLogger(this.getClass)

  val METRIC_INITIAL_LIVE_REVISIONS = "denormalize.initialLiveRevisions"
  val METRIC_INITIAL_ARCHIVED_REVISIONS = "denormalize.initialArchivedRevisions"

  val METRIC_INITIAL_PAGE_STATES = "denormalize.pageStates.initial"
  val METRIC_WRONG_IDS_PAGE_STATES = "denormalize.pageStates.wrongIds"
  val METRIC_FILTERED_OUT_PAGE_STATES = "denormalize.pageStates.filteredOut"
  val METRIC_INITIAL_PAGE = "denormalize.page.initial"

  val METRIC_INITIAL_USER_STATES = "denormalize.userStates.initial"
  val METRIC_FILTERED_OUT_USER_STATES = "denormalize.userStates.filteredOut"
  val METRIC_INITIAL_USER = "denormalize.user.initial"

  val METRIC_WRITTEN_ROWS = "denormalize.writtenRows"

  val workPartitions = numPartitions * 4


  /**
    * Filters states RDD by the key defined by getStateKey
    * (usually wikiDb, userId or pageId).
    *
    * Removes whole-history-states (having (start, end) = (None, None))
    * if such a state is not the only one for its key.
    *
    * @param states The states RDD
    * @param getStateKey The function providing the state key
    * @tparam S The state type
    * @return The filtered states RDD
    */
  def filterStates[S <: TimeBoundaries](
                                         states: RDD[S],
                                         getStateKey: S => StateKey,
                                         metricName: String
                                      )(implicit cTagS: ClassTag[S]): RDD[S] = {
    // TODO -- Add more complete overlapping-events filtering

    // Extract partition keys for which a whole-history-state exists
    // ((start, end) = (None, None))
    val wholeHistoryStates = states
      .flatMap(s => (s.startTimestamp, s.endTimestamp) match {
        case (None, None) => Seq((getStateKey(s).partitionKey, true))
        case _ => Seq.empty[(PartitionKey, Boolean)]
      })

    // Key join whole-history-states with all-states,
    // count number of states for those keys and keep only
    // those having more than 1 (to be removed)
    val toBeRemovedStates: RDD[(StateKey, Boolean)]= wholeHistoryStates
      .join(states.map(s => (getStateKey(s).partitionKey, true)))
      // Can't use coutByKey since it returns a Map and not a RDD
      .combineByKey(_ => 1L, (agg: Long, _) => agg + 1L, (agg1: Long, agg2: Long) => agg1 + agg2)
      .flatMap {
        case (key, count) =>
          if (count > 1L)
            Seq((new StateKey(key, None.asInstanceOf[Option[Timestamp]], None.asInstanceOf[Option[Timestamp]]), true))
          else
            Seq.empty[(StateKey, Boolean)]
      }

    // Left join all-states with to-be-filtered states
    // and keep only the non-joined ones.
    states
      .keyBy(s => getStateKey(s)) // RDD[(K, State)]
      .leftOuterJoin(toBeRemovedStates) // RDD [(K, (State, Option[_]))]
      .flatMap(t => {
        if (t._2._2.isEmpty) Seq(t._2._1)
        else {
          addOptionalStat(s"${t._1.partitionKey.db}.$metricName", 1)
          Seq.empty[S]
        }
      })
  }

  def getUserStates(userHistoryPath: String, wikiClause: String): RDD[UserState] = {
    val userStatesDf = spark.read.parquet(userHistoryPath).repartition(workPartitions).where(s"TRUE $wikiClause")
    val userStatesToFilter = userStatesDf.rdd.map(r => {
      val state = UserState.fromRow(r)
      addOptionalStat(s"${state.wikiDb}.$METRIC_INITIAL_USER_STATES", 1)
      state
    })
    filterStates[UserState](
      userStatesToFilter,
      DenormalizedKeysHelper.userStateKey,
      METRIC_FILTERED_OUT_USER_STATES
    ).cache()
  }


  def getPageStates(pageHistoryPath: String, wikiClause: String): RDD[PageState] = {
    val pageStatesDf = spark.read.parquet(pageHistoryPath).repartition(workPartitions).where(s"TRUE $wikiClause")
    val pageStatesToFilter = pageStatesDf.rdd
      .map(r => {
        val state = PageState.fromRow(r)
        addOptionalStat(s"${state.wikiDb}.$METRIC_INITIAL_PAGE_STATES", 1)
        state
      })
    filterStates[PageState](
      pageStatesToFilter,
      // We use the regular pageCreationTimestamp as start-timestamp of each page first event
      // as the filtering stage is NOT about the link between page and revision, but about
      // page information correctness
      s => DenormalizedKeysHelper.pageStateKey(s, useFirstEditTimestamp = false),
      METRIC_FILTERED_OUT_PAGE_STATES
    ).cache()
  }

  def getLiveRevisions(wikiClause: String) = {
    spark.sql(
      s"""
  SELECT
    wiki_db,
    rev_timestamp,
    comment_text,
    actor_user,
    actor_name,
    actor_is_anon,
    rev_page,
    rev_id,
    rev_parent_id,
    rev_minor_edit,
    rev_deleted,
    rev_len,
    rev_sha1,
    rev_content_model,
    rev_content_format,
    rev_tags
  FROM ${SQLHelper.REVISION_VIEW}
        """)
      .rdd
      .map(row => {
        val wikiDb = row.getString(0)
        addOptionalStat(s"$wikiDb.$METRIC_INITIAL_LIVE_REVISIONS", 1L)
        MediawikiEvent.fromRevisionRow(row)
      })
  }

  def getArchivedRevisionsNotFiltered(wikiClause: String): RDD[MediawikiEvent] = {
    spark.sql(
      s"""
  SELECT
    a.wiki_db,
    ar_timestamp,
    comment_text,
    actor_user,
    actor_name,
    actor_is_anon,
    ar_page_id,
    ar_title,
    ar_namespace,
    -- Default to non-content namespace
    CASE WHEN n.is_content IS NULL THEN NULL ELSE n.is_content == 1 END as ar_namespace_is_content,
    ar_rev_id,
    ar_parent_id,
    ar_minor_edit,
    ar_deleted,
    ar_len,
    ar_sha1,
    ar_content_model,
    ar_content_format,
    ar_tags
  FROM ${SQLHelper.ARCHIVE_VIEW} a
    LEFT JOIN ${SQLHelper.NAMESPACES_VIEW} n
      ON a.wiki_db = n.wiki_db
        AND a.ar_namespace = n.namespace
      """)
      .rdd
      .map(row => {
        val wikiDb = row.getString(0)
        addOptionalStat(s"$wikiDb.$METRIC_INITIAL_ARCHIVED_REVISIONS", 1L)
        MediawikiEvent.fromArchiveRow(row)
      })
  }


  /**
    * Keep only the most recent archived revision if there are multiple sharing the same rev_id
    */
  def filterArchivedRevisions(archivedRevisionsNotFiltered: RDD[MediawikiEvent]): RDD[MediawikiEvent] = {
    archivedRevisionsNotFiltered.
      keyBy(r => DenormalizedKeysHelper.revisionMediawikiEventKey(r).partitionKey).
      groupByKey().
      flatMap { case (k, revisionsIterator) =>
        if (k.id > 0) {
          // Keep only most recent revision if multiple
          Seq(revisionsIterator.toVector.sortBy(r => r.eventTimestamp).last)
        } else {
          // Don't touch fake Ids
          revisionsIterator.toVector
        }
      }
  }

  /**
    * Extract and clean core data needed for:
    *  - revisions data augmentation
    *  - revisions denormalization with users and pages states
    *  - users and pages denormalization with users
    *  - Union of revisions, users and pages denormalized data
    *  - result output in parquet files
    *
    * @param userHistoryPath The path of the user states built in UserHistory process
    * @param pageHistoryPath The path of the page states built in PageHistory process (parquet files)
    * @param outputPath The path to output the denormalized history (parquet files)
    */
  def run(
           userHistoryPath: String,
           pageHistoryPath: String,
           outputPath: String,
           errorsPathOption: Option[String]
  ): Unit = {

    log.info(s"Denormalized MW Events jobs starting")

    //***********************************
    // Prepare (live-archived) revisions, users and pages history RDDs
    //***********************************

    // Work with 4 times more partitions that expected for file production
    spark.sql("SET spark.sql.shuffle.partitions=" + workPartitions)

    val wikiClause = SQLHelper.inClause("wiki_db", wikiConstraint)

    val userStates = getUserStates(userHistoryPath, wikiClause)
    val pageStates = getPageStates(pageHistoryPath, wikiClause)

    val liveRevisions = getLiveRevisions(wikiClause)
    val archivedRevisionsNotFiltered = getArchivedRevisionsNotFiltered(wikiClause)

    val userMediawikiEvents = userStates.map(userState => {
      addOptionalStat(s"${userState.wikiDb}.$METRIC_INITIAL_USER", 1)
      MediawikiEvent.fromUserState(userState)
    })
    val pageMediawikiEvents = pageStates.map(pageState => {
      addOptionalStat(s"${pageState.wikiDb}.$METRIC_INITIAL_PAGE", 1)
      MediawikiEvent.fromPageState(pageState)
    })

    log.info(s"Denormalized MW Events data defined")

    //***********************************
    // Prepare for revisions updates and MW Events denormalization
    //***********************************

    // Partitioners for partition-sort-zip
    val statePartitioner = new PartitionKeyPartitioner[StateKey](workPartitions)
    val mediawikiEventPartitioner = new PartitionKeyPartitioner[MediawikiEventKey](workPartitions)

    // Partitioned-sorted user and page states for future zipping
    val sortedUserStates = userStates
      .map(userState => {
        val userStateKey = DenormalizedKeysHelper.userStateKey(userState)
        (userStateKey, userState)
      })
      .repartitionAndSortWithinPartitions(statePartitioner)
      .cache()
    val sortedPageStates = pageStates
      .map(pageState => {
        // We use the firstRevisionTimestamp as start-timestamp of each page first event
        // if it's before the pageCreationTimestamp, as sortedPageStates data is to link
        // revisions to pages, and we want this link to happen for revisions having been
        // imported into page and having timestamps before page creation (yes, this happens)
        val pageStateKey = DenormalizedKeysHelper.pageStateKey(pageState, useFirstEditTimestamp = true)
        (pageStateKey,  pageState)
      })
      .repartitionAndSortWithinPartitions(statePartitioner)

    // user and page iterate functions setup
    val userIterate = DenormalizedKeysHelper.leftOuterZip(
      DenormalizedKeysHelper.compareMediawikiEventAndStateKeys,
      MediawikiEvent.updateWithOptionalUser(statsAccumulator)) _
    val pageIterate = DenormalizedKeysHelper.leftOuterZip(
      DenormalizedKeysHelper.compareMediawikiEventAndStateKeys,
      MediawikiEvent.updateWithOptionalPage(statsAccumulator)) _

    // User and page with previous functions setup
    val userMetricsMapperWithPrevious = DenormalizedKeysHelper.mapWithPreviouslyComputed[MediawikiEventKey, MediawikiEvent, MediawikiEvent](
      DenormalizedKeysHelper.compareMediawikiEventPartitionKeys,
      MediawikiEvent.updateWithOptionalUserPrevious(statsAccumulator)
    ) _

    val pageMetricsMapperWithPrevious = DenormalizedKeysHelper.mapWithPreviouslyComputed[MediawikiEventKey, MediawikiEvent, MediawikiEvent](
      DenormalizedKeysHelper.compareMediawikiEventPartitionKeys,
      MediawikiEvent.updateWithOptionalPagePrevious(statsAccumulator)
    ) _


    //***********************************
    // Run revision updates
    //***********************************

    // Remove duplicate revision Ids in archive, use the most recent one
    val archivedRevisions = filterArchivedRevisions(archivedRevisionsNotFiltered)

    val revisions = new DenormalizedRevisionsBuilder(spark, statsAccumulator, workPartitions).run(
      liveRevisions,
      archivedRevisions,
      pageStates,
      mediawikiEventPartitioner
    )

    log.info(s"Repartitioning and sorting denormalized revisions, adding per-user values and zipping with user states")
    val revisionsWithUserData = revisions
      .keyBy(r => DenormalizedKeysHelper.userMediawikiEventKey(r))
      .repartitionAndSortWithinPartitions(mediawikiEventPartitioner)
      .zipPartitions(sortedUserStates)((it1, it2) => userIterate(userMetricsMapperWithPrevious(it1), it2))

    log.info(s"Repartitioning and sorting denormalized revisions, adding per-page values and zipping with page states")
    val revisionsWithUserAndPageData = revisionsWithUserData
      .keyBy(r => DenormalizedKeysHelper.pageMediawikiEventKey(r))
      .repartitionAndSortWithinPartitions(mediawikiEventPartitioner)
      .zipPartitions(sortedPageStates)((it1, it2) => pageIterate(pageMetricsMapperWithPrevious(it1), it2))


    //***********************************
    // Run user and page history updates
    //***********************************

    log.info(s"Repartitioning and sorting denormalized users, zipping with user states")
    val userMediawikiEventsWithUserData = userMediawikiEvents
      .keyBy(u => DenormalizedKeysHelper.userMediawikiEventKey(u))
      .repartitionAndSortWithinPartitions(mediawikiEventPartitioner)
      .zipPartitions(sortedUserStates)((it1, it2) => userIterate(it1, it2))

    log.info(s"Repartitioning and sorting denormalized pages, zipping with user states")
    val pageMediawikiEventsWithUserData = pageMediawikiEvents
      .keyBy(p => DenormalizedKeysHelper.userMediawikiEventKey(p))
      .repartitionAndSortWithinPartitions(mediawikiEventPartitioner)
      .zipPartitions(sortedUserStates)((it1, it2) => userIterate(it1, it2))

    //***********************************
    // Join revisions, user and page and write results and errors
    //***********************************

    log.info(s"Union-ing denormalized revisions, pages and users, and writing results and errors")
    val denormalizedMediawikiEventsRdd = revisionsWithUserAndPageData
      .union(userMediawikiEventsWithUserData)
      .union(pageMediawikiEventsWithUserData)

    //***********************************
    // Write results
    //***********************************

    val denormalizedMediawikiEventsDf = spark.createDataFrame(
        denormalizedMediawikiEventsRdd
          // eventErrors should not be filtered out,
          // They would impair global stats correctness
          //.filter(_.eventErrors.isEmpty)
          .map(event => {
            addOptionalStat(s"${event.wikiDb}.$METRIC_WRITTEN_ROWS", 1)
            event.toRow
          }),
        MediawikiEvent.schema)
    denormalizedMediawikiEventsDf.repartition(workPartitions).write.mode(SaveMode.Overwrite).parquet(outputPath)
    log.info(s"Denormalized MW Events results written")

    //***********************************
    // Optionally write errors
    //***********************************
    errorsPathOption.foreach(errorsPath => {
      val errorDf = spark.createDataFrame(
        denormalizedMediawikiEventsRdd
          .filter(_.eventErrors.nonEmpty)
          .map(e => Row(e.wikiDb, "zipping", e.toString)),
        StructType(Seq(
          StructField("wiki_db", StringType, nullable = false),
          StructField("error_type", StringType, nullable = false),
          StructField("event", StringType, nullable = false)
        ))
      )
      errorDf.repartition(numPartitions / 16).write.mode(SaveMode.Overwrite).format("csv").option("sep", "\t").save(errorsPath)
      log.info(s"Denormalized MW Events errors written")
    })


    log.info(s"Denormalized MW Events jobs done")

  }
}
