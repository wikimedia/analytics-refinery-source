package org.wikimedia.analytics.refinery.job.mediawikihistory.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.wikimedia.analytics.refinery.spark.utils.{MapAccumulator, StatsHelper}

import scala.collection.mutable

/**
 * This class instantiates and runs view-registerer for namespace, archive,
 * revision, logging, page and user views.
 *
 * @param spark the spark session to use
 * @param statsAccumulator the stats accumulator tracking job stats
 * @param numPartitions the number of partitions to use
 * @param wikiConstraint the wikis to build the restriction clause. Should be a list
 *                       of wiki-project strings, empty for all.
 * @param readerFormat The spark reader format to use. Should be one of
 *                     com.databricks.spark.avro, parquet, json, csv
 *                     NOTE: the reader used for NamespaceViewRegisterer is hard-coded
 *                           as CSV.
 */
class AllViewsRegisterer(
  val spark: SparkSession,
  val statsAccumulator: Option[MapAccumulator[String, Long]],
  val numPartitions: Int,
  val wikiConstraint: Seq[String],
  val readerFormat: String
) extends StatsHelper with Serializable {

  import org.apache.log4j.Logger

  @transient
  lazy val log: Logger = Logger.getLogger(this.getClass)

  def run(
    namespacesCSVPath: String,
    actorUnprocessedPath: String,
    archiveUnprocessedPath: String,
    changeTagUnprocessedPath: String,
    changeTagDefUnprocessedPath: String,
    commentUnprocessedPath: String,
    loggingUnprocessedPath: String,
    pageUnprocessedPath: String,
    revisionUnprocessedPath: String,
    userUnprocessedPath: String,
    userGroupsUnprocessedPath: String
  ): Unit = {

    log.info(s"Registering all views")

    spark.sql(s"SET spark.sql.shuffle.partitions=$numPartitions")

    val wikiClause = SQLHelper.inClause("wiki_db", wikiConstraint)

    // Warning: using CSV reader for namespace data, not provided readerFormat
    new NamespaceViewRegisterer(spark, wikiClause, "csv").registerNamespacesView(namespacesCSVPath)

    new ChangeTagsViewRegisterer(spark, statsAccumulator, numPartitions, wikiClause, readerFormat)
      .registerChangeTagsView(changeTagUnprocessedPath, changeTagDefUnprocessedPath)

    new ArchiveViewRegisterer(spark, statsAccumulator, numPartitions, wikiClause, readerFormat)
      .registerArchiveView(actorUnprocessedPath, archiveUnprocessedPath, revisionUnprocessedPath)

    new RevisionViewRegisterer(spark, statsAccumulator, numPartitions, wikiClause, readerFormat)
      .registerRevisionView(actorUnprocessedPath, commentUnprocessedPath, revisionUnprocessedPath)

    new LoggingViewRegisterer(spark, statsAccumulator, numPartitions, wikiClause, readerFormat)
      .registerLoggingView(actorUnprocessedPath, commentUnprocessedPath, loggingUnprocessedPath)


    // Warning: page and user view registration need to happen AFTER archive and revision one
    // as they are expected to be registered

    new PageViewRegisterer(spark, statsAccumulator, numPartitions, wikiClause, readerFormat)
      .registerPageView(pageUnprocessedPath)

    new UserViewRegisterer(spark, statsAccumulator, numPartitions, wikiClause, readerFormat)
      .registerUserView(userUnprocessedPath, userGroupsUnprocessedPath)

    log.info(s"All views registered")

  }

}
