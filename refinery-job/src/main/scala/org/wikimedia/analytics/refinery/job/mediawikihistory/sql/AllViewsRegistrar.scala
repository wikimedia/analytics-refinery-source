package org.wikimedia.analytics.refinery.job.mediawikihistory.sql

import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.spark.utils.{MapAccumulator, StatsHelper}

/**
 * This class instantiates and runs view registrars for namespace, archive,
 * revision, logging, page and user views.
 *
 * A view registrar augments the SQL spark-context with a named temporary view,
 * allowing SQL queries to be run using this table name. The temporary aspect
 * of the view means it's only accessible in the spark-session it has been
 * registered on, and that it will not be accessible anymore when the spark
 * session terminates.
 *
 * @param spark            the spark session to use
 * @param statsAccumulator the stats accumulator tracking job stats
 * @param numPartitions    the number of partitions to use
 * @param wikiConstraint   the wikis to build the restriction clause. Should be a list
 *                         of wiki-project strings, empty for all.
 * @param readerFormat     The spark reader format to use. Should be one of
 *                         avro, parquet, json, csv
 *                         NOTE: the reader used for NamespaceViewRegistrar is hard-coded
 *                         as CSV.
 */
class AllViewsRegistrar(
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
        contentUnprocessedPath: String,
        commentUnprocessedPath: String,
        loggingUnprocessedPath: String,
        pageUnprocessedPath: String,
        revisionUnprocessedPath: String,
        slotRolesUnprocessedPath: String,
        slotsUnprocessedPath: String,
        userUnprocessedPath: String,
        userGroupsUnprocessedPath: String
    ): Unit = {

        log.info(s"Registering all views")

        spark.sql(s"SET spark.sql.shuffle.partitions=$numPartitions")

        val wikiClause = SQLHelper.inClause("wiki_db", wikiConstraint)

        // Warning: using CSV reader for namespace data, not provided readerFormat
        new NamespaceViewRegistrar(spark, wikiClause, "csv").run(namespacesCSVPath)

        new ChangeTagsViewRegistrar(spark, statsAccumulator, numPartitions, wikiClause, readerFormat)
            .run(changeTagUnprocessedPath, changeTagDefUnprocessedPath)

        new ArchiveViewRegistrar(spark, statsAccumulator, numPartitions, wikiClause, readerFormat)
            .run(actorUnprocessedPath, archiveUnprocessedPath, contentUnprocessedPath,
                revisionUnprocessedPath, slotRolesUnprocessedPath, slotsUnprocessedPath)

        new RevisionViewRegistrar(spark, statsAccumulator, numPartitions, wikiClause, readerFormat)
            .run(actorUnprocessedPath, commentUnprocessedPath, contentUnprocessedPath,
                revisionUnprocessedPath, slotRolesUnprocessedPath, slotsUnprocessedPath)

        new LoggingViewRegistrar(spark, statsAccumulator, numPartitions, wikiClause, readerFormat)
            .run(actorUnprocessedPath, userUnprocessedPath, commentUnprocessedPath, loggingUnprocessedPath)


        // Warning: page, deleted_page and user view registration need to happen
        // AFTER archive and revision one as they are expected to be registered

        new PageViewRegistrar(spark, statsAccumulator, numPartitions, wikiClause, readerFormat)
            .run(pageUnprocessedPath)

        new DeletedPageViewRegistrar(spark, statsAccumulator, numPartitions, wikiClause, readerFormat).run()

        new UserViewRegistrar(spark, statsAccumulator, numPartitions, wikiClause, readerFormat)
            .run(userUnprocessedPath, userGroupsUnprocessedPath)

        log.info(s"All views registered")

    }

}
