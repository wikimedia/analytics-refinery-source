package org.wikimedia.analytics.refinery.job.monitoring

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.TimeZone

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.collection.mutable

import com.github.nscala_time.time.Imports._
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{CollectionAccumulator, LongAccumulator}
import org.wikimedia.analytics.refinery.spark.utils.{SerializableHadoopConfiguration, SparkLogHelper}
import org.wikimedia.analytics.refinery.tools.config._

/**
 * Reads Spark event logs from HDFS and writes per-stage metrics to an Iceberg
 * table.
 *
 * Every Spark application with spark.eventLog.enabled already writes a complete
 * record of its jobs, stages and tasks. This job reads that record rather than
 * asking applications to report metrics themselves, which means it covers every
 * application that runs -- notebooks and ad-hoc work included -- without any of
 * them being configured for it. Writing the log is not free, but it is a cost
 * the drivers already pay; this adds nothing on top of it.
 *
 * Nothing here parses the log. Spark's ReplayListenerBus deserialises each line
 * and posts it to a listener exactly as the live bus would, so
 * [[org.wikimedia.analytics.refinery.spark.monitoring.SparkStageMetricsCollector]]
 * does the aggregation.
 *
 * Logs are selected by modification time, so a run covers the applications that
 * finished in a window. Applications already in the table are skipped: a
 * completed event log never changes, so one that has been read once would yield
 * identical rows. That makes reruns free and safe, which is what stands in for
 * bookkeeping.
 *
 * Runs on Spark 3.5, which is what this module is built against. It reads logs
 * written by any 3.x: Spark's own JsonProtocol deserialises them, and that is
 * what lets one job cover a cluster running several Spark versions at once.
 *
 * Example:
 * {{{
 * spark3-submit --class org.wikimedia.analytics.refinery.job.monitoring.SparkEventLogMetrics \
 *   refinery-job-35.jar \
 *   --event_log_dir hdfs:///var/log/spark \
 *   --table wmf.spark_stage_metrics \
 *   --since 24 --until 0
 * }}}
 */
object SparkEventLogMetrics extends SparkLogHelper with ConfigHelper {

    /**
     * @param event_log_dir       Directory Spark writes event logs to.
     * @param table               Fully qualified Iceberg table to write.
     * @param since               Read logs modified at or after this. An integer
     *                            is read as a number of hours ago.
     * @param until               Read logs modified before this.
     * @param dedup_lookback_days How far back to look for application ids already
     *                            in the table. Must exceed the longest
     *                            application, or its rows could be written twice.
     * @param reprocess           Delete the rows of the applications being read
     *                            before writing them, instead of skipping them.
     * @param require_airflow_id  Skip applications with no airflow id, rather
     *                            than writing them to the null partition.
     * @param max_applications    Stop after this many logs, oldest first. 0 means
     *                            no limit. For bounding a first run or a backfill
     *                            over the whole retained backlog.
     * @param coalesce_files      Numbers of partition to coalesce to before writing.
     * @param dry_run             Report what would be written, and write nothing.
     */
    case class Config(
        event_log_dir: String = "",
        table: String = "",
        since: DateTime = DateTime.now - 24.hours,
        until: DateTime = DateTime.now,
        dedup_lookback_days: Int = 30,
        reprocess: Boolean = false,
        require_airflow_id: Boolean = true,
        max_applications: Int = 0,
        coalesce_files: Int = 1,
        dry_run: Boolean = false
    )

    object Config {
        val defaults = Config()

        val usage: String =
            """|Read Spark event logs and write per-stage metrics to an Iceberg table.
               |
               |One row per stage attempt, with the job and application that ran it
               |denormalised onto it. The table is expected to exist already, created
               |by refinery HQL, and to be partitioned by airflow_id and
               |months(application_start).
               |
               |Applications already present in the table are skipped, so rerunning a
               |window is safe and cheap.
               |
               |Example:
               |
               |  spark3-submit --class org.wikimedia.analytics.refinery.job.monitoring.SparkEventLogMetrics \
               |    /srv/deployment/analytics/refinery/artifacts/refinery-job-35.jar \
               |    --event_log_dir hdfs:///var/log/spark \
               |    --table wmf.spark_stage_metrics \
               |    --since 24 --until 0
               |"""
                .stripMargin

        val propertiesDoc: ListMap[String, String] = ListMap(
            "config_file" ->
                """Config properties file. Properties specified in the command line
                  |override those specified in the config file.""".stripMargin,
            "event_log_dir" -> "Directory Spark writes event logs to. Mandatory.",
            "table" -> "Fully qualified Iceberg table to write to. Mandatory.",
            "since" ->
                s"""Read event logs modified at or after this. Either an ISO-8601 datetime
                   |or an integer number of hours ago. Default: 24 hours ago.""".stripMargin,
            "until" ->
                s"""Read event logs modified before this. Either an ISO-8601 datetime or an
                   |integer number of hours ago. Default: now.""".stripMargin,
            "dedup_lookback_days" ->
                s"""How far back to look for application ids already written, so they can be
                   |skipped. Must be longer than the longest application, or its rows could
                   |be written twice. Default: ${defaults.dedup_lookback_days}.""".stripMargin,
            "reprocess" ->
                s"""Delete the rows of the applications being read before writing them,
                   |instead of skipping applications already present.
                   |Default: ${defaults.reprocess}.""".stripMargin,
            "require_airflow_id" ->
                s"""Skip applications that carry no ${AirflowIdKey}, instead of writing them
                   |to the null partition. Default: ${defaults.require_airflow_id}.""".stripMargin,
            "max_applications" ->
                s"""Read at most this many event logs, oldest first. 0 means no limit.
                   |For bounding one run rather than for sampling: the first run against
                   |the ~90 days of logs the cluster keeps, and backfills, where reading
                   |the whole backlog in one application is not what you want.
                   |Default: ${defaults.max_applications}.""".stripMargin,
            "coalesce_files" ->
                s"""Numbers of partition to coalesce to before writing.
                   |Default: ${defaults.coalesce_files}.""".stripMargin,
            "dry_run" ->
                s"""Report what would be written and write nothing.
                   |Default: ${defaults.dry_run}.""".stripMargin
        )
    }

    private val AirflowIdKey =
        org.wikimedia.analytics.refinery.spark.monitoring.SparkStageMetricsCollector.AirflowIdKey

    /**
     * An event log worth reading, and how much of it there is.
     *
     * The size is carried from the directory listing that found the log, so that
     * the replay can be scheduled longest-first without asking the namenode
     * again.
     *
     * @param path      The event log file, or the rolling event log directory.
     * @param sizeBytes Total bytes to replay.
     */
    private[monitoring] case class EventLogCandidate(path: String, sizeBytes: Long)

    /** How many incomplete applications [[reportIncomplete]] names before summarising. */
    private val IncompleteReportLimit: Int = 20

    /** Names that are never an event log. */
    private def isCandidate(status: FileStatus): Boolean = {
        val name = status.getPath.getName
        // Hidden files, _SUCCESS-style markers, and applications still writing.
        !name.startsWith("_") && !name.startsWith(".") && !name.endsWith(".inprogress")
    }

    def main(args: Array[String]): Unit = {
        if (args.contains("--help")) {
            println(help(Config.usage, Config.propertiesDoc))
            sys.exit(0)
        }

        val config = loadConfig(args)
        val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
        adjustLoggingLevelToSparkMaster(spark)

        // Capture deploy mode before stopping the session, to decide on the exit code.
        val exitOnFinish = spark.conf.get("spark.master") != "yarn" ||
            spark.conf.get("spark.submit.deployMode") == "client"

        val success = try {
            apply(spark, config)
            true
        } catch {
            case e: Exception =>
                log.error("Failed reading Spark event logs.", e)
                false
        } finally {
            spark.stop()
        }

        if (exitOnFinish) sys.exit(if (success) 0 else 1)
    }

    def loadConfig(args: Array[String]): Config = {
        val config = try {
            configureArgs[Config](args)
        } catch {
            case e: ConfigHelperException =>
                log.fatal(e.getMessage + ". Aborting.")
                sys.exit(1)
        }
        validate(config)
        log.info("Loaded configuration:\n" + prettyPrint(config))
        config
    }

    private def validate(config: Config): Unit = {
        require(config.event_log_dir.nonEmpty, "--event_log_dir is mandatory.")
        require(config.table.nonEmpty, "--table is mandatory.")
        require(config.since < config.until, "--since must be before --until.")
        require(config.dedup_lookback_days > 0, "--dedup_lookback_days must be positive.")
    }

    /**
     * Checks the table is there before anything is read.
     *
     * The job does not create it -- the DDL lives in refinery, so that the
     * partitioning and the column comments are reviewed where every other table
     * is. Without this check a missing table surfaces much later, as whatever
     * `spark.table` throws from inside the column alignment, after a full window
     * of logs has already been replayed.
     *
     * A dry run is exempt: reporting what would be written is useful before the
     * table exists.
     */
    private def requireTable(spark: SparkSession, config: Config): Unit =
        if (!config.dry_run && !spark.catalog.tableExists(config.table)) {
            throw new IllegalArgumentException(
                s"${config.table} does not exist. Create it with the refinery HQL " +
                    "create_spark_stage_metrics_table.hql, or pass --dry_run to see what " +
                    "would be written without it."
            )
        }

    /**
     * Puts the rows in the table's column order, so a schema that has drifted
     * fails loudly here rather than writing values into the wrong columns.
     */
    private[monitoring] def alignToTable(spark: SparkSession, config: Config, rows: DataFrame): DataFrame = {
        val tableColumns = spark.table(config.table).schema.fieldNames
        rows.select(tableColumns.map(col): _*)
    }

    /**
     * Appends already-aligned rows to the configured Iceberg table.
     *
     * Separated from [[apply]] so that a test can capture what would be written
     * instead of writing it, which is what keeps the cases about selection,
     * ordering and column alignment free of a catalog. The real path is covered
     * too, by TestSparkEventLogMetricsIceberg.
     */
    private def appendToTable(spark: SparkSession, config: Config, rows: DataFrame): Unit =
        // repartition, not coalesce. coalesce does not shuffle: it merges the
        // upstream partitions into fewer tasks, and the replay is in that same
        // stage, so coalesce(1) would replay every event log in a single task.
        // repartition puts a shuffle boundary between the replay and the write,
        // so the replay keeps its parallelism and only the rows -- tens of MB at
        // most -- pass through the narrowed write.
        rows.repartition(config.coalesce_files).writeTo(config.table).append()

    /**
     * Reads the event logs in the configured window and writes their stage
     * metrics. Returns the number of rows written.
     *
     * @param write How to write the rows. Defaults to appending to the
     *              configured table; tests pass their own.
     *
     *              It must *consume* the DataFrame. The returned row count is
     *              gathered by an accumulator as the rows stream into the write,
     *              which is what keeps this to one pass -- so a writer that only
     *              holds on to the DataFrame without running an action on it
     *              makes this method report zero rows.
     */
    def apply(
        spark: SparkSession,
        config: Config,
        write: (SparkSession, Config, DataFrame) => Unit = appendToTable
    ): Long = {
        requireTable(spark, config)

        val candidates = listCandidates(spark, config)
        if (candidates.isEmpty) {
            log.info(s"No event logs modified between ${config.since} and ${config.until}. Nothing to do.")
            return 0L
        }
        log.info(s"Found ${candidates.size} candidate event logs.")

        val toRead = if (config.reprocess) candidates else skipAlreadyWritten(spark, config, candidates)
        if (toRead.isEmpty) {
            log.info("Every candidate event log is already in the table. Nothing to do.")
            return 0L
        }
        log.info(s"Reading ${toRead.size} event logs.")

        // Event logs are wildly uneven: most replay in milliseconds, and a single
        // large application can take minutes on its own. A stage therefore costs
        // roughly the worst log in it, not the average, and a second pass over
        // the replay costs that worst log all over again. Caching is a poor
        // defence, because the partition that is expensive to recompute is also
        // the one too big to keep.
        //
        // So the normal path makes exactly one pass: the skip reasons and the
        // row count are gathered into accumulators as the rows stream into the
        // write, rather than each costing an action of their own.
        val skipped = spark.sparkContext.collectionAccumulator[String]("skippedEventLogs")
        val incomplete = spark.sparkContext.collectionAccumulator[String]("incompleteApplications")
        val rowCounter = spark.sparkContext.longAccumulator("stageMetricsRows")

        import spark.implicits._
        val replayed: Dataset[SparkStageMetricsRow] =
            spark.createDataset(replayAll(spark, config, toRead, skipped, incomplete, rowCounter))

        // dry_run and reprocess genuinely need to look at the rows before
        // writing them, so they pay to hold them. MEMORY_AND_DISK_SER because
        // whatever does not fit must spill rather than be replayed: RDD.cache()
        // is MEMORY_ONLY and silently recomputes what it had to drop.
        val needsSecondPass = config.dry_run || config.reprocess
        val rows = if (needsSecondPass) replayed.persist(StorageLevel.MEMORY_AND_DISK_SER) else replayed

        try {
            if (config.dry_run) {
                val rowCount = rows.count()
                log.info(s"DRY-RUN, would have written $rowCount rows to ${config.table}. First rows:")
                rows.show(5, truncate = false)
                reportSkipped(skipped)
                reportIncomplete(incomplete)
                return rowCount
            }

            if (config.reprocess) {
                deleteApplications(spark, config, rows.map(r => (r.application_id, r.application_start)).distinct().collect())
            }

            // An empty write is a no-op for Iceberg, so there is no count to
            // check first: that check would have cost a pass of its own.
            write(spark, config, alignToTable(spark, config, rows.toDF()))

            reportSkipped(skipped)
            reportIncomplete(incomplete)
            val written = rowCounter.value
            log.info(s"Wrote $written rows for ${toRead.size} applications to ${config.table}.")
            written
        } finally {
            if (needsSecondPass) rows.unpersist()
        }
    }

    /**
     * Formats an instant as a SQL timestamp literal the session reads back as
     * that same instant.
     *
     * Both `Timestamp.toString` and joda's printer render in a JVM-local zone,
     * while a `TIMESTAMP '...'` literal is parsed with
     * `spark.sql.session.timeZone`. The stored `application_start` is an absolute
     * instant, so when the two zones differ an interpolated literal is silently
     * shifted by the offset -- which on the reprocess DELETE means the predicate
     * matches nothing and the append adds a second copy of every row instead of
     * replacing it. Formatting in the session's own zone is what keeps the
     * literal and the column talking about the same moment.
     */
    private[monitoring] def sqlTimestamp(spark: SparkSession, millis: Long): String = {
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        format.setTimeZone(TimeZone.getTimeZone(spark.conf.get("spark.sql.session.timeZone")))
        format.format(new java.util.Date(millis))
    }

    /** Event logs in the directory whose modification time falls in the window. */
    private[monitoring] def listCandidates(spark: SparkSession, config: Config): Seq[EventLogCandidate] = {
        val dir = new Path(config.event_log_dir)
        val fs: FileSystem = dir.getFileSystem(spark.sparkContext.hadoopConfiguration)
        val sinceMs = config.since.getMillis
        val untilMs = config.until.getMillis

        // listStatusIterator, not listStatus. The directory holds every event log
        // the cluster has kept -- hundreds of thousands of them -- and listStatus
        // builds the whole array in the driver before anything is filtered.
        // The iterator pages the listing instead (dfs.ls.limit, 1000 by default),
        // so only the matches are retained.
        //
        // The filter cannot be pushed any further than this. Modification time is
        // not something the namenode can be asked to filter on, and Hadoop's
        // listStatus(Path, PathFilter) applies its filter client side too, so it
        // would move nothing off the driver.
        val matches = mutable.ArrayBuffer.empty[FileStatus]
        val listing = fs.listStatusIterator(dir)
        while (listing.hasNext) {
            val status = listing.next()
            if (isCandidate(status) &&
                status.getModificationTime >= sinceMs &&
                status.getModificationTime < untilMs) {
                matches += status
            }
        }

        // Oldest first, so that a max_applications cut takes the oldest backlog
        // rather than an arbitrary slice of it.
        val selected = matches.sortBy(_.getModificationTime).toSeq

        val capped = if (config.max_applications > 0) selected.take(config.max_applications) else selected
        capped.map(status => EventLogCandidate(status.getPath.toString, sizeOf(fs, status)))
    }

    /**
     * How much there is to replay for one candidate.
     *
     * A rolling event log is a directory, and a directory's own length is zero,
     * so it has to be asked for its parts. That costs one listing per rolling
     * log and nothing at all for a single-file one, so whichever shape the
     * cluster settles on, only the logs that need the extra listing pay for it.
     */
    private def sizeOf(fs: FileSystem, status: FileStatus): Long =
        if (status.isDirectory) fs.listStatus(status.getPath).map(_.getLen).sum else status.getLen

    /**
     * The order to replay candidates in: biggest first.
     *
     * Event logs are wildly uneven -- most replay in milliseconds and one can
     * take minutes -- and a stage costs roughly its worst task, not its average.
     * Spark dispatches tasks in partition order, so starting the longest logs
     * first stops a slow one being picked up last and added to the tail. This is
     * longest-processing-time-first scheduling; it only matters when there are
     * more logs than executor slots, and is harmless when there are fewer,
     * because then every task starts at once.
     */
    private[monitoring] def replayOrder(candidates: Seq[EventLogCandidate]): Seq[String] =
        candidates.sortBy(_.sizeBytes)(Ordering[Long].reverse).map(_.path)

    /**
     * Drops candidates whose application is already in the table.
     *
     * An event log is named after the application that wrote it, so the id can be
     * matched without opening anything. See [[applicationOf]] for the shapes a
     * name can take, and why this compares for equality rather than by prefix.
     *
     * The lookup is a Set membership test per candidate rather than a scan of
     * every id in the window, which matters: `written` holds every application of
     * the last dedup_lookback_days, and a scan would be back to doing work in the
     * driver proportional to the product of the two.
     */
    private def skipAlreadyWritten(
        spark: SparkSession,
        config: Config,
        candidates: Seq[EventLogCandidate]
    ): Seq[EventLogCandidate] = {
        if (!spark.catalog.tableExists(config.table)) {
            log.info(s"${config.table} does not exist yet, reading every candidate.")
            return candidates
        }

        val horizon = sqlTimestamp(spark, (config.since - config.dedup_lookback_days.days).getMillis)

        val written: Set[String] = spark
            .sql(
                s"""SELECT DISTINCT application_id FROM ${config.table}
                   |WHERE application_start >= TIMESTAMP '$horizon'""".stripMargin
            )
            .collect()
            .map(_.getString(0))
            .toSet

        val (skipped, keep) =
            candidates.partition(c => candidateApplicationIds(c.path).exists(written.contains))
        if (skipped.nonEmpty) {
            log.info(s"Skipping ${skipped.size} event logs whose application is already in ${config.table}.")
        }
        keep
    }

    /** A trailing `_<digits>`, which on an event log name is the application attempt. */
    private val AttemptSuffix = """_\d+$""".r

    /**
     * The id of the application an event log's name describes.
     *
     * Spark names a log `<applicationId>`, with `_<attempt>` appended when YARN
     * restarted the driver and `.<codec>` when the log is compressed; a rolling
     * log is a directory with all that behind an `eventlog_v2_` prefix. Taking
     * the name apart gives an id that can be compared for equality.
     *
     * Equality, and not "some id in the table is a prefix of this name", which is
     * what this used to do. A YARN application id ends in a counter that is zero
     * padded to four digits and then simply grows, so once a cluster passes ten
     * thousand applications `application_1757246400000_10000` starts with
     * `application_1757246400000_1000` -- and that application would be treated
     * as already written on every run until its log aged out, without ever being
     * reported as skipped.
     *
     * Stripping the attempt can take the counter off an id that never had an
     * attempt (`application_1757246400000_0001` becomes
     * `application_1757246400000`). That is harmless: the result is compared for
     * equality against ids read from the table, and a truncated id matches none
     * of them.
     */
    private[monitoring] def applicationOf(path: String): String = {
        val name = new Path(path).getName
        val unrolled =
            if (name.startsWith(EventLogReplay.RollingLogDirPrefix)) {
                name.substring(EventLogReplay.RollingLogDirPrefix.length)
            } else {
                name
            }
        // A codec extension, if any. Neither a YARN nor a local application id
        // contains a dot, so everything from the first one is suffix.
        val uncompressed = unrolled.indexOf('.') match {
            case -1 => unrolled
            case dot => unrolled.substring(0, dot)
        }
        uncompressed
    }

    /**
     * The ids an event log's name could be naming: the name itself, and the name
     * with an attempt suffix removed. One of the two is the application id, and
     * which one cannot be told from the name alone -- an id ends in digits too.
     */
    private[monitoring] def candidateApplicationIds(path: String): Seq[String] = {
        val name = applicationOf(path)
        Seq(name) ++ AttemptSuffix.findFirstIn(name).map(suffix => name.dropRight(suffix.length))
    }

    /** Replays one event log per task, so a failure retries one log. */
    private def replayAll(
        spark: SparkSession,
        config: Config,
        candidates: Seq[EventLogCandidate],
        skipped: CollectionAccumulator[String],
        incomplete: CollectionAccumulator[String],
        rowCounter: LongAccumulator
    ): RDD[SparkStageMetricsRow] = {
        val serializableConf = new SerializableHadoopConfiguration(spark.sparkContext.hadoopConfiguration)
        val requireAirflowId = config.require_airflow_id

        val paths = replayOrder(candidates)

        spark.sparkContext
            .parallelize(paths, paths.size)
            .flatMap { path =>
                val replayed = EventLogReplay.replay(path, serializableConf.value, requireAirflowId)
                replayed.skipReason.foreach(reason => skipped.add(s"$reason\t${replayed.path}"))
                if (replayed.untrackedStages > 0L) {
                    incomplete.add(s"${replayed.untrackedStages}\t${replayed.path}")
                }
                rowCounter.add(replayed.rows.size.toLong)
                replayed.rows
            }
    }

    /**
     * Logs why any log produced nothing, grouped so one bad day is one message.
     *
     * Read from an accumulator rather than from a pass of its own. Accumulator
     * updates made in a transformation are counted again if a task is retried,
     * so treat these figures as a report rather than as a total: a duplicated
     * line here means a task was re-run, not that a log was read twice.
     */
    private def reportSkipped(skipped: CollectionAccumulator[String]): Unit = {
        val entries = skipped.value.asScala.map(_.split("\t", 2)).collect {
            case Array(reason, path) => (reason, path)
        }
        if (entries.nonEmpty) {
            val byReason = entries.groupBy(_._1).map {
                case (reason, logs) => s"  $reason: ${logs.size} (e.g. ${logs.head._2})"
            }
            log.warn(
                s"${entries.size} event logs produced no rows:\n" + byReason.mkString("\n")
            )
        }
    }

    /**
     * Warns about applications whose rows describe only part of the run.
     *
     * An application that submits more stage attempts than the aggregator tracks
     * -- a long lived notebook session, typically -- has the rest counted but not
     * measured. Its rows are still worth having; they are just not the whole
     * application, and a reader summing them would get a number that is quietly
     * too small. Same accumulator caveat as [[reportSkipped]].
     */
    private def reportIncomplete(incomplete: CollectionAccumulator[String]): Unit = {
        val entries = incomplete.value.asScala.map(_.split("\t", 2)).collect {
            case Array(count, path) => (count, path)
        }
        if (entries.nonEmpty) {
            // Listed rather than grouped -- unlike the skip report, each line here
            // is a different application -- but capped, so that a backfill across
            // a great many notebook sessions is one readable message.
            val shown = entries.take(IncompleteReportLimit)
            val rest = entries.size - shown.size
            log.warn(
                s"${entries.size} applications went past max tracked stages; their rows " +
                    s"describe only part of the run:\n" +
                    shown.map { case (count, path) => s"  $path: $count stage attempts unmeasured" }
                        .mkString("\n") +
                    (if (rest > 0) s"\n  ... and $rest more" else "")
            )
        }
    }

    /**
     * Removes the rows of the applications about to be written.
     *
     * application_id is not a partition column, so on its own it prunes nothing:
     * the delete would have to open every data file in the table to find out
     * which ones hold those ids, and under copy-on-write rewrite each one it
     * touches. The applications being rewritten all started inside the window
     * this run read, so bounding the predicate by application_start -- which is
     * a partition column -- cuts that down to the months actually involved.
     */
    private def deleteApplications(
        spark: SparkSession,
        config: Config,
        applications: Array[(String, Timestamp)]
    ): Unit = {
        // Every candidate can be read and still yield no rows -- an application
        // with no airflow id under require_airflow_id, say -- and there is then
        // nothing to delete and no window to delete it in.
        if (applications.isEmpty) {
            log.info("Nothing to delete before rewriting: the logs read produced no rows.")
            return
        }

        val starts = applications.map(_._2.getTime)
        val from = sqlTimestamp(spark, starts.min)
        val to = sqlTimestamp(spark, starts.max)

        // Chunked so the predicate stays a sane size on a large backfill.
        applications.map(_._1).grouped(500).foreach { chunk =>
            val inList = chunk.map(id => s"'$id'").mkString(",")
            spark.sql(
                s"""DELETE FROM ${config.table}
                   |WHERE application_start >= TIMESTAMP '$from'
                   |  AND application_start <= TIMESTAMP '$to'
                   |  AND application_id IN ($inList)""".stripMargin
            )
        }
        log.info(
            s"Deleted existing rows for ${applications.length} applications started between " +
                s"$from and $to, before rewriting them."
        )
    }
}
