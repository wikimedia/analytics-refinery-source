package org.wikimedia.analytics.refinery.job.monitoring

import java.io.{File, FileInputStream, InputStream}
import java.sql.Timestamp
import scala.util.control.NonFatal
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.history.EventLogFileReader
import org.apache.spark.scheduler.EventLogReplayer
import org.wikimedia.analytics.refinery.spark.monitoring.{SparkStageMetricsCollector, StageMetrics}

/**
 * Turns one Spark event log into table rows.
 *
 * Nothing here parses the log. Spark's own ReplayListenerBus deserialises each
 * line and posts it to a listener exactly as the live bus would, so
 * [[SparkStageMetricsCollector]] rolls tasks up by the same rules whether it is
 * running inside the application or reading its log afterward.
 *
 * Results are returned in [[Replayed.skipReason]] and reported by the driver.
 */
object EventLogReplay {

    /**
     * The three names Spark's RollingEventLogFilesWriter builds a rolling event
     * log out of. They are `private[spark]` constants there, so they are repeated
     * here; a Spark release that changed them would be a behaviour change this
     * code would have to follow anyway.
     */
    private[monitoring] val RollingLogDirPrefix: String = "eventlog_v2_"
    private val RollingLogFilePrefix: String = "events_"
    private val RollingLogStatusPrefix: String = "appstatus_"

    /**
     * The outcome of replaying one event log.
     *
     * @param path            The log that was read.
     * @param rows            One row per stage attempt. Empty when skipReason is set.
     * @param skipReason      Why the log produced nothing, if it produced nothing.
     * @param untrackedStages Stage attempts the aggregator counted but did not
     *                        measure, having gone past its bound. Non-zero means
     *                        the rows describe only part of the application, which
     *                        the job reports rather than passing off as the whole.
     */
    case class Replayed(
        path: String,
        rows: Seq[SparkStageMetricsRow],
        skipReason: Option[String] = None,
        untrackedStages: Long = 0L
    )

    /**
     * Replays one event log and renders its stage attempts as rows.
     *
     * A log that cannot be read at all is reported rather than thrown, so that
     * one corrupt log does not fail a run over thousands of them. A log whose
     * tail was lost -- the application was killed before Spark finished writing
     * -- is replayed as far as it goes, which is the point of reading logs at
     * all: the work that completed before the kill is still described.
     *
     * @param pathStr           The event log file, or the rolling log directory.
     * @param conf              Hadoop configuration, for resolving the filesystem.
     * @param requireAirflowId  Skip applications that carry no airflow id.
     */
    def replay(pathStr: String, conf: Configuration, requireAirflowId: Boolean = false): Replayed = {
        val path = new Path(pathStr)
        try {
            val fs = path.getFileSystem(conf)
            EventLogFileReader(fs, path) match {
                case None =>
                    Replayed(pathStr, Nil, Some(whyUnreadable(fs, path)))
                case Some(reader) if !reader.completed =>
                    // The application is still running. Its log would be a
                    // partial picture, and it will be complete by the next run.
                    Replayed(pathStr, Nil, Some("application still running"))
                case Some(reader) =>
                    // A rolling log is several files that must be replayed in
                    // order into the same listener; listEventLogFiles gives them
                    // in that order.
                    val sources = reader.listEventLogFiles.map { status =>
                        (status.getPath.toString, () => EventLogFileReader.openEventLog(status.getPath, fs))
                    }
                    replayAll(pathStr, sources, requireAirflowId)
            }
        } catch {
            case NonFatal(e) =>
                Replayed(pathStr, Nil, Some(s"${e.getClass.getSimpleName}: ${e.getMessage}"))
        }
    }

    /**
     * Why Spark would not open a path as an event log.
     *
     * `EventLogFileReader` answers None for every reason there is, which leaves a
     * run reporting hundreds of identically unexplained paths. These are the three
     * cases it refuses, worked out from the path itself.
     *
     * Two things are deliberately not handled here. A missing path never reaches
     * this point -- `EventLogFileReader` resolves the status itself and throws,
     * which the caller catches -- so the status can be asked for without checking
     * first. And an ordinary file that simply is not an event log is *accepted*
     * by Spark and fails later, when the replay finds no application start in it.
     * Only directories and hidden files are turned away at this point.
     */
    private def whyUnreadable(fs: FileSystem, path: Path): String = {
        val status = fs.getFileStatus(path)
        if (!status.isDirectory) {
            // isSingleEventLog turns away a hidden file; listCandidates normally
            // filters those out before we get here.
            "a hidden file, which Spark will not read as an event log"
        } else if (!path.getName.startsWith(RollingLogDirPrefix)) {
            s"a directory, but not a rolling event log: its name does not start with $RollingLogDirPrefix"
        } else {
            // A rolling event log directory is only read when it holds both an
            // index of parts and the marker saying whether the application
            // finished. One without the other is a log still being written, or one
            // whose cleanup was interrupted. Spark checks for both before handing
            // back a reader, so at least one of them is missing here.
            val names = fs.listStatus(status.getPath).map(_.getPath.getName)
            val missing = Seq(
                if (names.exists(_.startsWith(RollingLogFilePrefix))) None else Some(s"$RollingLogFilePrefix*"),
                if (names.exists(_.startsWith(RollingLogStatusPrefix))) None else Some(s"$RollingLogStatusPrefix*")
            ).flatten
            s"rolling event log directory with no ${missing.mkString(" and no ")} file"
        }
    }

    /**
     * Replays a local event log file.
     *
     * Visible for testing. Opening a Hadoop FileSystem resolves the OS user,
     * which is not always possible in a test environment, so tests reach the
     * replay through here instead. Everything after the stream is opened --
     * deserialising the events, the roll-up and building the rows -- is the same
     * code either way.
     */
    private[monitoring] def replayFile(file: File, requireAirflowId: Boolean = false): Replayed = {
        try {
            replayAll(file.getPath, Seq((file.getPath, () => new FileInputStream(file))), requireAirflowId)
        } catch {
            case NonFatal(e) =>
                Replayed(file.getPath, Nil, Some(s"${e.getClass.getSimpleName}: ${e.getMessage}"))
        }
    }

    /** Replays every file of one application into one collector, and renders it. */
    private def replayAll(
        path: String,
        sources: Seq[(String, () => InputStream)],
        requireAirflowId: Boolean
    ): Replayed = {
        val collector = SparkStageMetricsCollector.forReplay()
        val replayer = new EventLogReplayer(collector)

        sources.foreach { case (name, open) =>
            val in = open()
            try {
                replayer.replay(in, name)
            } finally {
                in.close()
            }
        }

        rowsFrom(collector, requireAirflowId) match {
            case Left(reason) => Replayed(path, Nil, Some(reason))
            case Right(rows) => Replayed(path, rows, untrackedStages = collector.untrackedStageCount)
        }
    }

    /** Renders a replayed application, or says why it cannot be rendered. */
    private[monitoring] def rowsFrom(
        collector: SparkStageMetricsCollector,
        requireAirflowId: Boolean
    ): Either[String, Seq[SparkStageMetricsRow]] = {
        val airflowId = collector.sparkProperty(SparkStageMetricsCollector.AirflowIdKey)

        // application_start is the time partition and application_id identifies
        // the row, so neither may be missing. Both come from
        // SparkListenerApplicationStart, which is absent only from a log whose
        // head was lost.
        (collector.applicationStartTimeMs, collector.applicationId) match {
            case _ if requireAirflowId && airflowId.isEmpty =>
                Left(s"no ${SparkStageMetricsCollector.AirflowIdKey} in the application's Spark properties")
            case (None, _) =>
                Left("no application start event")
            case (_, None) =>
                Left("no application id")
            case (Some(startMs), Some(applicationId)) =>
                val start = new Timestamp(startMs)
                val end = collector.applicationEndTimeMs.map(new Timestamp(_))
                Right(collector.stageMetrics.map { stage =>
                    row(stage, airflowId, start, end, applicationId, collector)
                })
        }
    }

    private def row(
        stage: StageMetrics,
        airflowId: Option[String],
        start: Timestamp,
        end: Option[Timestamp],
        applicationId: String,
        collector: SparkStageMetricsCollector
    ): SparkStageMetricsRow = {
        val m = stage.taskMetrics
        SparkStageMetricsRow(
            airflow_id = airflowId,
            application_start = start,
            application_id = applicationId,
            application_end = end,
            application_result = collector.applicationResult,
            application_name = collector.applicationName,
            application_attempt_id = collector.applicationAttemptId,
            spark_user = collector.applicationUser,
            spark_version = collector.sparkVersion,
            spark_master = collector.sparkProperty("spark.master"),
            spark_config = collector.sparkConfig,

            job_id = stage.jobId,
            job_group = stage.jobGroup,
            job_result = stage.jobResult,

            stage_id = stage.stageId,
            stage_attempt = stage.attemptId,
            stage_name = stage.name,
            stage_result = stage.result,
            stage_submitted = stage.submissionTimeMs.map(new Timestamp(_)),
            stage_completed = stage.completionTimeMs.map(new Timestamp(_)),
            stage_duration_ms = stage.durationMs,

            num_tasks = stage.numTasks,
            tasks_completed = stage.tasksCompleted,
            tasks_failed = stage.tasksFailed,
            tasks_killed = stage.tasksKilled,

            executor_run_time_ms = m.executorRunTime,
            executor_cpu_time_ns = m.executorCpuTime,
            executor_deserialize_time_ms = m.executorDeserializeTime,
            executor_deserialize_cpu_time_ns = m.executorDeserializeCpuTime,
            jvm_gc_time_ms = m.jvmGcTime,
            result_size_bytes = m.resultSize,
            result_serialization_time_ms = m.resultSerializationTime,
            memory_bytes_spilled = m.memoryBytesSpilled,
            disk_bytes_spilled = m.diskBytesSpilled,
            peak_execution_memory_bytes = m.peakExecutionMemory,
            input_bytes_read = m.inputBytesRead,
            input_records_read = m.inputRecordsRead,
            output_bytes_written = m.outputBytesWritten,
            output_records_written = m.outputRecordsWritten,
            shuffle_local_blocks_fetched = m.shuffleLocalBlocksFetched,
            shuffle_remote_blocks_fetched = m.shuffleRemoteBlocksFetched,
            shuffle_local_bytes_read = m.shuffleLocalBytesRead,
            shuffle_remote_bytes_read = m.shuffleRemoteBytesRead,
            shuffle_remote_bytes_read_to_disk = m.shuffleRemoteBytesReadToDisk,
            shuffle_records_read = m.shuffleRecordsRead,
            shuffle_fetch_wait_time_ms = m.shuffleFetchWaitTime,
            shuffle_bytes_written = m.shuffleBytesWritten,
            shuffle_records_written = m.shuffleRecordsWritten,
            shuffle_write_time_ns = m.shuffleWriteTime
        )
    }
}
