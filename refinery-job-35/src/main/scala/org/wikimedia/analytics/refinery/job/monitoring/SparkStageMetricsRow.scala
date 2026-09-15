package org.wikimedia.analytics.refinery.job.monitoring

import java.sql.Timestamp

/**
 * One row of the Spark stage metrics table: one attempt at one stage, with the
 * job and the application that ran it denormalised onto it.
 *
 * Field names are snake_case because the table is schema'd after this class.
 *
 * Durations carry their unit in the column name because Spark does not report
 * them all in the same one: every CPU time and the shuffle write time are
 * nanoseconds, every other duration is milliseconds. They are passed through as
 * Spark reports them rather than normalised, so that a reader comparing these
 * figures with the Spark UI sees the same numbers.
 *
 * `application_start` is the table's time partition, so it is not optional. An
 * event log whose application start was lost produces no rows at all rather than
 * rows that cannot be placed in time.
 */
case class SparkStageMetricsRow(
    // Partition keys, and what identifies the application.
    airflow_id: Option[String],
    application_id: String,
    application_attempt_id: Option[String],
    application_start: Timestamp,
    application_end: Option[Timestamp],
    // Derived from the jobs, not reported by Spark: an event log has no
    // application success flag. See SparkStageMetricsAggregator.applicationResult.
    application_result: String,
    application_name: Option[String],
    spark_user: Option[String],
    spark_version: Option[String],
    spark_master: Option[String],
    // An allow-listed subset of the application's Spark properties, so that a
    // change in how a job is configured is visible next to the change in what it
    // cost. See SparkStageMetricsCollector.ConfigAllowList.
    spark_config: Map[String, String],

    // The job that submitted the stage. Absent when the event log never
    // attributed the stage to one.
    job_id: Option[Int],
    job_group: Option[String],
    job_result: Option[String],

    // The stage attempt. stage_id and stage_attempt together identify it within
    // the application; a retried stage appears once per attempt.
    stage_id: Int,
    stage_attempt: Int,
    stage_name: Option[String],
    stage_result: String,
    stage_submitted: Option[Timestamp],
    stage_completed: Option[Timestamp],
    stage_duration_ms: Option[Long],

    // Task counts.
    num_tasks: Long,
    tasks_completed: Long,
    tasks_failed: Long,
    tasks_killed: Long,

    // Roll-up of the metrics of the attempt's tasks. peak_execution_memory_bytes
    // is not a sum but the largest value any single task reported: summing per-task peaks
    // would not describe anything real.
    executor_run_time_ms: Long,
    executor_cpu_time_ns: Long,
    executor_deserialize_time_ms: Long,
    executor_deserialize_cpu_time_ns: Long,
    jvm_gc_time_ms: Long,
    result_size_bytes: Long,
    result_serialization_time_ms: Long,
    memory_bytes_spilled: Long,
    disk_bytes_spilled: Long,
    peak_execution_memory_bytes: Long,
    input_bytes_read: Long,
    input_records_read: Long,
    output_bytes_written: Long,
    output_records_written: Long,
    shuffle_local_blocks_fetched: Long,
    shuffle_remote_blocks_fetched: Long,
    shuffle_local_bytes_read: Long,
    shuffle_remote_bytes_read: Long,
    shuffle_remote_bytes_read_to_disk: Long,
    shuffle_records_read: Long,
    shuffle_fetch_wait_time_ms: Long,
    shuffle_bytes_written: Long,
    shuffle_records_written: Long,
    shuffle_write_time_ns: Long
)
