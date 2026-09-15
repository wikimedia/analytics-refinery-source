package org.wikimedia.analytics.refinery.spark.monitoring

import scala.collection.mutable

/**
 * Rolls the metrics of finished tasks up to the stage attempt that ran them,
 * and hands the result out as [[StageMetrics]].
 *
 * Task metrics are aggregated eagerly, as tasks end, so that only one counter
 * set per stage attempt is retained rather than one per task. This keeps memory
 * flat in the number of tasks, and keeps the output proportional to the number
 * of stages.
 *
 * A stage is the level at which a Spark application is actually diagnosed. A
 * shuffle, a spill or a skewed partition belongs to one stage, and rolling it
 * up to the job hides which one. Every [[StageMetrics]] carries the id of the
 * job that submitted it, so job level figures remain available by summing.
 *
 * Retries are kept apart. A stage that fails and is resubmitted produces one
 * [[StageMetrics]] per attempt, told apart by its attemptId: the work a failed
 * attempt did is real, and folding it into the successful attempt would hide
 * it.
 *
 * This class holds mutable state and is not thread safe. It is meant to be
 * driven from a SparkListener, whose callbacks all run on the single listener
 * bus thread.
 *
 * @param maxTrackedStages  Upper bound on the number of stage attempts kept in
 *                          memory. A long lived driver (a notebook session,
 *                          say) can submit an unbounded number of them; past
 *                          this many, stages are only counted, not measured.
 *                          The same bound is applied to the much lighter
 *                          per-job metadata.
 */
class SparkStageMetricsAggregator(
    maxTrackedStages: Int = SparkStageMetricsAggregator.DefaultMaxTrackedStages
) {

    import SparkStageMetricsAggregator._

    /** Identifies one attempt at one stage. */
    private case class StageKey(stageId: Int, attemptId: Int)

    /** Mutable per-stage-attempt accumulator. */
    private class StageState(val stageId: Int, val attemptId: Int, val jobId: Option[Int]) {
        var name: Option[String] = None
        var numTasks: Long = 0L
        var submissionTimeMs: Option[Long] = None
        var completionTimeMs: Option[Long] = None
        var result: String = ResultIncomplete
        var tasksCompleted: Long = 0L
        var tasksFailed: Long = 0L
        var tasksKilled: Long = 0L
        var taskMetrics: TaskMetricsSnapshot = TaskMetricsSnapshot.Empty

        /** Takes what Spark knows about the stage, without unsetting what it does not. */
        def describe(stage: StageDescriptor): Unit = {
            name = stage.name.orElse(name)
            if (stage.numTasks > 0L) numTasks = stage.numTasks
            submissionTimeMs = stage.submissionTimeMs.orElse(submissionTimeMs)
            completionTimeMs = stage.completionTimeMs.orElse(completionTimeMs)
        }
    }

    /**
     * What a stage needs from the job that submitted it. Jobs are kept only for
     * these two attributes and for the application level job counts, so they
     * stay cheap even in a driver that submits a great many.
     */
    private class JobState(val jobGroup: Option[String]) {
        var result: String = ResultIncomplete
    }

    private val stageToJob = mutable.Map.empty[Int, Int]
    private val stages = mutable.LinkedHashMap.empty[StageKey, StageState]
    private val jobs = mutable.LinkedHashMap.empty[Int, JobState]

    private var appStartTimeMs: Option[Long] = None
    private var appEndTimeMs: Option[Long] = None
    private var untrackedStages: Long = 0L
    private var untrackedJobs: Long = 0L

    /**
     * Counted outside [[jobs]] on purpose, so that a job the aggregator stopped
     * tracking past maxTrackedStages still marks the application as having
     * failed. Losing the detail of which job failed is acceptable; losing the
     * fact that one did is not.
     */
    private var failedJobs: Long = 0L

    def applicationStarted(timeMs: Long): Unit = appStartTimeMs = Some(timeMs)

    def applicationEnded(timeMs: Long): Unit = appEndTimeMs = Some(timeMs)

    def jobStarted(jobId: Int, stageIds: Seq[Int], jobGroup: Option[String] = None): Unit = {
        if (jobs.size >= maxTrackedStages) {
            untrackedJobs += 1L
        } else {
            jobs(jobId) = new JobState(jobGroup)
            // A stage shared with an earlier job is re-pointed at this job. A
            // stage attempt resolves its job when it is first seen, so attempts
            // already under way keep the job they were attributed to.
            stageIds.foreach(stageId => stageToJob(stageId) = jobId)
        }
    }

    def jobEnded(jobId: Int, succeeded: Boolean): Unit = {
        if (!succeeded) failedJobs += 1L
        jobs.get(jobId).foreach { state =>
            state.result = if (succeeded) ResultSucceeded else ResultFailed
        }
        // Stages report before their job does, so dropping the mapping here
        // bounds the size of stageToJob without losing attributions.
        stageToJob.retain((_, mappedJobId) => mappedJobId != jobId)
    }

    def stageSubmitted(stage: StageDescriptor): Unit = {
        stateForStage(stage.stageId, stage.attemptId) match {
            case Some(state) => state.describe(stage)
            // Counted here rather than on task end, so that the figure is a
            // number of stages and not a number of tasks belonging to them.
            case None => untrackedStages += 1L
        }
    }

    def stageCompleted(stage: StageDescriptor): Unit = {
        stateForStage(stage.stageId, stage.attemptId).foreach { state =>
            state.describe(stage)
            state.result = if (stage.failed) ResultFailed else ResultSucceeded
        }
    }

    def taskEnded(stageId: Int, attemptId: Int, outcome: TaskOutcome, metrics: TaskMetricsSnapshot): Unit = {
        stateForStage(stageId, attemptId).foreach { state =>
            outcome match {
                case TaskOutcome.Completed => state.tasksCompleted += 1L
                case TaskOutcome.Failed => state.tasksFailed += 1L
                case TaskOutcome.Killed => state.tasksKilled += 1L
            }
            state.taskMetrics = state.taskMetrics + metrics
        }
    }

    /**
     * Finds a stage attempt, creating it if there is room for it.
     *
     * A stage attempt can first be seen on a task end rather than on a stage
     * submission: the listener may have been registered after the stage
     * started. Creating it here means its tasks are measured anyway, with
     * whatever the stage itself later reports filled in on top. Such a stage
     * may have no known job, in which case its samples say so rather than
     * being dropped.
     */
    private def stateForStage(stageId: Int, attemptId: Int): Option[StageState] = {
        val key = StageKey(stageId, attemptId)
        stages.get(key).orElse {
            if (stages.size >= maxTrackedStages) {
                None
            } else {
                val state = new StageState(stageId, attemptId, stageToJob.get(stageId))
                stages(key) = state
                Some(state)
            }
        }
    }

    /** Number of stage attempts currently being measured. */
    def trackedStageCount: Int = stages.size

    /** Number of jobs whose metadata is currently held. */
    def trackedJobCount: Int = jobs.size

    /**
     * Stage attempts not measured because the application went past
     * maxTrackedStages. Non-zero means the picture is incomplete, so a consumer
     * should say so rather than present its output as the whole run.
     */
    def untrackedStageCount: Long = untrackedStages

    /** Jobs not measured, for the same reason. */
    def untrackedJobCount: Long = untrackedJobs

    /** Jobs of the application that ended in failure. */
    def failedJobCount: Long = failedJobs

    /**
     * How the application as a whole turned out.
     *
     * Derived, not reported: Spark's SparkListenerApplicationEnd carries a
     * timestamp and nothing else, so there is no success flag to read. What can
     * be said is whether the application got as far as reporting an end at all,
     * and whether any of its jobs failed on the way.
     *
     *  - [[ResultIncomplete]] -- no end event. The driver was killed, or the log
     *    was truncated before its tail was written.
     *  - [[ResultFailed]] -- the application ended, but at least one job failed.
     *    Note that this says a job failed, not that the application exited
     *    non-zero: one that catches a job failure and carries on is reported as
     *    failed here. [[failedJobCount]] is the figure to look at when that
     *    distinction matters.
     *  - [[ResultSucceeded]] -- the application ended with no failed job.
     */
    def applicationResult: String =
        if (appEndTimeMs.isEmpty) ResultIncomplete
        else if (failedJobs > 0L) ResultFailed
        else ResultSucceeded

    /** When the application started, if we saw it start. */
    def applicationStartTimeMs: Option[Long] = appStartTimeMs

    /** When the application ended, if we saw it end. */
    def applicationEndTimeMs: Option[Long] = appEndTimeMs

    /**
     * Everything collected so far, one value per stage attempt.
     *
     * This is the aggregator's output. A consumer renders these into whatever
     * shape it needs -- a table row, say -- rather than re-implementing the
     * roll-up.
     */
    def stageMetrics: Seq[StageMetrics] = stages.values.toSeq.map(metricsForStage)


    /** Resolves a stage's mutable state, and its job, into an immutable value. */
    private def metricsForStage(state: StageState): StageMetrics = {
        val job = state.jobId.flatMap(jobs.get)
        StageMetrics(
            stageId = state.stageId,
            attemptId = state.attemptId,
            name = state.name,
            result = state.result,
            numTasks = state.numTasks,
            submissionTimeMs = state.submissionTimeMs,
            completionTimeMs = state.completionTimeMs,
            tasksCompleted = state.tasksCompleted,
            tasksFailed = state.tasksFailed,
            tasksKilled = state.tasksKilled,
            taskMetrics = state.taskMetrics,
            jobId = state.jobId,
            jobResult = job.map(_.result),
            jobGroup = job.flatMap(_.jobGroup)
        )
    }
}

object SparkStageMetricsAggregator {

    val DefaultMaxTrackedStages: Int = 10000

    /** The values [[StageMetrics.result]] and its job's result can take. */
    val ResultSucceeded: String = "succeeded"
    val ResultFailed: String = "failed"
    val ResultIncomplete: String = "incomplete"
}
