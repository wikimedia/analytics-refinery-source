package org.wikimedia.analytics.refinery.spark.monitoring

/**
 * Everything known about one attempt at one stage: what Spark said about the
 * stage, the roll-up of the metrics of its tasks, and the job that submitted it.
 *
 * This is the value that every consumer of this package renders from. The event
 * log batch job turns it into a table row. Anything else that wants these
 * numbers renders them too, rather than re-implementing the roll-up, so that
 * the descriptions cannot drift apart as the rules here change.
 *
 * @param stageId          Stage identifier, unique within the application.
 * @param attemptId        Which attempt at that stage this is, counting from 0.
 * @param name             Spark's name for the stage.
 * @param result           succeeded, failed, or incomplete.
 * @param numTasks         Number of tasks the stage attempt planned to run.
 * @param submissionTimeMs When the stage attempt was submitted.
 * @param completionTimeMs When the stage attempt finished, if it did.
 * @param tasksCompleted   Tasks that completed.
 * @param tasksFailed      Tasks that failed.
 * @param tasksKilled      Tasks that were killed.
 * @param taskMetrics      Roll-up of the metrics of every task that reported.
 * @param jobId            Job that submitted the attempt, if we ever saw it.
 * @param jobResult        Result of that job, if we know it.
 * @param jobGroup         Job group of that job, if it had one.
 */
case class StageMetrics(
    stageId: Int,
    attemptId: Int,
    name: Option[String],
    result: String,
    numTasks: Long,
    submissionTimeMs: Option[Long],
    completionTimeMs: Option[Long],
    tasksCompleted: Long,
    tasksFailed: Long,
    tasksKilled: Long,
    taskMetrics: TaskMetricsSnapshot,
    jobId: Option[Int],
    jobResult: Option[String],
    jobGroup: Option[String]
) {

    /**
     * Wall clock duration of the attempt, when both ends are known. A stage that
     * was never submitted, or that never finished, has no duration rather than a
     * made up one.
     */
    def durationMs: Option[Long] = for {
        submitted <- submissionTimeMs
        completed <- completionTimeMs
    } yield completed - submitted
}
