package org.wikimedia.analytics.refinery.spark.monitoring

import org.apache.spark.scheduler.StageInfo

/**
 * An immutable copy of what Spark tells us about one attempt at a stage.
 *
 * Like [[TaskMetricsSnapshot]], this exists so that
 * [[SparkStageMetricsAggregator]] can be driven with plain values rather than
 * with Spark's own [[StageInfo]], which carries a live task metrics object and
 * cannot be built outside of org.apache.spark.
 *
 * A stage that is retried is reported as several attempts sharing a stageId
 * and differing in attemptId. The two together identify a timeseries.
 *
 * @param stageId          Stage identifier, unique within the application.
 * @param attemptId        Which attempt at that stage this is, counting from 0.
 * @param name             Spark's name for the stage, e.g. "count at Foo.scala:42".
 * @param numTasks         Number of tasks the stage attempt planned to run.
 * @param submissionTimeMs When the stage attempt was submitted. Spark leaves
 *                         this empty for a stage that was never submitted.
 * @param completionTimeMs When the stage attempt finished, if it has.
 * @param failed           Whether Spark recorded a failure reason for it.
 */
case class StageDescriptor(
    stageId: Int,
    attemptId: Int,
    name: Option[String] = None,
    numTasks: Long = 0L,
    submissionTimeMs: Option[Long] = None,
    completionTimeMs: Option[Long] = None,
    failed: Boolean = false
)

object StageDescriptor {

    /** Copies what Spark reports for a submitted or completed stage attempt. */
    def apply(info: StageInfo): StageDescriptor = StageDescriptor(
        stageId = info.stageId,
        attemptId = info.attemptNumber(),
        name = Option(info.name),
        numTasks = info.numTasks.toLong,
        submissionTimeMs = info.submissionTime,
        completionTimeMs = info.completionTime,
        failed = info.failureReason.isDefined
    )
}
