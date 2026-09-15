package org.wikimedia.analytics.refinery.spark.monitoring

import org.scalatest.{FlatSpec, Matchers}

class TestSparkStageMetricsAggregator extends FlatSpec with Matchers {

    /** A task that read a gigabyte and shuffled nothing. */
    private val readingTask = TaskMetricsSnapshot(
        executorRunTime = 300L,
        executorCpuTime = 250000000L,
        jvmGcTime = 20L,
        peakExecutionMemory = 100L,
        inputBytesRead = 1073741824L,
        inputRecordsRead = 1000L
    )

    /** One attempt at a stage, submitted and completed over a known window. */
    private def stage(
        stageId: Int,
        attemptId: Int = 0,
        name: String = "count at Test.scala:1",
        numTasks: Long = 1L,
        submittedMs: Long = 1100L,
        completedMs: Option[Long] = Some(1500L),
        failed: Boolean = false
    ): StageDescriptor = StageDescriptor(
        stageId = stageId,
        attemptId = attemptId,
        name = Some(name),
        numTasks = numTasks,
        submissionTimeMs = Some(submittedMs),
        completionTimeMs = completedMs,
        failed = failed
    )

    /** The one stage attempt the aggregator holds, failing if there is not one. */
    private def only(aggregator: SparkStageMetricsAggregator): StageMetrics = {
        val stages = aggregator.stageMetrics
        stages should have size 1
        stages.head
    }

    "A SparkStageMetricsAggregator" should "roll task metrics up to their stage, not their job" in {
        val aggregator = new SparkStageMetricsAggregator()
        aggregator.applicationStarted(1000L)
        aggregator.jobStarted(0, Seq(0, 1))
        Seq(0, 1).foreach { stageId =>
            aggregator.stageSubmitted(stage(stageId))
            aggregator.taskEnded(stageId, 0, TaskOutcome.Completed, readingTask)
            aggregator.stageCompleted(stage(stageId))
        }
        aggregator.jobEnded(0, succeeded = true)
        aggregator.applicationEnded(2000L)

        val stages = aggregator.stageMetrics

        // One value per stage, each carrying only its own stage's tasks.
        stages should have size 2
        stages.map(_.stageId) should contain theSameElementsAs Seq(0, 1)
        all(stages.map(_.taskMetrics.inputBytesRead)) should equal(1073741824L)
        all(stages.map(_.jobId)) should equal(Some(0))
    }

    it should "keep the attempts of a retried stage apart" in {
        val aggregator = new SparkStageMetricsAggregator()
        aggregator.jobStarted(0, Seq(0))

        aggregator.stageSubmitted(stage(0, attemptId = 0))
        aggregator.taskEnded(0, 0, TaskOutcome.Failed, readingTask)
        aggregator.stageCompleted(stage(0, attemptId = 0, failed = true))

        aggregator.stageSubmitted(stage(0, attemptId = 1))
        aggregator.taskEnded(0, 1, TaskOutcome.Completed, readingTask)
        aggregator.stageCompleted(stage(0, attemptId = 1))

        val byAttempt = aggregator.stageMetrics.map(s => s.attemptId -> s).toMap

        // The work the failed attempt did is real, so it is reported too.
        byAttempt.keySet should equal(Set(0, 1))
        byAttempt(0).result should equal(SparkStageMetricsAggregator.ResultFailed)
        byAttempt(1).result should equal(SparkStageMetricsAggregator.ResultSucceeded)
        byAttempt(0).tasksFailed should equal(1L)
        byAttempt(1).tasksCompleted should equal(1L)
    }

    it should "max rather than sum the peak execution memory of a stage's tasks" in {
        val aggregator = new SparkStageMetricsAggregator()
        aggregator.stageSubmitted(stage(0, numTasks = 2L))
        aggregator.taskEnded(0, 0, TaskOutcome.Completed, TaskMetricsSnapshot(peakExecutionMemory = 100L))
        aggregator.taskEnded(0, 0, TaskOutcome.Completed, TaskMetricsSnapshot(peakExecutionMemory = 400L))

        // Summing per-task peaks would not describe anything real.
        only(aggregator).taskMetrics.peakExecutionMemory should equal(400L)
    }

    it should "report the wall clock window of a stage attempt" in {
        val aggregator = new SparkStageMetricsAggregator()
        aggregator.stageSubmitted(stage(0, submittedMs = 1100L, completedMs = Some(1500L)))
        aggregator.taskEnded(0, 0, TaskOutcome.Completed, readingTask)
        aggregator.stageCompleted(stage(0, submittedMs = 1100L, completedMs = Some(1500L)))

        val metrics = only(aggregator)

        metrics.submissionTimeMs should equal(Some(1100L))
        metrics.completionTimeMs should equal(Some(1500L))
        metrics.durationMs should equal(Some(400L))
    }

    it should "describe every stage by its stage and its job" in {
        val aggregator = new SparkStageMetricsAggregator()
        aggregator.jobStarted(7, Seq(3), Some("nightly-refine"))
        aggregator.stageSubmitted(stage(3, name = "save at Refine.scala:120"))
        aggregator.taskEnded(3, 0, TaskOutcome.Completed, readingTask)
        aggregator.stageCompleted(stage(3, name = "save at Refine.scala:120"))
        aggregator.jobEnded(7, succeeded = false)

        val metrics = only(aggregator)

        metrics.stageId should equal(3)
        metrics.attemptId should equal(0)
        metrics.name should equal(Some("save at Refine.scala:120"))
        metrics.result should equal(SparkStageMetricsAggregator.ResultSucceeded)
        metrics.jobId should equal(Some(7))
        // A stage can succeed inside a job that goes on to fail.
        metrics.jobResult should equal(Some(SparkStageMetricsAggregator.ResultFailed))
        metrics.jobGroup should equal(Some("nightly-refine"))
    }

    it should "let a job's totals be recovered by summing its stages" in {
        val aggregator = new SparkStageMetricsAggregator()
        aggregator.jobStarted(0, Seq(0, 1))
        aggregator.jobStarted(1, Seq(2))
        Seq(0, 1, 2).foreach { stageId =>
            aggregator.stageSubmitted(stage(stageId))
            aggregator.taskEnded(stageId, 0, TaskOutcome.Completed, readingTask)
            aggregator.stageCompleted(stage(stageId))
        }
        aggregator.jobEnded(0, succeeded = true)
        aggregator.jobEnded(1, succeeded = true)

        val byJob = aggregator.stageMetrics
            .groupBy(_.jobId)
            .mapValues(_.map(_.taskMetrics.inputBytesRead).sum).toMap

        byJob should equal(Map(Some(0) -> 2 * 1073741824L, Some(1) -> 1073741824L))
    }

    it should "count tasks by outcome, and report how many were planned" in {
        val aggregator = new SparkStageMetricsAggregator()
        aggregator.stageSubmitted(stage(0, numTasks = 4L))
        aggregator.taskEnded(0, 0, TaskOutcome.Completed, readingTask)
        aggregator.taskEnded(0, 0, TaskOutcome.Completed, readingTask)
        aggregator.taskEnded(0, 0, TaskOutcome.Failed, readingTask)
        aggregator.taskEnded(0, 0, TaskOutcome.Killed, readingTask)

        val metrics = only(aggregator)

        metrics.numTasks should equal(4L)
        metrics.tasksCompleted should equal(2L)
        metrics.tasksFailed should equal(1L)
        metrics.tasksKilled should equal(1L)
    }

    it should "measure a stage it only ever sees tasks for" in {
        val aggregator = new SparkStageMetricsAggregator()

        // The collector can be attached after a stage started, so the first the
        // aggregator hears of it is a task ending.
        aggregator.taskEnded(9, 0, TaskOutcome.Completed, readingTask)

        val metrics = only(aggregator)

        metrics.stageId should equal(9)
        metrics.taskMetrics.inputBytesRead should equal(1073741824L)
        // No job was ever seen for it, and that is said rather than guessed.
        metrics.jobId shouldBe empty
        metrics.jobResult shouldBe empty
        metrics.result should equal(SparkStageMetricsAggregator.ResultIncomplete)
    }

    it should "stop measuring stages past maxTrackedStages, and say how many it skipped" in {
        val aggregator = new SparkStageMetricsAggregator(maxTrackedStages = 2)
        aggregator.jobStarted(0, Seq(0, 1, 2, 3, 4))
        (0 to 4).foreach { stageId =>
            aggregator.stageSubmitted(stage(stageId))
            aggregator.taskEnded(stageId, 0, TaskOutcome.Completed, readingTask)
        }

        aggregator.trackedStageCount should equal(2)
        aggregator.stageMetrics should have size 2
        // Three stages were skipped, not the several tasks belonging to them.
        aggregator.untrackedStageCount should equal(3L)
    }

    it should "report an unfinished stage as incomplete, with no duration" in {
        val aggregator = new SparkStageMetricsAggregator()
        aggregator.jobStarted(0, Seq(0))
        aggregator.stageSubmitted(stage(0, completedMs = None))
        aggregator.taskEnded(0, 0, TaskOutcome.Completed, readingTask)

        val metrics = only(aggregator)

        metrics.result should equal(SparkStageMetricsAggregator.ResultIncomplete)
        metrics.completionTimeMs shouldBe empty
        // A stage with no completion time has no meaningful duration.
        metrics.durationMs shouldBe empty
        // Its job never ended either.
        metrics.jobResult should equal(Some(SparkStageMetricsAggregator.ResultIncomplete))
    }

    it should "expose the application window it saw" in {
        val aggregator = new SparkStageMetricsAggregator()
        aggregator.applicationStartTimeMs shouldBe empty
        aggregator.applicationEndTimeMs shouldBe empty

        aggregator.applicationStarted(1000L)
        aggregator.applicationEnded(2000L)

        aggregator.applicationStartTimeMs should equal(Some(1000L))
        aggregator.applicationEndTimeMs should equal(Some(2000L))
    }

    it should "call an application that ended with no failed job a success" in {
        val aggregator = new SparkStageMetricsAggregator()
        aggregator.applicationStarted(1000L)
        aggregator.jobStarted(0, Seq(0))
        aggregator.jobEnded(0, succeeded = true)
        aggregator.applicationEnded(2000L)

        aggregator.failedJobCount should equal(0L)
        aggregator.applicationResult should equal(SparkStageMetricsAggregator.ResultSucceeded)
    }

    it should "call an application that ended with a failed job a failure" in {
        val aggregator = new SparkStageMetricsAggregator()
        aggregator.applicationStarted(1000L)
        aggregator.jobStarted(0, Seq(0))
        aggregator.jobEnded(0, succeeded = true)
        aggregator.jobStarted(1, Seq(1))
        aggregator.jobEnded(1, succeeded = false)
        aggregator.applicationEnded(2000L)

        aggregator.failedJobCount should equal(1L)
        aggregator.applicationResult should equal(SparkStageMetricsAggregator.ResultFailed)
    }

    /**
     * Spark's application end event carries only a timestamp, so there is nothing
     * to read when it never arrives. Not knowing is reported as not knowing.
     */
    it should "call an application that never reported an end incomplete, failed job or not" in {
        val aggregator = new SparkStageMetricsAggregator()
        aggregator.applicationStarted(1000L)
        aggregator.jobStarted(0, Seq(0))
        aggregator.jobEnded(0, succeeded = false)

        aggregator.applicationResult should equal(SparkStageMetricsAggregator.ResultIncomplete)
    }

    /**
     * The failure count is kept outside the per-job map on purpose. A long lived
     * driver stops tracking jobs past the bound, and losing which job failed is
     * acceptable where losing that one did is not.
     */
    it should "count a failed job it stopped tracking" in {
        val aggregator = new SparkStageMetricsAggregator(maxTrackedStages = 1)
        aggregator.applicationStarted(1000L)
        aggregator.jobStarted(0, Seq(0))
        // Past the bound: this job's metadata is not held.
        aggregator.jobStarted(1, Seq(1))
        aggregator.jobEnded(1, succeeded = false)
        aggregator.applicationEnded(2000L)

        aggregator.untrackedJobCount should equal(1L)
        aggregator.failedJobCount should equal(1L)
        aggregator.applicationResult should equal(SparkStageMetricsAggregator.ResultFailed)
    }
}
