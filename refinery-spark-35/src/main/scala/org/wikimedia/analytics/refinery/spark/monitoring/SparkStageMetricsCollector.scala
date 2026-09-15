package org.wikimedia.analytics.refinery.spark.monitoring

import org.apache.spark.{SparkConf, Success => TaskSucceeded, TaskKilled}
import org.apache.spark.scheduler._
import org.wikimedia.analytics.refinery.tools.LogHelper

/**
 * Translates the callbacks of Spark's listener bus into [[SparkStageMetricsAggregator]]
 * calls, and holds what the events say about the application they came from.
 *
 * The translation is kept separate from any particular consumer of it, because
 * there are two ways to drive it:
 *
 *  - after the fact, by replaying an application's event log through Spark's
 *    own ReplayListenerBus, which is what the SparkEventLogMetrics batch job in
 *    refinery-job-35 does; and
 *  - live, from a Spark application that registered a listener of its own.
 *
 * Both roll tasks up by the same rules. Any consumer reads the result through
 * [[stageMetrics]].
 *
 * Nothing here is thread safe: the listener bus calls a listener from a single
 * thread, and a replay is single threaded by construction.
 *
 * @param conf             Configuration of the application this collector runs
 *                         in, if it runs in one. A replay has none, and takes
 *                         everything from the events instead.
 * @param maxTrackedStages See [[SparkStageMetricsAggregator]].
 */
class SparkStageMetricsCollector(
    conf: Option[SparkConf] = None,
    maxTrackedStages: Int = SparkStageMetricsAggregator.DefaultMaxTrackedStages
) extends SparkListener with LogHelper {

    import SparkStageMetricsCollector._

    protected val aggregator: SparkStageMetricsAggregator =
        new SparkStageMetricsAggregator(maxTrackedStages)

    /**
     * The Spark properties of the application the events describe. Spark logs
     * these in SparkListenerEnvironmentUpdate, so on a replay they are the
     * original application's, not the replaying job's.
     */
    private var eventProperties: Map[String, String] = Map.empty

    /**
     * Spark version, from SparkListenerLogStart. That event is only ever written
     * to an event log, so this stays empty when running live.
     */
    private var eventSparkVersion: Option[String] = None

    protected var appName: Option[String] = conf.flatMap(_.getOption("spark.app.name"))
    protected var appId: Option[String] = conf.flatMap(_.getOption("spark.app.id"))
    protected var appAttemptId: Option[String] = None
    protected var sparkUser: Option[String] = None

    override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = guard("onApplicationStart") {
        appName = Some(applicationStart.appName)
        appId = applicationStart.appId.orElse(appId)
        appAttemptId = applicationStart.appAttemptId
        sparkUser = Some(applicationStart.sparkUser)
        aggregator.applicationStarted(applicationStart.time)
    }

    override def onJobStart(jobStart: SparkListenerJobStart): Unit = guard("onJobStart") {
        val jobGroup = Option(jobStart.properties).flatMap(p => Option(p.getProperty(JobGroupProperty)))
        aggregator.jobStarted(
            jobStart.jobId,
            jobStart.stageInfos.map(_.stageId),
            jobGroup
        )
    }

    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = guard("onJobEnd") {
        val succeeded = jobEnd.jobResult match {
            case JobSucceeded => true
            case _ => false
        }
        aggregator.jobEnded(jobEnd.jobId, succeeded)
    }

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = guard("onStageSubmitted") {
        aggregator.stageSubmitted(StageDescriptor(stageSubmitted.stageInfo))
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = guard("onStageCompleted") {
        aggregator.stageCompleted(StageDescriptor(stageCompleted.stageInfo))
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = guard("onTaskEnd") {
        val outcome = taskEnd.reason match {
            case TaskSucceeded => TaskOutcome.Completed
            case _: TaskKilled => TaskOutcome.Killed
            case _ => TaskOutcome.Failed
        }
        // taskMetrics is null for tasks that failed before reporting anything.
        val metrics = Option(taskEnd.taskMetrics)
            .map(TaskMetricsSnapshot(_))
            .getOrElse(TaskMetricsSnapshot.Empty)

        aggregator.taskEnded(taskEnd.stageId, taskEnd.stageAttemptId, outcome, metrics)
    }

    override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = guard("onEnvironmentUpdate") {
        environmentUpdate.environmentDetails.get(SparkPropertiesKey).foreach { properties =>
            eventProperties = properties.toMap
        }
    }

    /**
     * Picks up SparkListenerLogStart, which carries the Spark version and which
     * the listener bus routes here because it has no callback of its own.
     */
    override def onOtherEvent(event: SparkListenerEvent): Unit = guard("onOtherEvent") {
        event match {
            case logStart: SparkListenerLogStart => eventSparkVersion = Some(logStart.sparkVersion)
            case _ => ()
        }
    }

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = guard("onApplicationEnd") {
        aggregator.applicationEnded(applicationEnd.time)
    }

    /** One value per stage attempt seen so far. */
    def stageMetrics: Seq[StageMetrics] = aggregator.stageMetrics

    def applicationName: Option[String] = appName

    def applicationId: Option[String] = appId

    def applicationAttemptId: Option[String] = appAttemptId

    def applicationUser: Option[String] = sparkUser

    def applicationStartTimeMs: Option[Long] = aggregator.applicationStartTimeMs

    def applicationEndTimeMs: Option[Long] = aggregator.applicationEndTimeMs

    /** See [[SparkStageMetricsAggregator.applicationResult]]. */
    def applicationResult: String = aggregator.applicationResult

    /** See [[SparkStageMetricsAggregator.untrackedStageCount]]. */
    def untrackedStageCount: Long = aggregator.untrackedStageCount

    /**
     * Spark version of the application the events describe, when the events say.
     *
     * Empty rather than guessed. The version worth recording is the one the
     * application being described ran on, and only SparkListenerLogStart carries
     * it. Falling back to the version this code is running on would be right
     * when running live and confidently wrong on a replay, where it is the batch
     * job's own version -- so it is left unanswered instead.
     */
    def sparkVersion: Option[String] = eventSparkVersion

    /**
     * A Spark property of the application the events describe, falling back to
     * the configuration of the application this collector runs in.
     */
    def sparkProperty(key: String): Option[String] =
        eventProperties.get(key).orElse(conf.flatMap(_.getOption(key)))

    /**
     * The settings an application ran with, so that a change in how a job is
     * configured can be seen next to the change in what it cost.
     *
     * Only the keys in [[SparkStageMetricsCollector.ConfigAllowList]] are
     * returned. An allow list, not a deny list: the properties map holds
     * whatever the submitter put on the command line, which in an ad-hoc job can
     * include credentials, and nothing here should be the thing that writes them
     * to a table everyone can read.
     *
     * Empty rather than absent when the events said nothing, so that "ran with
     * no interesting settings" and "we never saw the environment" are not told
     * apart by a null -- an event log without SparkListenerEnvironmentUpdate has
     * lost its head and produces no rows at all.
     */
    def sparkConfig: Map[String, String] =
        eventProperties.filterKeys(k => ConfigAllowList.exists(k.startsWith)).toMap

    /**
     * Runs body, logging and swallowing anything it throws. A listener that
     * throws is removed from the listener bus, and would take the metrics of the
     * rest of the application with it.
     */
    protected def guard(callback: String)(body: => Unit): Unit = {
        try {
            body
        } catch {
            // An exhausted JVM is not ours to hide.
            case e: VirtualMachineError => throw e
            case e: Throwable =>
                log.error(s"${getClass.getSimpleName}.$callback failed, metrics may be incomplete.", e)
        }
    }
}

object SparkStageMetricsCollector {

    /** SparkContext.SPARK_JOB_GROUP_ID, which is private[spark]. */
    val JobGroupProperty: String = "spark.jobGroup.id"

    /** Key of the SparkConf entries inside SparkListenerEnvironmentUpdate. */
    val SparkPropertiesKey: String = "Spark Properties"

    /** Prefix of every SparkConf property this feature reads. */
    val ConfigPrefix: String = "spark.wmf.metrics"

    def key(name: String): String = s"$ConfigPrefix.$name"

    /**
     * Identifier of the Airflow task that launched an application, as
     * {DAG_ID}_{TASK_ID}. Set on the application so that it is recorded in the
     * event log, where the batch job reads it back and partitions by it.
     *
     * camelCase, because this is a Spark property and Spark's own are
     * camelCase within a dotted path -- spark.sql.shuffle.partitions,
     * spark.dynamicAllocation.maxExecutors. The table column it ends up in is
     * `airflow_id`, snake_case like every other column there; the two naming
     * conventions belong to different places and are not meant to match.
     */
    val AirflowIdKey: String = key("airflowId")

    /**
     * Prefixes of the Spark properties kept in [[SparkStageMetricsCollector.sparkConfig]].
     *
     * The settings that decide what an application is allowed to cost, which is
     * what makes a run comparable with the one before it. Deliberately short: a
     * property that is not here cannot leak, and one that turns out to be worth
     * having is one line away.
     */
    val ConfigAllowList: Seq[String] = Seq(
        "spark.executor.cores",
        "spark.executor.instances",
        "spark.executor.memory",
        "spark.executor.memoryOverhead",
        "spark.driver.cores",
        "spark.driver.memory",
        "spark.driver.memoryOverhead",
        "spark.dynamicAllocation.",
        "spark.sql.adaptive.",
        "spark.sql.shuffle.partitions",
        ConfigPrefix + "."
    )

    /** Builds a collector for replaying an event log, with no local config. */
    def forReplay(maxTrackedStages: Int = SparkStageMetricsAggregator.DefaultMaxTrackedStages): SparkStageMetricsCollector =
        new SparkStageMetricsCollector(conf = None, maxTrackedStages = maxTrackedStages)
}
