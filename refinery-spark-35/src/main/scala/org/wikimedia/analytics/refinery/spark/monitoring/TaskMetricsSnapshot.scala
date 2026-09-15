package org.wikimedia.analytics.refinery.spark.monitoring

import org.apache.spark.executor.TaskMetrics

/**
 * An immutable copy of the metrics of a single finished task.
 *
 * Spark's own [[TaskMetrics]] is a live, accumulator backed object that cannot
 * be built outside of org.apache.spark, which makes it awkward to aggregate and
 * impossible to construct in tests. Taking a plain snapshot at
 * SparkListenerTaskEnd time keeps [[SparkStageMetricsAggregator]] free of Spark
 * internals.
 *
 * All time fields keep the unit Spark reports them in: the *CpuTime and
 * shuffleWriteTime fields are nanoseconds, every other duration is milliseconds.
 */
case class TaskMetricsSnapshot(
    executorRunTime: Long = 0L,
    executorCpuTime: Long = 0L,
    executorDeserializeTime: Long = 0L,
    executorDeserializeCpuTime: Long = 0L,
    jvmGcTime: Long = 0L,
    resultSize: Long = 0L,
    resultSerializationTime: Long = 0L,
    memoryBytesSpilled: Long = 0L,
    diskBytesSpilled: Long = 0L,
    peakExecutionMemory: Long = 0L,
    inputBytesRead: Long = 0L,
    inputRecordsRead: Long = 0L,
    outputBytesWritten: Long = 0L,
    outputRecordsWritten: Long = 0L,
    shuffleLocalBlocksFetched: Long = 0L,
    shuffleRemoteBlocksFetched: Long = 0L,
    shuffleLocalBytesRead: Long = 0L,
    shuffleRemoteBytesRead: Long = 0L,
    shuffleRemoteBytesReadToDisk: Long = 0L,
    shuffleRecordsRead: Long = 0L,
    shuffleFetchWaitTime: Long = 0L,
    shuffleBytesWritten: Long = 0L,
    shuffleRecordsWritten: Long = 0L,
    shuffleWriteTime: Long = 0L
) {

    /**
     * Adds up two snapshots. peakExecutionMemory is maxed rather than summed:
     * summing per-task peaks would not describe anything real.
     */
    def +(other: TaskMetricsSnapshot): TaskMetricsSnapshot = TaskMetricsSnapshot(
        executorRunTime = executorRunTime + other.executorRunTime,
        executorCpuTime = executorCpuTime + other.executorCpuTime,
        executorDeserializeTime = executorDeserializeTime + other.executorDeserializeTime,
        executorDeserializeCpuTime = executorDeserializeCpuTime + other.executorDeserializeCpuTime,
        jvmGcTime = jvmGcTime + other.jvmGcTime,
        resultSize = resultSize + other.resultSize,
        resultSerializationTime = resultSerializationTime + other.resultSerializationTime,
        memoryBytesSpilled = memoryBytesSpilled + other.memoryBytesSpilled,
        diskBytesSpilled = diskBytesSpilled + other.diskBytesSpilled,
        peakExecutionMemory = math.max(peakExecutionMemory, other.peakExecutionMemory),
        inputBytesRead = inputBytesRead + other.inputBytesRead,
        inputRecordsRead = inputRecordsRead + other.inputRecordsRead,
        outputBytesWritten = outputBytesWritten + other.outputBytesWritten,
        outputRecordsWritten = outputRecordsWritten + other.outputRecordsWritten,
        shuffleLocalBlocksFetched = shuffleLocalBlocksFetched + other.shuffleLocalBlocksFetched,
        shuffleRemoteBlocksFetched = shuffleRemoteBlocksFetched + other.shuffleRemoteBlocksFetched,
        shuffleLocalBytesRead = shuffleLocalBytesRead + other.shuffleLocalBytesRead,
        shuffleRemoteBytesRead = shuffleRemoteBytesRead + other.shuffleRemoteBytesRead,
        shuffleRemoteBytesReadToDisk = shuffleRemoteBytesReadToDisk + other.shuffleRemoteBytesReadToDisk,
        shuffleRecordsRead = shuffleRecordsRead + other.shuffleRecordsRead,
        shuffleFetchWaitTime = shuffleFetchWaitTime + other.shuffleFetchWaitTime,
        shuffleBytesWritten = shuffleBytesWritten + other.shuffleBytesWritten,
        shuffleRecordsWritten = shuffleRecordsWritten + other.shuffleRecordsWritten,
        shuffleWriteTime = shuffleWriteTime + other.shuffleWriteTime
    )
}

object TaskMetricsSnapshot {

    val Empty: TaskMetricsSnapshot = TaskMetricsSnapshot()

    /** Copies the values Spark reports for a finished task. */
    def apply(metrics: TaskMetrics): TaskMetricsSnapshot = TaskMetricsSnapshot(
        executorRunTime = metrics.executorRunTime,
        executorCpuTime = metrics.executorCpuTime,
        executorDeserializeTime = metrics.executorDeserializeTime,
        executorDeserializeCpuTime = metrics.executorDeserializeCpuTime,
        jvmGcTime = metrics.jvmGCTime,
        resultSize = metrics.resultSize,
        resultSerializationTime = metrics.resultSerializationTime,
        memoryBytesSpilled = metrics.memoryBytesSpilled,
        diskBytesSpilled = metrics.diskBytesSpilled,
        peakExecutionMemory = metrics.peakExecutionMemory,
        inputBytesRead = metrics.inputMetrics.bytesRead,
        inputRecordsRead = metrics.inputMetrics.recordsRead,
        outputBytesWritten = metrics.outputMetrics.bytesWritten,
        outputRecordsWritten = metrics.outputMetrics.recordsWritten,
        shuffleLocalBlocksFetched = metrics.shuffleReadMetrics.localBlocksFetched,
        shuffleRemoteBlocksFetched = metrics.shuffleReadMetrics.remoteBlocksFetched,
        shuffleLocalBytesRead = metrics.shuffleReadMetrics.localBytesRead,
        shuffleRemoteBytesRead = metrics.shuffleReadMetrics.remoteBytesRead,
        shuffleRemoteBytesReadToDisk = metrics.shuffleReadMetrics.remoteBytesReadToDisk,
        shuffleRecordsRead = metrics.shuffleReadMetrics.recordsRead,
        shuffleFetchWaitTime = metrics.shuffleReadMetrics.fetchWaitTime,
        shuffleBytesWritten = metrics.shuffleWriteMetrics.bytesWritten,
        shuffleRecordsWritten = metrics.shuffleWriteMetrics.recordsWritten,
        shuffleWriteTime = metrics.shuffleWriteMetrics.writeTime
    )
}

/** How a task finished, as reported by SparkListenerTaskEnd. */
sealed trait TaskOutcome

object TaskOutcome {
    case object Completed extends TaskOutcome
    case object Failed extends TaskOutcome
    case object Killed extends TaskOutcome
}
