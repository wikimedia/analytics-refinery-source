# Spark application metrics

This package works out what a Spark application actually did — how long its
stages took, how much they read, shuffled and spilled, how many of their tasks
failed — so that the runtime behaviour of our Spark jobs is queryable alongside
everything else.

It holds only the aggregation. Its consumer is `SparkEventLogMetrics` in
`refinery-job-35`, a batch job that replays Spark event logs off HDFS and writes
an Iceberg table. Because Spark's own `ReplayListenerBus` posts a log's events to a
listener exactly as the live bus would, that job drives the same code an
in-application `SparkListener` would.

## The pipeline

```
   after the fact: ReplayListenerBus         live: Spark listener bus
   reading an event log off HDFS             (a registered SparkListener)
              \                             /
               \                           /   the same events, either way
                v                         v
            +---------------------------------+
            |  SparkStageMetricsCollector     |  callbacks -> aggregator calls
            |                                 |  every callback wrapped in guard()
            +---------------------------------+
                            |
                            |  StageDescriptor, TaskMetricsSnapshot, TaskOutcome
                            |  (plain immutable values -- no Spark types below here)
                            v
            +---------------------------------+
            |  SparkStageMetricsAggregator    |  one accumulator per stage attempt,
            |                                 |  updated eagerly as tasks end
            +---------------------------------+
                            |
                            |  stageMetrics: Seq[StageMetrics]
                            v
                   SparkStageMetricsRow -> Iceberg table
                   (refinery-job-35, SparkEventLogMetrics)
```

The layers are deliberately independent. The aggregator knows nothing about
where its numbers end up, and the value types between the layers belong to
neither side. That is what makes the whole thing testable without a
`SparkContext` — see `Testing` below.

## One aggregation, however it is driven

The roll-up rules here are easy to get subtly wrong: retried attempts are kept
apart, `peakExecutionMemory` is maxed rather than summed, and the time units are
not uniform. So there is exactly one implementation of them. Anything that wants
these numbers renders `StageMetrics` into its own shape rather than re-deriving
them, which is what stops two descriptions of the same stage drifting apart.

## The files

| File | Role |
| --- | --- |
| `SparkStageMetricsCollector.scala` | Translates Spark callbacks into aggregator calls. Drivable live or from an event log replay. |
| `SparkStageMetricsAggregator.scala` | The state machine. Holds one accumulator per stage attempt. |
| `StageMetrics.scala` | One finished stage attempt as an immutable value. What every consumer renders from. |
| `StageDescriptor.scala` | Immutable copy of Spark's `StageInfo`. |
| `TaskMetricsSnapshot.scala` | Immutable copy of Spark's `TaskMetrics`, plus `TaskOutcome`. |

### SparkStageMetricsCollector

Owns the aggregator and every Spark callback. Each one runs inside `guard()`,
which logs and swallows anything thrown: a listener that throws is *removed from
the listener bus*, which would silently take the metrics of the rest of the
application with it. `VirtualMachineError` is deliberately re-thrown — an
exhausted JVM is not ours to hide.

Two callbacks exist for the replay path in particular:

- `onEnvironmentUpdate` captures the application's Spark properties, which the
  event log always carries. `sparkProperty` reads those first and falls back to
  the local `SparkConf`, so a replay reports the *original* application's
  `spark.master` and `spark.wmf.metrics.airflowId` rather than the replaying
  job's.
- `onOtherEvent` picks up `SparkListenerLogStart`, which carries the Spark
  version and is only ever written to an event log. `sparkVersion` is left empty
  when running live rather than filled in from the running JVM. The version
  worth recording is the one the application *being described* ran on; falling
  back to the version this code happens to be running on would be right when
  live and confidently wrong on a replay, where it is the batch job's own
  version. Not knowing is reported as not knowing.
- `sparkConfig` returns an allow-listed slice of those same properties, so that
  a change in how a job is configured can be read next to the change in what it
  cost. An allow list and not a deny list: the properties map holds whatever the
  submitter put on the command line, credentials included.

### SparkStageMetricsAggregator

Holds mutable state and is **not thread safe** — it relies on being called from a
single thread, which both the Spark listener bus and a replay are.

Task metrics are folded into their stage's accumulator eagerly, as each task
ends, so only one counter set per stage attempt is retained. Memory stays flat
in the number of tasks, and the output is proportional to the number of *stages*.

Four behaviours are worth knowing about before reading the code:

- **The stage is the unit, not the job.** A shuffle, a spill or a skewed
  partition belongs to one stage; rolling it up to the job hides which one.
  Every `StageMetrics` carries its `jobId`, so job-level figures remain
  recoverable by summing.
- **Retries are kept apart.** A stage that fails and is resubmitted produces one
  `StageMetrics` per attempt, distinguished by `attemptId`. The work a failed
  attempt did is real, and folding it into the successful attempt would hide it.
- **A stage can be discovered from a task.** If the collector was attached after
  a stage started, `stateForStage` creates the accumulator on first task end.
  Such a stage may have no known job, in which case its `jobId` is empty rather
  than the stage being dropped.
- **Tracking is bounded.** Past `maxTrackedStages` (default 10000), stage
  attempts are only counted, not measured, and the count is available as
  `untrackedStageCount`. This is what keeps a long-lived driver from growing
  without limit; a non-zero count means the picture is incomplete, and a
  consumer should say so.

### StageDescriptor and TaskMetricsSnapshot

These exist for one reason: Spark's `StageInfo` and `TaskMetrics` are live,
accumulator-backed objects that **cannot be constructed outside of
`org.apache.spark`**. Copying them into plain case classes at callback time lets
the aggregator be driven with ordinary values, and lets tests build any scenario
they like.

Two details that bite:

- `TaskMetricsSnapshot.+` **maxes** `peakExecutionMemory` instead of summing it.
  Summing per-task peaks would not describe anything real.
- Time units follow Spark's own: every `*CpuTime` field and `shuffleWriteTime`
  are **nanoseconds**, every other duration is **milliseconds**. They are carried
  through as Spark reports them rather than normalised, so that figures match
  the Spark UI.

## Spark versions

This module is built against **Spark 3.5.8**, which is why it exists separately
from `refinery-spark`: the rest of the repo is pinned to 3.1.2, and
`refinery-spark-35` overrides `spark.version` for itself the way
`refinery-job-35` already does.

Two different things are version dependent here, and only one of them is a risk.

**What the code is compiled against.** `EventLogReplayer` lives in
`org.apache.spark.scheduler` because `ReplayListenerBus` is `private[spark]`,
which is the same trick the repo already uses in
`org.apache.spark.sql.hive.thriftserver` and
`org.apache.spark.sql.execution.datasources.custom.jdbc`. That bus has had the
same two-argument `replay` signature across the whole 3.x line. If a future
Spark changes it, this module stops compiling — which is the failure mode worth
having, rather than a silent runtime break.

**What the code reads.** The event logs are read by Spark's own `JsonProtocol`,
not by anything here, and Spark reads logs written by older versions: that is
what the History Server does for the ~90 days of logs the cluster keeps. So a
3.5-built job reads 3.1 and 3.3 event logs, and the checked-in fixtures declare
`"Spark Version":"3.1.2"` and `"Spark Version":"3.5.8"` for that reason. A field
a newer Spark added is simply absent from an older log, and the corresponding
metric comes out zero rather than failing.

**Spark 4.x is untested.** Nothing here is known to break on it and nothing has
been run against it. The two things to check before claiming it works are that
`ReplayListenerBus.replay` still takes `(InputStream, String, Boolean)`, and
that `JsonProtocol` still accepts a 3.x log — Spark 4 removed a number of
deprecated event fields, and whether it reads old logs is a question for its
release notes, not for this README.

## Testing

`TestSparkStageMetricsAggregator` covers this package and **starts no
`SparkContext`** — that is the point of the plain value types at the collector
boundary. The aggregator is driven directly with `StageDescriptor` and
`TaskMetricsSnapshot`.

```
./mvnw -pl refinery-spark-35 \
  -DwildcardSuites=org.wikimedia.analytics.refinery.spark.monitoring test
```

In `refinery-job-35`, `TestEventLogReplay` covers the other end, replaying
checked-in event log fixtures into rows, and also needs no `SparkContext`.
`TestSparkEventLogMetrics` covers which logs the job picks up and what it hands
the writer, with the write stubbed out, and `TestSparkEventLogMetricsIceberg`
runs the job against a real Iceberg table so that the append, the dedup and the
reprocess delete are exercised rather than assumed.
