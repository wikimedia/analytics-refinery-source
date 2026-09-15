package org.wikimedia.analytics.refinery.job.monitoring

import java.io.File
import java.nio.file.Paths

import org.scalatest.{FlatSpec, Matchers}

/**
 * Checks that a real Spark event log replays into the rows we expect.
 *
 * This needs no SparkSession: the replay is Spark's own ReplayListenerBus
 * deserialising a file and posting events to a listener, so the whole
 * events-to-rows path is covered by an ordinary unit test. It goes in through
 * `replayFile` rather than `replay`, because opening a Hadoop FileSystem
 * resolves the OS user and that is not always possible where tests run; only
 * the file discovery differs between the two.
 *
 * The fixtures are hand-written logs rather than captured ones, so that every
 * number in them is chosen and can be asserted exactly.
 */
class TestEventLogReplay extends FlatSpec with Matchers {

    private def fixture(name: String): File =
        Paths.get(getClass.getResource(s"/spark_event_logs/$name").toURI).toFile

    private def replayFixture(name: String, requireAirflowId: Boolean = false): EventLogReplay.Replayed =
        EventLogReplay.replayFile(fixture(name), requireAirflowId)

    private val completeLog = "application_1757246400000_0001"

    "EventLogReplay" should "produce one row per stage attempt" in {
        val replayed = replayFixture(completeLog)

        withClue(s"skipped: ${replayed.skipReason}: ") { replayed.skipReason shouldBe empty }
        replayed.rows should have size 2
        replayed.rows.map(_.stage_id) should contain theSameElementsAs Seq(0, 1)
    }

    it should "carry the application identity from the log, not from the replaying job" in {
        val row = replayFixture(completeLog).rows.head

        row.application_id should equal("application_1757246400000_0001")
        row.application_name should equal(Some("test_refine"))
        row.application_attempt_id should equal(Some("1"))
        row.spark_user should equal(Some("analytics"))
        // Both of these would be the batch job's own values if they were read
        // from the local configuration instead of from the log.
        row.spark_master should equal(Some("yarn"))
        row.spark_version should equal(Some("3.1.2"))
        row.application_start.getTime should equal(1757246400000L)
        row.application_end.map(_.getTime) should equal(Some(1757246406000L))
    }

    it should "read the airflow id out of the logged Spark properties" in {
        replayFixture(completeLog).rows.map(_.airflow_id).distinct should equal(Seq(Some("refine_event_hourly")))
    }

    it should "carry the allow-listed settings the application ran with" in {
        val config = replayFixture(completeLog).rows.head.spark_config

        config should equal(Map(
            "spark.executor.memory" -> "8g",
            "spark.executor.cores" -> "4",
            "spark.dynamicAllocation.maxExecutors" -> "64",
            "spark.sql.shuffle.partitions" -> "256",
            "spark.wmf.metrics.airflowId" -> "refine_event_hourly"
        ))
    }

    /**
     * The whole reason the allow list is a list of what to keep rather than a
     * list of what to drop. The fixture carries a property shaped like a
     * credential; a deny list would have had to know about that one in advance.
     */
    it should "leave out a property that is not on the allow list" in {
        val config = replayFixture(completeLog).rows.head.spark_config

        config.keys should not contain "spark.hadoop.fs.s3a.secret.key"
        config.values.toSeq should not contain "not-a-real-secret"
    }

    it should "call an application that ended with no failed job a success" in {
        replayFixture(completeLog).rows.map(_.application_result).distinct should equal(Seq("succeeded"))
    }

    it should "call an application with a failed job a failure" in {
        val replayed = replayFixture("application_1757246400000_0004")

        withClue(s"skipped: ${replayed.skipReason}: ") { replayed.skipReason shouldBe empty }
        replayed.rows should have size 1
        val row = replayed.rows.head
        row.application_result should equal("failed")
        // The application did end; it is the job inside it that failed.
        row.application_end should be (defined)
        row.job_result should equal(Some("failed"))
        row.stage_result should equal("failed")
        row.tasks_failed should equal(1L)
    }

    it should "roll a stage's tasks up into its row" in {
        val stage0 = replayFixture(completeLog).rows.find(_.stage_id == 0).get

        stage0.stage_name should equal(Some("load at Refine.scala:10"))
        stage0.stage_result should equal("succeeded")
        stage0.num_tasks should equal(2L)
        stage0.tasks_completed should equal(2L)
        stage0.tasks_failed should equal(0L)
        // Summed across the stage's two tasks.
        stage0.executor_run_time_ms should equal(300L + 400L)
        stage0.executor_cpu_time_ns should equal(250000000L + 350000000L)
        stage0.input_bytes_read should equal(1073741824L + 2147483648L)
        stage0.input_records_read should equal(3000L)
        stage0.shuffle_bytes_written should equal(4096L + 8192L)
        // Peak execution memory is the largest one task reported, not the sum.
        stage0.peak_execution_memory_bytes should equal(2097152L)
        stage0.stage_duration_ms should equal(Some(2000L))
    }

    it should "count a killed task as killed rather than failed" in {
        val stage1 = replayFixture(completeLog).rows.find(_.stage_id == 1).get

        stage1.tasks_completed should equal(1L)
        stage1.tasks_killed should equal(1L)
        stage1.tasks_failed should equal(0L)
        stage1.shuffle_records_read should equal(300L)
        stage1.output_records_written should equal(300L)
    }

    it should "attribute every stage to the job that submitted it" in {
        val rows = replayFixture(completeLog).rows

        all(rows.map(_.job_id)) should equal(Some(0))
        all(rows.map(_.job_result)) should equal(Some("succeeded"))
        all(rows.map(_.job_group)) should equal(Some("refine-group"))
    }

    it should "report rather than throw when the file is not an event log" in {
        val replayed = replayFixture("not_an_event_log")

        replayed.rows shouldBe empty
        replayed.skipReason shouldBe defined
    }

    it should "skip an application with no airflow id when one is required" in {
        val replayed = replayFixture("application_1757246400000_0002", requireAirflowId = true)

        replayed.rows shouldBe empty
        replayed.skipReason.get should include("airflowId")
    }

    it should "still write that application when no airflow id is required" in {
        val replayed = replayFixture("application_1757246400000_0002")

        replayed.skipReason shouldBe empty
        replayed.rows should have size 1
        replayed.rows.head.airflow_id shouldBe empty
    }

    it should "read what an application killed mid-flight managed to report" in {
        val replayed = replayFixture("application_1757246400000_0003")

        withClue(s"skipped: ${replayed.skipReason}: ") { replayed.skipReason shouldBe empty }
        // The log stops after the stage completed but before the job or the
        // application ended, and its last line is a truncated fragment.
        replayed.rows should have size 1
        replayed.rows.head.stage_result should equal("succeeded")
        replayed.rows.head.application_end shouldBe empty
        // The job was still open when the driver died, so it is reported as
        // such rather than guessed either way.
        replayed.rows.head.job_result should equal(Some("incomplete"))
        // No end event means the application cannot be called either way.
        replayed.rows.head.application_result should equal("incomplete")
    }

    /**
     * The notebook fixture has no spark.wmf.* and no executor settings, so the
     * allow list matches nothing in it. Empty rather than null: there is a
     * difference between an application that set none of these and one whose
     * environment we never saw, and the latter produces no rows at all.
     */
    it should "give an empty config map for an application that set none of the allow-listed properties" in {
        replayFixture("application_1757246400000_0002").rows.head.spark_config shouldBe empty
    }
}
