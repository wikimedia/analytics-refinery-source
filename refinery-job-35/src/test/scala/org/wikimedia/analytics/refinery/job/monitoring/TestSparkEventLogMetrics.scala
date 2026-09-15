package org.wikimedia.analytics.refinery.job.monitoring

import java.nio.file.{Files, Path, Paths}

import scala.collection.JavaConverters._

import com.github.nscala_time.time.Imports._
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.joda.time.DateTimeZone
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.core.PathUtils

/**
 * Covers the job around the replay: which event logs it picks up, which it skips
 * because their application is already in the table, and what it hands to the
 * writer.
 *
 * The job's writer is injected here, so these cases cost no Iceberg catalog and
 * the output table is a plain Parquet one. That is deliberate: it lets the
 * selection, ordering and alignment cases stay cheap, and it lets the table be
 * built with its columns in the wrong order to prove the alignment works.
 *
 * `TestSparkEventLogMetricsIceberg` runs the same job against a real Iceberg
 * table, and `TestEventLogReplay` covers the event-log-to-rows path itself.
 */
class TestSparkEventLogMetrics
    extends FlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

    private val table = "db.spark_stage_metrics"

    lazy val spark: SparkSession = SparkSession.builder()
        .master("local")
        .appName("TestSparkEventLogMetrics")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()

    /**
     * Clearing the active and default session matters as much as stopping it.
     * Every Spark suite in this module builds its own session with `getOrCreate`
     * in one JVM, and a session left behind here is handed to the next suite
     * whatever it asked for -- so a suite that needs the Iceberg extensions could
     * silently get one without them.
     */
    override def afterAll(): Unit = {
        spark.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
    }

    /**
     * A fixed point in time, so the modification-time windows are not flaky.
     * Deliberately just after the fixtures' application end (2025-09-07T12:00:06Z):
     * the dedup lookback counts back from `since`, so a `t0` far in the future
     * would put the horizon after the rows and never match them.
     */
    private val t0 = new DateTime(2025, 9, 7, 13, 0, 0, DateTimeZone.UTC)

    private val completeLog = "application_1757246400000_0001"
    private val notebookLog = "application_1757246400000_0002"
    private val rollingLog = "eventlog_v2_application_1757246400000_0005"

    private var logDir: Path = _

    override def beforeEach(): Unit = {
        super.beforeEach()
        logDir = Files.createTempDirectory("spark_event_logs")
        spark.sql("CREATE DATABASE IF NOT EXISTS db")
    }

    override def afterEach(): Unit = {
        spark.sql("DROP DATABASE IF EXISTS db CASCADE")
        PathUtils.recursiveDelete(logDir)
        super.afterEach()
    }

    /**
     * Copies a checked-in fixture into the log directory with a chosen mtime.
     *
     * A rolling event log fixture is a directory of parts, so the copy has to
     * handle both shapes. Only the top level gets the modification time, which is
     * what the job selects on.
     */
    private def putLog(fixture: String, as: String, modifiedAt: DateTime): Unit = {
        val source = Paths.get(getClass.getResource(s"/spark_event_logs/$fixture").toURI)
        val target = logDir.resolve(as)
        if (Files.isDirectory(source)) {
            Files.createDirectory(target)
            val parts = Files.list(source)
            try parts.iterator().asScala.foreach(part => Files.copy(part, target.resolve(part.getFileName)))
            finally parts.close()
        } else {
            Files.copy(source, target)
        }
        target.toFile.setLastModified(modifiedAt.getMillis) should equal(true)
    }

    /**
     * `require_airflow_id` is set explicitly rather than left to the production
     * default, so that these tests keep saying what they mean if that default
     * changes. The tests that care about it set it themselves.
     */
    private def config(
        since: DateTime = t0.minusHours(1),
        until: DateTime = t0.plusHours(1),
        maxApplications: Int = 0,
        dryRun: Boolean = false,
        requireAirflowId: Boolean = false
    ): SparkEventLogMetrics.Config = SparkEventLogMetrics.Config(
        event_log_dir = logDir.toUri.toString,
        table = table,
        since = since,
        until = until,
        max_applications = maxApplications,
        dry_run = dryRun,
        require_airflow_id = requireAirflowId
    )

    /**
     * Creates the output table with its columns in a different order from the
     * case class, so that the alignment is doing something observable rather
     * than the two orders happening to agree.
     */
    private def createTable(): Seq[String] = {
        import spark.implicits._
        val rowSchema = spark.emptyDataset[SparkStageMetricsRow].schema
        val tableSchema = StructType(rowSchema.fields.reverse)
        spark
            .createDataFrame(spark.sparkContext.emptyRDD[Row], tableSchema)
            .write
            .mode(SaveMode.Overwrite)
            .format("parquet")
            .saveAsTable(table)
        tableSchema.fieldNames.toSeq
    }

    /**
     * Runs the job, capturing what it would have written instead of writing.
     *
     * The writer consumes the DataFrame, as the real one does. That is part of
     * the contract rather than an incidental detail: the job counts rows with an
     * accumulator filled while they stream into the write, so a writer that only
     * stored the DataFrame would leave the job reporting zero rows written.
     */
    private def runCapturing(cfg: SparkEventLogMetrics.Config): (Long, Option[DataFrame]) = {
        var captured: Option[DataFrame] = None
        val count = SparkEventLogMetrics.apply(spark, cfg, { (_, _, df) =>
            df.count()
            captured = Some(df)
        })
        (count, captured)
    }

    "listCandidates" should "take the event logs modified inside the window, oldest first" in {
        putLog(completeLog, "application_0001", t0)
        putLog(notebookLog, "application_0002", t0.minusMinutes(30))
        putLog(completeLog, "application_0003", t0.minusDays(2))
        putLog(completeLog, "application_0004", t0.plusDays(2))

        val candidates = SparkEventLogMetrics.listCandidates(spark, config()).map(c => new HadoopPath(c.path).getName)

        candidates should equal(Seq("application_0002", "application_0001"))
    }

    it should "ignore in-progress, hidden and marker files" in {
        putLog(completeLog, "application_0001", t0)
        putLog(completeLog, "application_0002.inprogress", t0)
        putLog(completeLog, ".hidden", t0)
        putLog(completeLog, "_SUCCESS", t0)

        val candidates = SparkEventLogMetrics.listCandidates(spark, config()).map(c => new HadoopPath(c.path).getName)

        candidates should equal(Seq("application_0001"))
    }

    it should "take the oldest logs first when capped" in {
        putLog(completeLog, "application_0001", t0)
        putLog(completeLog, "application_0002", t0.minusMinutes(30))
        putLog(completeLog, "application_0003", t0.minusMinutes(50))

        val candidates = SparkEventLogMetrics
            .listCandidates(spark, config(maxApplications = 2))
            .map(c => new HadoopPath(c.path).getName)

        candidates should equal(Seq("application_0003", "application_0002"))
    }

    it should "report how much there is to replay for each candidate" in {
        putLog(completeLog, "application_0001", t0)
        putLog(notebookLog, "application_0002", t0)

        val bySize = SparkEventLogMetrics.listCandidates(spark, config())
            .map(c => new HadoopPath(c.path).getName -> c.sizeBytes).toMap

        // The two fixtures differ in size, and both must be measured, not zero.
        all(bySize.values.toSeq.map(_ > 0L)) should equal(true)
        bySize("application_0001") should be > bySize("application_0002")
    }

    "replayOrder" should "put the biggest event log first" in {
        val candidates = Seq(
            SparkEventLogMetrics.EventLogCandidate("small", 10L),
            SparkEventLogMetrics.EventLogCandidate("huge", 9000L),
            SparkEventLogMetrics.EventLogCandidate("medium", 500L)
        )

        // A stage costs its worst task, so the slowest log must be dispatched
        // first rather than be picked up last and added to the tail.
        SparkEventLogMetrics.replayOrder(candidates) should equal(Seq("huge", "medium", "small"))
    }

    it should "keep every candidate" in {
        val candidates = (1 to 5).map(i => SparkEventLogMetrics.EventLogCandidate(s"log_$i", i.toLong))

        SparkEventLogMetrics.replayOrder(candidates) should contain theSameElementsAs candidates.map(_.path)
    }

    "apply" should "do nothing when no event log falls in the window" in {
        putLog(completeLog, "application_0001", t0.minusDays(5))
        createTable()

        val (count, captured) = runCapturing(config())

        count should equal(0L)
        captured shouldBe empty
    }

    it should "write one row per stage attempt of every log it read" in {
        putLog(completeLog, completeLog, t0)
        putLog(notebookLog, notebookLog, t0)
        createTable()

        val (count, captured) = runCapturing(config())

        // Two stages in the first fixture, one in the second.
        count should equal(3L)
        captured shouldBe defined
        captured.get.count() should equal(3L)
    }

    it should "hand the writer rows in the table's column order" in {
        putLog(completeLog, completeLog, t0)
        val tableColumns = createTable()

        val (_, captured) = runCapturing(config())

        // The case class order reversed, so this only passes if the job aligned.
        captured.get.columns.toSeq should equal(tableColumns)
    }

    it should "carry the values read from the event log through to the writer" in {
        putLog(completeLog, completeLog, t0)
        createTable()

        val (_, captured) = runCapturing(config())
        val stage0 = captured.get.where(col("stage_id") === 0).head()

        stage0.getAs[String]("application_id") should equal("application_1757246400000_0001")
        stage0.getAs[String]("airflow_id") should equal("refine_event_hourly")
        stage0.getAs[String]("stage_name") should equal("load at Refine.scala:10")
        stage0.getAs[Long]("num_tasks") should equal(2L)
        stage0.getAs[Long]("input_bytes_read") should equal(1073741824L + 2147483648L)
        stage0.getAs[Long]("stage_duration_ms") should equal(2000L)
    }

    it should "write nothing in a dry run, but still report what it would have written" in {
        putLog(completeLog, completeLog, t0)
        createTable()

        val (count, captured) = runCapturing(config(dryRun = true))

        count should equal(2L)
        captured shouldBe empty
        spark.table(table).count() should equal(0L)
    }

    it should "skip an application whose rows are already in the table" in {
        putLog(completeLog, completeLog, t0)
        putLog(notebookLog, notebookLog, t0)
        val tableColumns = createTable()

        // First run: everything is new. Put what it produced into the table, so
        // the second run meets rows written exactly as the job writes them.
        val (firstCount, firstRows) = runCapturing(config())
        firstCount should equal(3L)
        firstRows.get.write.mode(SaveMode.Append).insertInto(table)

        val (secondCount, secondCaptured) = runCapturing(config())

        secondCount should equal(0L)
        secondCaptured shouldBe empty
        tableColumns should not be empty
    }

    it should "still read an application the table does not know about" in {
        putLog(completeLog, completeLog, t0)
        createTable()

        val (_, firstRows) = runCapturing(config())
        firstRows.get.write.mode(SaveMode.Append).insertInto(table)

        // A second, different application appears in the same window.
        putLog(notebookLog, notebookLog, t0)
        val (count, captured) = runCapturing(config())

        count should equal(1L)
        captured.get.head().getAs[String]("application_id") should
            equal("application_1757246400000_0002")
    }

    it should "read every candidate when the table does not exist yet" in {
        putLog(completeLog, completeLog, t0)

        val (count, captured) = runCapturing(config(dryRun = true))

        count should equal(2L)
        captured shouldBe empty
    }

    /**
     * The default, so that nothing lands in the null airflow_id partition unless
     * it was asked for. The notebook fixture deliberately carries no airflow id.
     */
    it should "leave out applications with no airflow id when one is required" in {
        putLog(completeLog, completeLog, t0)
        putLog(notebookLog, notebookLog, t0)
        createTable()

        val (count, captured) = runCapturing(config(requireAirflowId = true))

        count should equal(2L)
        captured.get.select("application_id").distinct().collect().map(_.getString(0)).toSeq should
            equal(Seq("application_1757246400000_0001"))
    }

    it should "use the production default of requiring an airflow id" in {
        SparkEventLogMetrics.Config().require_airflow_id should equal(true)
    }

    /**
     * A rolling event log is a directory of numbered parts that Spark replays in
     * order into one listener, so it has to come out as the same rows a
     * single-file log of the same application would.
     */
    "a rolling event log" should "be read as one application" in {
        putLog(rollingLog, rollingLog, t0)
        createTable()

        val (count, captured) = runCapturing(config())

        count should equal(2L)
        captured.get.select("application_id").distinct().collect().map(_.getString(0)).toSeq should
            equal(Seq("application_1757246400000_0005"))
    }

    /**
     * The directory is named eventlog_v2_<applicationId>, not <applicationId>.
     * Matching the raw name against the ids in the table never hits, and the same
     * rolling log would be read and appended on every run.
     */
    it should "be skipped once its application is in the table" in {
        putLog(rollingLog, rollingLog, t0)
        createTable()

        val (firstCount, firstRows) = runCapturing(config())
        firstCount should equal(2L)
        firstRows.get.write.mode(SaveMode.Append).insertInto(table)

        val (secondCount, secondCaptured) = runCapturing(config())

        secondCount should equal(0L)
        secondCaptured shouldBe empty
    }

    it should "be reported, not thrown, when the directory is not one" in {
        // A directory that is not named eventlog_v2_*, which is how Spark decides.
        Files.createDirectory(logDir.resolve("some_other_directory"))
        logDir.resolve("some_other_directory").toFile.setLastModified(t0.getMillis) should equal(true)
        createTable()

        val (count, captured) = runCapturing(config())

        // It is a candidate -- nothing about the name rules a directory out -- so
        // the job gets as far as the write, and writes nothing.
        count should equal(0L)
        captured.get.count() should equal(0L)
    }

    "apply" should "refuse to run against a table that does not exist" in {
        putLog(completeLog, completeLog, t0)

        val thrown = the [IllegalArgumentException] thrownBy runCapturing(config())

        thrown.getMessage should include(table)
        thrown.getMessage should include("create_spark_stage_metrics_table.hql")
    }
}
