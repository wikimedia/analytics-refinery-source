package org.wikimedia.analytics.refinery.job.monitoring

import java.nio.file.{Files, Path, Paths}

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTimeZone
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.core.PathUtils

/**
 * Runs the job against a real Iceberg table, writing through the path production
 * uses rather than through an injected writer.
 *
 * `TestSparkEventLogMetrics` covers the selection and alignment with the write
 * stubbed out, which keeps those cases cheap. What it cannot cover is the part
 * that only an Iceberg catalog has: that the rows this job builds are accepted by
 * a table declared the way refinery declares it, that the dedup query reads back
 * what the append wrote, and that `--reprocess` removes an application's rows
 * without touching its neighbours'. Those are what this suite is for.
 *
 * The table is created here with the same columns and partitioning as refinery's
 * create_spark_stage_metrics_table.hql. Keeping the two in step by hand is the
 * price of the DDL living in another repository; a column added there and not
 * here fails this suite at the append, which is the point.
 */
class TestSparkEventLogMetricsIceberg
    extends FlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

    private val warehouseDir: String =
        Files.createTempDirectory("spark-stage-metrics-iceberg").toAbsolutePath.toString

    private val table = "local.db.spark_stage_metrics"

    lazy val spark: SparkSession = SparkSession.builder()
        .master("local")
        .appName("TestSparkEventLogMetricsIceberg")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouseDir)
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

    /** As in TestSparkEventLogMetrics: just after the fixtures' application end. */
    private val t0 = new DateTime(2025, 9, 7, 13, 0, 0, DateTimeZone.UTC)

    private val completeLog = "application_1757246400000_0001"
    private val notebookLog = "application_1757246400000_0002"

    private var logDir: Path = _

    override def beforeEach(): Unit = {
        super.beforeEach()
        logDir = Files.createTempDirectory("spark_event_logs")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
        createTable()
    }

    override def afterEach(): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $table PURGE")
        PathUtils.recursiveDelete(logDir)
        super.afterEach()
    }

    /**
     * The table as refinery creates it. No column is NOT NULL, including the
     * partition keys: Spark's encoder marks every non-primitive field of the row
     * nullable, and an Iceberg append of a nullable field into a required column
     * is rejected.
     */
    private def createTable(): Unit = spark.sql(
        s"""CREATE TABLE IF NOT EXISTS $table (
           |    `airflow_id`                        STRING,
           |    `application_id`                    STRING,
           |    `application_attempt_id`            STRING,
           |    `application_start`                 TIMESTAMP,
           |    `application_end`                   TIMESTAMP,
           |    `application_result`                STRING,
           |    `application_name`                  STRING,
           |    `spark_user`                        STRING,
           |    `spark_version`                     STRING,
           |    `spark_master`                      STRING,
           |    `spark_config`                      MAP<STRING, STRING>,
           |    `job_id`                            INT,
           |    `job_group`                         STRING,
           |    `job_result`                        STRING,
           |    `stage_id`                          INT,
           |    `stage_attempt`                     INT,
           |    `stage_name`                        STRING,
           |    `stage_result`                      STRING,
           |    `stage_submitted`                   TIMESTAMP,
           |    `stage_completed`                   TIMESTAMP,
           |    `stage_duration_ms`                 BIGINT,
           |    `num_tasks`                         BIGINT,
           |    `tasks_completed`                   BIGINT,
           |    `tasks_failed`                      BIGINT,
           |    `tasks_killed`                      BIGINT,
           |    `executor_run_time_ms`              BIGINT,
           |    `executor_cpu_time_ns`              BIGINT,
           |    `executor_deserialize_time_ms`      BIGINT,
           |    `executor_deserialize_cpu_time_ns`  BIGINT,
           |    `jvm_gc_time_ms`                    BIGINT,
           |    `result_size_bytes`                 BIGINT,
           |    `result_serialization_time_ms`      BIGINT,
           |    `memory_bytes_spilled`              BIGINT,
           |    `disk_bytes_spilled`                BIGINT,
           |    `peak_execution_memory_bytes`       BIGINT,
           |    `input_bytes_read`                  BIGINT,
           |    `input_records_read`                BIGINT,
           |    `output_bytes_written`              BIGINT,
           |    `output_records_written`            BIGINT,
           |    `shuffle_local_blocks_fetched`      BIGINT,
           |    `shuffle_remote_blocks_fetched`     BIGINT,
           |    `shuffle_local_bytes_read`          BIGINT,
           |    `shuffle_remote_bytes_read`         BIGINT,
           |    `shuffle_remote_bytes_read_to_disk` BIGINT,
           |    `shuffle_records_read`              BIGINT,
           |    `shuffle_fetch_wait_time_ms`        BIGINT,
           |    `shuffle_bytes_written`             BIGINT,
           |    `shuffle_records_written`           BIGINT,
           |    `shuffle_write_time_ns`             BIGINT
           |)
           |USING ICEBERG
           |PARTITIONED BY (airflow_id, months(application_start))
           |TBLPROPERTIES (
           |    'format-version' = '2',
           |    'write.delete.mode' = 'copy-on-write',
           |    'write.parquet.compression-codec' = 'zstd'
           |)""".stripMargin
    )

    private def putLog(fixture: String, as: String, modifiedAt: DateTime): Unit = {
        val source = Paths.get(getClass.getResource(s"/spark_event_logs/$fixture").toURI)
        Files.copy(source, logDir.resolve(as))
        logDir.resolve(as).toFile.setLastModified(modifiedAt.getMillis) should equal(true)
    }

    private def config(reprocess: Boolean = false): SparkEventLogMetrics.Config =
        SparkEventLogMetrics.Config(
            event_log_dir = logDir.toUri.toString,
            table = table,
            since = t0.minusHours(1),
            until = t0.plusHours(1),
            reprocess = reprocess,
            require_airflow_id = false
        )

    private def applicationIdsInTable: Seq[String] =
        spark.sql(s"SELECT DISTINCT application_id FROM $table ORDER BY application_id")
            .collect().map(_.getString(0)).toSeq

    "The job" should "append its rows to the Iceberg table" in {
        putLog(completeLog, completeLog, t0)
        putLog(notebookLog, notebookLog, t0)

        val written = SparkEventLogMetrics(spark, config())

        written should equal(3L)
        spark.table(table).count() should equal(3L)
        applicationIdsInTable should equal(Seq(
            "application_1757246400000_0001",
            "application_1757246400000_0002"
        ))
    }

    it should "write the values the event log carried" in {
        putLog(completeLog, completeLog, t0)
        SparkEventLogMetrics(spark, config())

        val row = spark
            .sql(s"SELECT * FROM $table WHERE application_id = 'application_1757246400000_0001' AND stage_id = 0")
            .head()

        row.getAs[String]("airflow_id") should equal("refine_event_hourly")
        row.getAs[String]("application_result") should equal("succeeded")
        row.getAs[String]("stage_name") should equal("load at Refine.scala:10")
        row.getAs[Long]("input_bytes_read") should equal(1073741824L + 2147483648L)
        // The map survives the round trip through Iceberg, filtered as the
        // collector filtered it.
        val sparkConfig = row.getAs[Map[String, String]]("spark_config")
        sparkConfig("spark.executor.memory") should equal("8g")
        sparkConfig.keys should not contain "spark.hadoop.fs.s3a.secret.key"
    }

    /**
     * The whole idempotence story. A completed event log never changes, so an
     * application already in the table would yield identical rows; rerunning a
     * window has to be free rather than doubling it.
     */
    it should "write nothing on a second run over the same window" in {
        putLog(completeLog, completeLog, t0)
        SparkEventLogMetrics(spark, config()) should equal(2L)

        SparkEventLogMetrics(spark, config()) should equal(0L)

        spark.table(table).count() should equal(2L)
    }

    it should "rewrite only the application it reprocessed" in {
        putLog(completeLog, completeLog, t0)
        putLog(notebookLog, notebookLog, t0)
        SparkEventLogMetrics(spark, config())
        spark.table(table).count() should equal(3L)

        // Leave only the one log in place, so reprocess picks up that application
        // alone and the other application's rows have to survive untouched.
        Files.delete(logDir.resolve(notebookLog))
        val written = SparkEventLogMetrics(spark, config(reprocess = true))

        written should equal(2L)
        spark.table(table).count() should equal(3L)
        applicationIdsInTable should equal(Seq(
            "application_1757246400000_0001",
            "application_1757246400000_0002"
        ))
    }

    it should "refuse to run before the table has been created" in {
        spark.sql(s"DROP TABLE IF EXISTS $table PURGE")
        putLog(completeLog, completeLog, t0)

        val thrown = the [IllegalArgumentException] thrownBy SparkEventLogMetrics(spark, config())

        thrown.getMessage should include(table)
    }
}
