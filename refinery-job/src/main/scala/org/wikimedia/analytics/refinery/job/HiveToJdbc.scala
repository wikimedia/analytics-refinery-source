package org.wikimedia.analytics.refinery.job

import scala.collection.immutable.ListMap

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.wikimedia.analytics.refinery.tools.LogHelper
import org.wikimedia.analytics.refinery.tools.config._

/**
 * Mirrors a single Hive/Iceberg table into a JDBC database.
 *
 * The `<database>.<table>` given as parameters is read from the Spark catalog and
 * written over JDBC to a target table of the same (unqualified) name. Default write
 * mode is `overwrite` with `truncate=true`: the target table is TRUNCATEd and reloaded,
 * so the table object, its grants and any Superset dataset binding are preserved across
 * runs. If the target table does not exist, Spark's JDBC writer creates it from the
 * source schema on the first run.
 *
 */
object HiveToJdbc extends LogHelper with ConfigHelper {

    case class Config(
        database    : String = "",
        table            : String = "",
        jdbc_url         : String = "",
        jdbc_driver      : String = "org.postgresql.Driver",
        db_user          : String = "",
        password_file    : String = "",
        target_schema : String = "", // defaults to the source database (see targetSchema)
        output_mode      : String = "overwrite",
        date_field       : String = "",
        date_value       : String = "",
        batch_size       : Int    = 10000
    )

    object Config {
        val defaults = Config()

        val usage =
            """|Mirror a single Hive/Iceberg table into a JDBC database.
               |
               |The <database>.<table> given as parameters is written to the JDBC table
               |<target_schema>.<table>, where target_schema defaults to the source
               |schema so the target mirrors the source. The schema must already exist.
               |
               |Example for PostgreSQL:
               |The PostgreSQL JDBC driver is NOT bundled in refinery-job, so pass it on the
               |classpath with --jars. In client mode the driver JVM also needs it on
               |spark.driver.extraClassPath (that is where the write happens).
               |
               |--password_file
               |is read through the Hadoop FileSystem, so a bare path resolves to HDFS; use a
               |file:// URI for a local file.
               |
               |Example:
               |
               |  spark3-submit --class org.wikimedia.analytics.refinery.job.HiveToJdbc \
               |   --jars artifacts/postgresql-42.7.5.jar \
               |   --conf spark.driver.extraClassPath=artifacts/postgresql-42.7.5.jar \
               |   artifacts/refinery-job-0.3.25-SNAPSHOT-shaded.jar \
               |   --database    database \
               |   --table            source_table_to_clone \
               |   --jdbc_url         "jdbc:postgresql://postgresql-superset-metrics.discovery.wmnet:31432/superset_metrics?sslmode=verify-full&sslNegotiation=direct&sslrootcert=/etc/ssl/certs/ca-certificates.crt" \
               |   --db_user          superset_metrics \
               |   --password_file    file://$PWD/pg.txt \
               |   --output_mode      overwrite
               |
               |target_schema defaults to --database (here database);
               |pass --target_schema to override it.
               |"""

        val propertiesDoc = ListMap(
            "config_file" ->
                """Config properties file. Properties specified in the command line
                  |override those specified in the config file.""",
            "database" ->
                "Input Hive database (schema) name. Mandatory.",
            "table" ->
                "Input Hive table name; also the target JDBC table name. Mandatory.",
            "jdbc_url" ->
                "JDBC URL of the target database. Mandatory.",
            "jdbc_driver" ->
                s"""JDBC driver class to use. Its jar must be on the classpath at runtime
                   |(pass it via --jars). Default: ${defaults.jdbc_driver}.""",
            "db_user" ->
                "JDBC user. Mandatory.",
            "password_file" ->
                "HDFS path to a file containing the JDBC password. Mandatory.",
            "target_schema" ->
                """JDBC schema the target table is written to (it must already exist).
                  |Default: the source schema given in --database.""",
            "output_mode" ->
                s"""Write mode (overwrite|append). overwrite => TRUNCATE + reload, which
                   |preserves the target table object, its grants and any Superset dataset
                   |binding. Default: ${defaults.output_mode}.""",
            "date_field" ->
                """Name of the date column to filter the source on when output_mode is not
                  |overwrite. Mandatory unless output_mode=overwrite.""",
            "date_value" ->
                """Value (string) the date_field must equal when output_mode is not
                  |overwrite. Mandatory unless output_mode=overwrite.""",
            "batch_size" ->
                s"JDBC batch size. Default: ${defaults.batch_size}."
        )
    }

    def main(args: Array[String]): Unit = {
        if (args.contains("--help")) {
            println(help(Config.usage, Config.propertiesDoc))
            sys.exit(0)
        }

        val config = loadConfig(args)
        val spark = SparkSession.builder().enableHiveSupport().appName("HiveToJdbc").getOrCreate()
        // Capture deploy mode before stopping the session, to decide on the exit code.
        val exitOnFinish = spark.conf.get("spark.master") != "yarn" ||
            spark.conf.get("spark.submit.deployMode") == "client"

        val success = try {
            val sourceTable = s"${config.database}.${config.table}"
            val password = readHdfsFile(spark, config.password_file).trim
            log.info(s"Mirroring $sourceTable -> ${config.table} (mode=${config.output_mode}) into ${config.jdbc_url}")

            val (options, saveMode) = jdbcOptions(config, password)
            selectSource(spark.table(sourceTable), config).write
                .format("jdbc")
                .options(options)
                .mode(saveMode)
                .save()

            log.info(s"Finished mirroring $sourceTable -> ${config.table}.")
            true
        } catch {
            case e: Exception =>
                log.error("Failed mirroring Hive table to JDBC.", e)
                false
        } finally {
            spark.stop()
        }

        // Exit with proper code only if not running in YARN or deploy mode is client.
        if (exitOnFinish) sys.exit(if (success) 0 else 1)
    }

    def loadConfig(args: Array[String]): Config = {
        val config = try {
            configureArgs[Config](args)
        } catch {
            case e: ConfigHelperException =>
                log.fatal(e.getMessage + ". Aborting.")
                sys.exit(1)
        }
        validate(config)
        log.info("Loaded configuration:\n" + prettyPrint(config))
        config
    }

    /**
     * Validate a loaded config, throwing IllegalArgumentException on the first problem.
     * ConfigHelper leaves case-class defaults in place, so we check the mandatory opts
     * (and the output_mode enum) ourselves rather than silently mirroring nothing.
     */
    def validate(config: Config): Unit = {
        require(config.database.nonEmpty, "--database is required")
        require(config.table.nonEmpty, "--table is required")
        require(config.jdbc_url.nonEmpty, "--jdbc_url is required")
        require(config.db_user.nonEmpty, "--db_user is required")
        require(config.password_file.nonEmpty, "--password_file is required")
        require(
            Set("overwrite", "append").contains(config.output_mode),
            s"--output_mode must be 'overwrite' or 'append', got '${config.output_mode}'"
        )
        // The date filter is only applied (and thus only required) when not overwriting.
        if (config.output_mode != "overwrite") {
            require(config.date_field.nonEmpty, "--date_field is required unless output_mode=overwrite")
            require(config.date_value.nonEmpty, "--date_value is required unless output_mode=overwrite")
        }
    }

    /**
     * JDBC schema the target table is written to: the explicit --target_schema
     * if given, otherwise the source schema (so the target mirrors the source).
     */
    def targetSchema(config: Config): String =
        if (config.target_schema.nonEmpty) config.target_schema else config.database

    /**
     * Resolve the JDBC writer options + SaveMode for a config. Pure (no Spark/DB) so it
     * is unit-testable. On overwrite we set truncate=true so the write TRUNCATEs instead
     * of DROP+CREATE, preserving the target table object, its grants and Superset's
     * dataset binding across runs.
     */
    def jdbcOptions(config: Config, password: String): (Map[String, String], SaveMode) = {
        val saveMode = if (config.output_mode == "append") SaveMode.Append else SaveMode.Overwrite
        var options = Map(
            "url"        -> config.jdbc_url,
            "dbtable"    -> s"${targetSchema(config)}.${config.table}",
            "user"       -> config.db_user,
            "password"   -> password,
            "driver"     -> config.jdbc_driver,
            "batchsize"  -> config.batch_size.toString,
            // Let Postgres coerce e.g. Spark strings into text/uuid as needed.
            "stringtype" -> "unspecified"
        )
        if (saveMode == SaveMode.Overwrite) options += ("truncate" -> "true")
        (options, saveMode)
    }

    /**
     * On overwrite we mirror the whole table; otherwise (append) we only select the rows
     * where the provided date_field equals the provided date_value (string comparison).
     */
    def selectSource(sourceDf: DataFrame, config: Config): DataFrame = {
        if (config.output_mode == "overwrite") sourceDf
        else sourceDf.where(col(config.date_field) === config.date_value)
    }

    /** Read a UTF-8 text file from the (HDFS) filesystem the SparkContext is configured for. */
    def readHdfsFile(spark: SparkSession, path: String): String = {
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val stream = fs.open(new Path(path))
        try scala.io.Source.fromInputStream(stream).mkString
        finally stream.close()
    }
}
