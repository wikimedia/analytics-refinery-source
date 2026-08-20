package org.wikimedia.analytics.refinery.job

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SaveMode
import org.scalatest.{FlatSpec, Matchers}

class TestHiveToJdbc extends FlatSpec with Matchers with DataFrameSuiteBase {

    /** A fully-populated, valid overwrite config; individual tests tweak copies of it. */
    private val overwriteConfig = HiveToJdbc.Config(
        database = "test_schema",
        table = "test_table",
        jdbc_url = "jdbc:postgresql://test-host:5432/test_db",
        db_user = "test_user",
        password_file = "/path/to/test_pw.txt"
    )

    private val appendConfig = overwriteConfig.copy(
        output_mode = "append",
        date_field = "dt",
        date_value = "2020-01-01"
    )

    // ---------------------------------------------------------------- validate

    "validate" should "accept a fully populated overwrite config" in {
        noException should be thrownBy HiveToJdbc.validate(overwriteConfig)
    }

    it should "accept an append config with date_field and date_value" in {
        noException should be thrownBy HiveToJdbc.validate(appendConfig)
    }

    it should "not require date_field/date_value on overwrite" in {
        // overwriteConfig leaves date_field/date_value empty on purpose.
        overwriteConfig.date_field shouldBe ""
        noException should be thrownBy HiveToJdbc.validate(overwriteConfig)
    }

    it should "reject a missing database" in {
        an[IllegalArgumentException] should be thrownBy
            HiveToJdbc.validate(overwriteConfig.copy(database = ""))
    }

    it should "reject a missing table" in {
        an[IllegalArgumentException] should be thrownBy
            HiveToJdbc.validate(overwriteConfig.copy(table = ""))
    }

    it should "reject a missing jdbc_url" in {
        an[IllegalArgumentException] should be thrownBy
            HiveToJdbc.validate(overwriteConfig.copy(jdbc_url = ""))
    }

    it should "reject a missing db_user" in {
        an[IllegalArgumentException] should be thrownBy
            HiveToJdbc.validate(overwriteConfig.copy(db_user = ""))
    }

    it should "reject a missing password_file" in {
        an[IllegalArgumentException] should be thrownBy
            HiveToJdbc.validate(overwriteConfig.copy(password_file = ""))
    }

    it should "reject an unknown output_mode" in {
        an[IllegalArgumentException] should be thrownBy
            HiveToJdbc.validate(overwriteConfig.copy(output_mode = "upsert"))
    }

    it should "reject append with no date_field" in {
        an[IllegalArgumentException] should be thrownBy
            HiveToJdbc.validate(appendConfig.copy(date_field = ""))
    }

    it should "reject append with no date_value" in {
        an[IllegalArgumentException] should be thrownBy
            HiveToJdbc.validate(appendConfig.copy(date_value = ""))
    }

    // -------------------------------------------------------------- jdbcOptions

    "jdbcOptions" should "TRUNCATE + reload on overwrite (preserving table/grants)" in {
        val (opts, mode) = HiveToJdbc.jdbcOptions(overwriteConfig, "s3cr3t")
        mode shouldBe SaveMode.Overwrite
        opts("truncate") shouldBe "true"
        opts("url") shouldBe overwriteConfig.jdbc_url
        opts("dbtable") shouldBe s"${overwriteConfig.database}.${overwriteConfig.table}" // schema defaults to source schema
        opts("user") shouldBe overwriteConfig.db_user
        opts("password") shouldBe "s3cr3t"
        opts("driver") shouldBe "org.postgresql.Driver" // default
        opts("stringtype") shouldBe "unspecified"
        opts("batchsize") shouldBe "10000"
    }

    it should "use a custom jdbc_driver when provided" in {
        val (opts, _) = HiveToJdbc.jdbcOptions(overwriteConfig.copy(jdbc_driver = "com.mysql.cj.jdbc.Driver"), "s3cr3t")
        opts("driver") shouldBe "com.mysql.cj.jdbc.Driver"
    }

    it should "append without truncate when output_mode=append" in {
        val (opts, mode) = HiveToJdbc.jdbcOptions(appendConfig, "s3cr3t")
        mode shouldBe SaveMode.Append
        opts.contains("truncate") shouldBe false
    }

    it should "reflect a custom batch_size" in {
        val (opts, _) = HiveToJdbc.jdbcOptions(overwriteConfig.copy(batch_size = 500), "s3cr3t")
        opts("batchsize") shouldBe "500"
    }

    it should "override the target schema when target_schema is set" in {
        val (opts, _) = HiveToJdbc.jdbcOptions(overwriteConfig.copy(target_schema = "custom_schema"), "s3cr3t")
        opts("dbtable") shouldBe s"custom_schema.${overwriteConfig.table}"
    }

    "targetSchema" should "fall back to the source schema when target_schema is empty" in {
        HiveToJdbc.targetSchema(overwriteConfig) shouldBe overwriteConfig.database
    }

    it should "use target_schema when it is set" in {
        HiveToJdbc.targetSchema(overwriteConfig.copy(target_schema = "custom_schema")) shouldBe "custom_schema"
    }

    // ------------------------------------------------------------- selectSource

    private def sampleDf = {
        import spark.implicits._
        Seq(
            (1, "2020-01-01"),
            (2, "2020-01-01"),
            (3, "2020-01-02")
        ).toDF("id", "dt")
    }

    "selectSource" should "return every row on overwrite" in {
        HiveToJdbc.selectSource(sampleDf, overwriteConfig).count() shouldBe 3
    }

    it should "return every row on overwrite even if date fields happen to be set" in {
        val cfg = overwriteConfig.copy(date_field = "dt", date_value = "2020-01-01")
        HiveToJdbc.selectSource(sampleDf, cfg).count() shouldBe 3
    }

    it should "select only rows matching date_field = date_value on append" in {
        val selected = HiveToJdbc.selectSource(sampleDf, appendConfig)
        selected.count() shouldBe 2
        selected.select("id").collect().map(_.getInt(0)).toSet shouldBe Set(1, 2)
    }

    it should "return no rows on append when date_value matches nothing" in {
        HiveToJdbc.selectSource(sampleDf, appendConfig.copy(date_value = "1999-01-01")).count() shouldBe 0
    }

    it should "fail fast on append when date_field does not exist" in {
        an[AnalysisException] should be thrownBy
            HiveToJdbc.selectSource(sampleDf, appendConfig.copy(date_field = "no_such_column")).count()
    }

    // -------------------------------------------------------------- readHdfsFile

    "readHdfsFile" should "return raw file contents without trimming (caller trims the password)" in {
        val tmp = Files.createTempFile("hivetojdbc_pw", ".txt")
        try {
            // A real password file often ends in a newline; readHdfsFile must not swallow it,
            // so the trailing-newline handling stays the caller's explicit responsibility.
            Files.write(tmp, "s3cr3t\n".getBytes(StandardCharsets.UTF_8))
            HiveToJdbc.readHdfsFile(spark, tmp.toUri.toString) shouldBe "s3cr3t\n"
        } finally {
            Files.deleteIfExists(tmp)
        }
    }

    // ---------------------------------------------------------------- loadConfig

    "loadConfig" should "map CLI args onto the Config fields with correct defaults" in {
        val config = HiveToJdbc.loadConfig(Array(
            "--database", "test_schema",
            "--table", "test_table",
            "--jdbc_url", "jdbc:postgresql://test-host:5432/test_db",
            "--db_user", "test_user",
            "--password_file", "/path/to/test_pw.txt",
            "--batch_size", "500"
        ))
        config.database shouldBe "test_schema"
        config.table shouldBe "test_table"
        config.jdbc_url shouldBe "jdbc:postgresql://test-host:5432/test_db"
        config.db_user shouldBe "test_user"
        config.password_file shouldBe "/path/to/test_pw.txt"
        config.output_mode shouldBe "overwrite" // default
        config.batch_size shouldBe 500          // parsed as Int
    }
}
