package org.wikimedia.analytics.refinery.spark.sql

import org.scalatest.{FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.core.config._

class TestSparkSQLConfig extends FlatSpec with Matchers with ConfigHelper {

    private case class BaseConfig(
        // Overriding the SparkSQLConfig fields for the ConfigHelper to pick them up
        override val database: Option[String] = None,
        override val quoted_query_string: Option[String] = None,
        override val query_file: Option[String] = None,
        override val initialisation_sql_files: Option[Seq[String]] = None,
        override val hiveconf: Option[Map[String, String]] = None,
        override val define: Option[Map[String, String]] = None
    ) extends SparkSQLConfig

    private case class ExtendedConfig(
        // Overriding the SparkSQLConfig fields for the ConfigHelper to pick them up
        override val database: Option[String] = None,
        override val quoted_query_string: Option[String] = None,
        override val query_file: Option[String] = None,
        override val initialisation_sql_files: Option[Seq[String]] = None,
        override val hiveconf: Option[Map[String, String]] = None,
        override val define: Option[Map[String, String]] = None,
        // Extended field
        test_field: String
    ) extends SparkSQLConfig

    private val testArgs:Array[String] = Array[String](
        "--database", "test_db",
        "--initialisation_sql_files", "file1.hql,file2.hql",
        "--hiveconf", "kh1:vh1,kh2:vh2",
        "--define", "kd1:vd1,kd2:vd2"
    )

    "SparkSQLConfig" should "correctly parse parameters for the base-config" in {
        val baseConfig = configureArgs[BaseConfig](testArgs :+ "--query_file" :+ "test_query_file.hql")
        baseConfig.validate()

        baseConfig.database should equal(Some("test_db"))
        baseConfig.query_file should equal(Some("test_query_file.hql"))
        baseConfig.quoted_query_string should equal(None)
        baseConfig.initialisation_sql_files should equal(Some(Seq("file1.hql", "file2.hql")))
        baseConfig.hiveconf should equal(Some(Map("kh1" -> "vh1", "kh2" -> "vh2")))
        baseConfig.define should equal(Some(Map("kd1" -> "vd1","kd2" -> "vd2")))
    }

    "SparkSQLConfig" should "correctly parse parameters for the extended-config" in {
        val extendedConfig = configureArgs[ExtendedConfig](testArgs :+ "--test_field" :+ "test_value" :+ "--quoted_query_string" :+ "'select ...'")
        extendedConfig.validate()

        extendedConfig.database should equal(Some("test_db"))
        extendedConfig.query_file should equal(None)
        extendedConfig.quoted_query_string should equal(Some("'select ...'"))
        extendedConfig.initialisation_sql_files should equal(Some(Seq("file1.hql", "file2.hql")))
        extendedConfig.hiveconf should equal(Some(Map("kh1" -> "vh1", "kh2" -> "vh2")))
        extendedConfig.define should equal(Some(Map("kd1" -> "vd1","kd2" -> "vd2")))
        extendedConfig.test_field should equal("test_value")
    }

    it should "fail validation if file-query and CLI-query parameters are used in conjunction" in {
        val baseConfig = configureArgs[BaseConfig](Array[String]("--quoted_query_string", "'select ...'", "--query_file", "test_query_file.hql"))
        an[IllegalArgumentException] should be thrownBy baseConfig.validate()
    }

    it should "fail if none of file-query or CLI-query parameters are used" in {
        val baseConfig = configureArgs[BaseConfig](testArgs)
        an[IllegalArgumentException] should be thrownBy baseConfig.validate()
    }

    "SparkSQLConfig" should "correctly generate SparkSQL CLI parameters" in {
        val baseConfig = configureArgs[BaseConfig](testArgs :+ "--query_file" :+ "test_query_file.hql")
        baseConfig.validate()
        val sparkSQLArgs = baseConfig.getSparkSQLArgs
        sparkSQLArgs should equal(Array[String](
            "--database", "test_db",
            "-f", "test_query_file.hql",
            "--hiveconf", "kh1=vh1",
            "--hiveconf", "kh2=vh2",
            "--define", "kd1=vd1",
            "--define", "kd2=vd2",
            "-i", "file1.hql",
            "-i", "file2.hql"
        ))
    }
}
