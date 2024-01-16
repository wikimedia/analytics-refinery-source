package org.wikimedia.analytics.refinery.tools.config

import com.github.nscala_time.time.Imports.{DateTime, DateTimeFormat, DateTimeZone}
import org.joda.time.format.DateTimeFormatter
import org.scalactic.Equality
import org.scalatest.{BeforeAndAfter, Inside}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.ListMap
import scala.util.matching.Regex


class TestConfigHelper extends FlatSpec with Matchers with BeforeAndAfter with Inside with ConfigHelper {

    case class Params(
        str: String = "default str",
        strOpt: Option[String] = None,
        int: Int = 0,
        intOpt: Option[Int] = None,
        double: Double = 1.0,
        doubleOpt: Option[Double] = None,
        bool: Boolean = false,
        groups: Seq[String] = Seq("first"),
        dt: DateTime = new DateTime(2018, 1, 1, 0, 0),
        dtFormatter: DateTimeFormatter = DateTimeFormat.forPattern("'hourly'/yyyy/MM/dd/HH"),
        pattern: Regex = "(.*)".r,
        patternOpt: Option[Regex] = None,
        map: Map[String, String] = Map(),
        listMap: ListMap[String, String] = ListMap()
    )

    implicit val regexEquality: Equality[Regex] = new Equality[Regex] {
        def areEqual(expected: Regex, given: Any): Boolean = {
            given match {
                case r: Regex => r.toString == expected.toString
                case _ => false
            }
        }
    }

    def assertOptionsEqual[_](expected: Option[_], given: Option[_]): Unit = {
        if (given.isDefined && expected.isDefined)
            given.get should equal(expected.get)
        else
            given should equal(expected)
    }

    def assertParamsEqual(given: Params, expected: Params): Unit = {
        inside(given) { case Params(str, strOpt, int, intOpt, double, doubleOpt, bool, groups, dt, dtFormatter, pattern, patternOpt, map, listMap) => {
            str should be(expected.str)
            assertOptionsEqual(strOpt, expected.strOpt)
            int should be(expected.int)
            assertOptionsEqual(intOpt, expected.intOpt)
            double should be(expected.double)
            assertOptionsEqual(doubleOpt, expected.doubleOpt)
            bool should be(expected.bool)
            groups should be(expected.groups)
            dt should be(expected.dt)
            dtFormatter should be (expected.dtFormatter)
            pattern should equal(expected.pattern)
            map should equal(expected.map)
            listMap should equal(expected.listMap)

            // can't use assertOptionsEqual, need to compare against Regex .toString
            if (patternOpt.isDefined && expected.patternOpt.isDefined)
                patternOpt.get should equal(expected.patternOpt.get)
            else
                patternOpt should equal(expected.patternOpt)
        }}

    }


    it should "load Params with defaults" in {
        val expected = Params()
        assertParamsEqual(configure[Params](Array.empty, Array.empty), expected)
    }

    it should "load Params with args" in {
        val expected = Params(
            "myStr",
            None,
            0,
            None,
            1.0,
            None,
            bool=true,
            Seq("first", "second"),
            new DateTime(2018, 2, 1, 0, 0, DateTimeZone.UTC),
            DateTimeFormat.forPattern("yyyy-MM-dd'T'HH"),
            "(\\w+) (\\w+)".r,
            Some("\\d+".r),
            Map("k1" -> "v1", "k2" -> "v2"),
            ListMap("k3" -> "v3", "k4" -> "v4")
        )

        val args = Array(
            "--str",            "myStr",
            "--dt",             "2018-02-01T00:00:00Z",
            "--groups",         "first,second",
            "--dtFormatter",    "yyyy-MM-dd'T'HH",
            "--pattern",        "(\\w+) (\\w+)",
            "--bool",           "true",
            "--patternOpt",     "\\d+",
            "--map",            "k1:v1,k2:v2",
            "--listMap",        "k3:v3,k4:v4"
        )
        assertParamsEqual(configure[Params](Array.empty, args), expected)
    }

    it should "load Params with file" in {
        val expected = Params(
            "default str",
            None,
            0,
            None,
            1.0,
            None,
            bool=true,
            Seq("joseph", "andrew"),
            new DateTime(2018, 3, 1, 0, 0, DateTimeZone.UTC),
            DateTimeFormat.forPattern("'hourly'/yyyy/MM/dd/HH"),
            "[abc]".r
        )

        val propertiesFile: String = getClass.getResource("/test_config_helper.properties").getPath
        assertParamsEqual(configure[Params](Array(propertiesFile), Array.empty), expected)
    }

    it should "load Params with file and args overrides" in {
        val expected = Params(
            "overrideString",
            None,
            123,
            Some(456),
            3.3,
            Some(4.4),
            bool=false,
            Seq("joseph", "andrew"),
            new DateTime(2018, 4, 1, 0, 0, DateTimeZone.UTC),
            DateTimeFormat.forPattern("'hourly'/yyyy/MM/dd/HH"),
            "[abc]".r,
            map=Map("k1" -> "v1", "k2" -> "v2"),
            listMap=ListMap("k3" -> "v3", "k4" -> "v4")
        )

        val args = Array(
            "--str", "overrideString",
            "--dt", "2018-04-01T00:00:00Z",
            "--bool", "false",
            "--int", "123",
            "--intOpt", "456",
            "--double", "3.3",
            "--doubleOpt", "4.4",
            "--map=k1:v1,k2:v2",
            "--listMap=k3:v3,k4:v4"
        )

        val propertiesFile: String = getClass.getResource("/test_config_helper.properties").getPath
        assertParamsEqual(configure[Params](Array(propertiesFile), args), expected)
    }


    it should "load Params with multiple files and args overrides" in {
        val expected = Params(
            "overrideString",
            Some("overriddenStringOpt"),
            123,
            Some(456),
            3.3,
            Some(1.234),
            bool=false,
            Seq("otto", "joal"),
            new DateTime(2018, 4, 1, 0, 0, DateTimeZone.UTC),
            DateTimeFormat.forPattern("'hourly'/yyyy/MM/dd/HH"),
            "[abc]".r
        )

        val args = Array(
            "--str", "overrideString",
            "--dt", "2018-04-01T00:00:00Z",
            "--bool", "false",
            "--int", "123",
            "--intOpt", "456",
            "--double", "3.3",
            "--strOpt", "overriddenStringOpt"
        )

        val propertiesFile: String = getClass.getResource("/test_config_helper.properties").getPath
        val propertiesFile2:       String = getClass.getResource("/test_config_helper2.properties").getPath

        assertParamsEqual(configure[Params](Array(propertiesFile, propertiesFile2), args), expected)
    }


    it should "load Params with multiple --config_file and args overrides" in {
        val expected = Params(
            "overrideString",
            Some("overriddenStringOpt"),
            123,
            Some(456),
            3.3,
            Some(1.234),
            bool=false,
            Seq("otto", "joal"),
            new DateTime(2018, 4, 1, 0, 0, DateTimeZone.UTC),
            DateTimeFormat.forPattern("'hourly'/yyyy/MM/dd/HH"),
            "[abc]".r
        )

        val propertiesFile: String = getClass.getResource("/test_config_helper.properties").getPath
        val propertiesFile2:       String = getClass.getResource("/test_config_helper2.properties").getPath


        val args = Array(
            "--str", "overrideString",
            "--dt", "2018-04-01T00:00:00Z",
            "--bool", "false",
            "--config_file", propertiesFile,
            "--int", "123",
            "--intOpt", "456",
            "--double", "3.3",
            "--strOpt", "overriddenStringOpt",
            "--config_file=" + propertiesFile2
        )


        assertParamsEqual(configureArgs[Params](args), expected)
    }


    it should "fail converting bad int" in {
        a [RuntimeException] should be thrownBy configure[Params](Array.empty, Array("--int", "not 1 an 2 int"))
    }
    it should "fail converting bad double" in {
        a [RuntimeException] should be thrownBy configure[Params](Array.empty, Array("--double", "not 1.0 an 2.0 double"))
    }
    it should "fail converting bad regex" in {
        a [RuntimeException] should be thrownBy configure[Params](Array.empty, Array("--pattern", "(.*"))
    }
    it should "fail converting bad boolean value" in {
        a [RuntimeException] should be thrownBy configure[Params](Array.empty, Array("--bool", "hello"))
    }
    it should "fail converting bad DateTimeFormatter" in {
        a [RuntimeException] should be thrownBy configure[Params](Array.empty, Array("--dtFormatter", "unquoted"))
    }
    it should "fail converting bad DateTime" in {
        a [RuntimeException] should be thrownBy configure[Params](Array.empty, Array("--dt", "not a dt"))
    }

    it should "throw ConfigHelperException for missing required config" in {
        case class ParamsWithRequired(required1: String, required2: String, optional: String = "no worries")
        a [ConfigHelperException] should be thrownBy configure[ParamsWithRequired](Array.empty, Array("--required2=got-it"))
    }


    it should "extract --config_file" in {
        val args = Array("--hi", "there", "--config_file", "/path/to/y.yaml", "--yo=true", "--config_file=/path/to/p.properties", "--yes=no", "--config_file=/path/1.yaml,/path/2.json")

        val (files, remainingArgs) = ConfigHelperMacros.extractOpts("--config_file")(args)

        val expectedFiles = Array("/path/to/y.yaml", "/path/to/p.properties", "/path/1.yaml", "/path/2.json")
        val expectedArgs = Array("--hi", "there", "--yo=true", "--yes=no")

        files should equal(expectedFiles)
        remainingArgs should equal(expectedArgs)
    }

}
