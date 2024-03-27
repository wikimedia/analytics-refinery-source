package org.wikimedia.analytics.refinery.job.refine

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}
import org.scalamock.scalatest.MockFactory
import org.wikimedia.analytics.refinery.job.refine.cli.RefineHiveDataset
import org.wikimedia.analytics.refinery.spark.sql.HiveExtensions._

class TestRefineHiveDataset extends FlatSpec
    with Matchers with DataFrameSuiteBase with BeforeAndAfterEach with MockFactory {

    val input_schema_base_uris: Seq[String] = Seq("https://schema.discovery.wmnet/repositories/secondary/jsonschema/")

    "Config" should "validate arguments" in {

        // This should not raise an error
        new RefineHiveDataset.Config(
            Seq("equiad", "codfw").map { dc =>
                s"/wmf/data/raw/eventlogging_legacy/${dc}.eventlogging_NavigationTiming/year=2024/month=02/day=15/hour=00"
            },
            "analytics/legacy/navigationtiming/1.6.0",
            "event.navigationtiming",
            Seq("eqiad", "codfw").map { dc => s"datacenter=$dc/year=2024/month=2/day=15/hour=0" },
            "json",
            input_schema_base_uris,
        )

        // This should raise an error
        assertThrows[java.lang.IllegalArgumentException] {
            new RefineHiveDataset.Config(
                input_schema_base_uris,
                "analytics/legacy/navigationtiming/1.6.0",
                "eventinvalidnavigationtiming",
                Seq("datacenter/eqiad/year/2024/month/2"),
                "json",
                Seq("https://schema.discovery.wmnet/repositories/secondary/jsonschema/"),
            )
        }

        // This should raise an error
        assertThrows[java.lang.IllegalArgumentException] {
            new RefineHiveDataset.Config(
                Seq("/wmf/data/raw/eventlogging_legacy/eventlogging_NavigationTiming/year=2024/month=02/day=15/hour=00"),
                "analytics/legacy/navigationtiming/1.6.0",
                "eventinvalidnavigationtiming",
                Seq("eqiad", "codfw").map { dc => s"datacenter=$dc/year=2024/month=2/day=15/hour=0" },
                "json",
                input_schema_base_uris,
            )
        }
    }

    var tableDir: Path = _
    var rawDataDir: Path = _

    private def rmDirIfExists(path: Path) = {
        if (path != null && Files.exists(path)) {
            Files.walkFileTree(path, new SimpleFileVisitor[Path]() {
                override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
                    Files.delete(file)
                    FileVisitResult.CONTINUE
                }

                override def postVisitDirectory(dir: Path, exc: java.io.IOException): FileVisitResult = {
                    Files.delete(dir)
                    FileVisitResult.CONTINUE
                }
            })
        }
    }

    override def afterEach(): Unit = {
        spark.sql("DROP DATABASE IF EXISTS db CASCADE")
        rmDirIfExists(tableDir)
        rmDirIfExists(rawDataDir)
    }
}
