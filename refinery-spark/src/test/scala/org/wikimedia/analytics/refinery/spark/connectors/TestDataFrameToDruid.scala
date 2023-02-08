package org.wikimedia.analytics.refinery.spark.connectors

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.http.ProtocolVersion
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.{HttpPost, HttpUriRequest}
import org.apache.http.entity.StringEntity
import org.apache.http.message.BasicHttpResponse
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.joda.time.DateTime
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.{FlatSpec, Matchers}

import scala.util.parsing.json.JSON


class TestDataFrameToDruid extends FlatSpec
    with Matchers with BeforeAndAfterEach with DataFrameSuiteBase with MockFactory {

    val testTempFile = "file:///tmp/TestDataFrameToDruid"
    var httpClientMock: HttpClient = null.asInstanceOf[HttpClient]
    var testDf: DataFrame = null.asInstanceOf[DataFrame]

    // Prevents JSON.parseFull to parse integers as doubles.
    JSON.globalNumberParser = (input: String) => {
        if (input.contains(".")) input.toDouble else input.toLong
    }

    // Asserts if the given json path for the given json string matches the expected value.
    // This function circumvents the type cast limitations of JSON.parseFull.
    def assertJson[T](jsonString: String, jsonPath: String, expectedValue: T): Unit = {
        val jsonObject = JSON.parseFull(jsonString).get.asInstanceOf[Map[String, Any]]
        // Allows paths with escaped dots for when they are part of the field name.
        val pathElements = jsonPath.replace("\\.", "_").split("\\.").map(_.replace("_", "."))
        val pathValue = pathElements.foldLeft(jsonObject.asInstanceOf[Any])((j, p) => {
            j.asInstanceOf[Map[String, Any]](p)
        })
        assert(pathValue.asInstanceOf[T] == expectedValue)
    }

    // Creates a DataFrameToDruid instance with testing defaults.
    def createDftd(): DataFrameToDruid = {
        new DataFrameToDruid(
            spark = spark,
            dataSource = "test",
            inputDf = testDf,
            dimensions = Seq("event.category", "event.action", "wiki"),
            metrics = Seq("event.seconds", "normalized_count"),
            transforms = Seq(DataFrameToDruid.Transform(
                name = "normalized_count",
                expression = "test_count / event.sampleRatio"
            )),
            countMetricName = "test_count",
            intervals = Seq((new DateTime(1970, 1, 1, 0, 0), new DateTime(1970, 1, 2, 0, 0))),
            timestampColumn = "timestamp",
            timestampFormat = "posix",
            segmentGranularity = "hour",
            queryGranularity = "minute",
            numShards = 2,
            mapMemory = "2048",
            reduceMemory = "8192",
            hadoopQueue = "default",
            druidHost = "test.druid.host",
            druidPort = "8090",
            checkInterval = 100,
            tempFilePathOver = testTempFile,
            httpClientOver = httpClientMock
        )
    }

    // Creates a BasicHttpResponse given a status code and the respose data.
    def createHttpResponse(statusCode: Int, data: String): BasicHttpResponse = {
        val response = new BasicHttpResponse(
            new ProtocolVersion("HTTP", 1, 1),
            statusCode,
            "TestDataFrameToDruid"
        )
        response.setEntity(new StringEntity(data))
        response
    }

    override def beforeEach(): Unit = {
        // Create schema and data to be used in tests.
        val testSchema: StructType = (new StructType)
            .add("event", (new StructType)
                .add("action", StringType)
                .add("category", StringType)
                .add("sampleRatio", DoubleType)
                .add("seconds", DoubleType)
            )
            .add("timestamp", LongType)
            .add("wiki", StringType)
        val testRDD = sc.parallelize(Seq(
            Row(Row("read", "cat1", 0.2, 10.0), 1L, "enwiki"),
            Row(Row("edit", "cat2", 0.2, 20.0), 2L, "enwiki"),
            Row(Row("read", "cat1", 0.5, 30.0), 3L, "enwiki"),
            Row(Row("read", "cat3", 0.5, 40.0), 4L, "enwiki")
        ))
        testDf = spark.createDataFrame(testRDD, testSchema)

        // Mock HttpClient to be injected into DataFrameToDruid.
        // Its behavior will be defined in each test.
        httpClientMock = mock[HttpClient]
    }

    override def afterEach(): Unit = {
        // Delete temp file in case DataFrameToDruid can not accomplish it
        // because of execution errors or assert failures.
        val path = new Path(testTempFile)
        val fs = FileSystem.get(sc.hadoopConfiguration)
        if (fs.exists(path)) fs.delete(path, true)
    }

    it should "request for ingestion with the correct url" in {
        inSequence {
            // Should recevie an ingestion request; checks method and url, returns ingestion task id.
            (httpClientMock.execute(_: HttpUriRequest)).expects(*).onCall { request: HttpUriRequest =>
                val uri = request.getURI
                assert(uri.getHost == "test.druid.host")
                assert(uri.getPort == 8090)
                assert(uri.getPath == "/druid/indexer/v1/task")
                createHttpResponse(200, """{"task": "test-task-1"}""")
            }
            // Should receive a status request; returns succeeded.
            (httpClientMock.execute(_: HttpUriRequest)).expects(*).returning(
                createHttpResponse(200, """{"status": {"status": "SUCCEEDED"}}""")
            )
        }
        createDftd().start().await()
    }

    it should "request for ingestion with the correct general spec" in {
        inSequence {
            // Should receive an ingestion request; checks spec, returns ingestion task id.
            (httpClientMock.execute(_: HttpUriRequest)).expects(*).onCall { request: HttpUriRequest =>
                val contentStream = request.asInstanceOf[HttpPost].getEntity.getContent
                val spec = IOUtils.toString(contentStream)

                assertJson(spec, "spec.ioConfig.inputSpec.paths", testTempFile)
                assertJson(spec, "spec.dataSchema.dataSource", "test")
                assertJson(spec, "spec.dataSchema.granularitySpec.segmentGranularity", "hour")
                assertJson(spec, "spec.dataSchema.granularitySpec.queryGranularity", "minute")
                assertJson(spec, "spec.dataSchema.granularitySpec.intervals",
                    Seq("1970-01-01T00:00Z/1970-01-02T00:00Z"))
                assertJson(spec, "spec.dataSchema.parser.parseSpec.dimensionsSpec.dimensions",
                    Seq("event_category", "event_action", "wiki"))
                assertJson(spec, "spec.dataSchema.parser.parseSpec.timestampSpec.format", "posix")
                assertJson(spec, "spec.dataSchema.parser.parseSpec.timestampSpec.column", "timestamp")
                assertJson(spec, "spec.dataSchema.metricsSpec", Seq(
                    Map("name" -> "event_seconds", "fieldName" -> "event_seconds", "type" -> "doubleSum"),
                    Map("name" -> "normalized_count", "fieldName" -> "normalized_count", "type" -> "doubleSum"),
                    Map("name" -> "test_count", "fieldName" -> "test_count", "type" -> "longSum")
                ))
                assertJson(spec, "spec.tuningConfig.partitionsSpec.numShards", 2L)
                assertJson(spec, "spec.tuningConfig.jobProperties.mapreduce\\.reduce\\.memory\\.mb", "8192")
                assertJson(spec, "spec.tuningConfig.jobProperties.mapreduce\\.job\\.queuename", "default")

                createHttpResponse(200, """{"task": "test-task-1"}""")
            }
            // Should receive a status request; returns succeeded.
            (httpClientMock.execute(_: HttpUriRequest)).expects(*).returning(
                createHttpResponse(200, """{"status": {"status": "SUCCEEDED"}}""")
            )
        }
        createDftd().start().await()
    }

    it should "request for ingestion with the correct transforms spec" in {
        inSequence {
            // Should receive an ingestion request; checks spec, returns ingestion task id.
            (httpClientMock.execute(_: HttpUriRequest)).expects(*).onCall { request: HttpUriRequest =>
                val contentStream = request.asInstanceOf[HttpPost].getEntity.getContent
                val spec = IOUtils.toString(contentStream)

                assertJson(spec, "spec.dataSchema.transformSpec.transforms", Seq(Map(
                    "type" -> "expression",
                    "name" -> "normalized_count",
                    "expression" -> "test_count / event_sampleRatio"
                )))

                createHttpResponse(200, """{"task": "test-task-1"}""")
            }
            // Should receive a status request; returns succeeded.
            (httpClientMock.execute(_: HttpUriRequest)).expects(*).returning(
                createHttpResponse(200, """{"status": {"status": "SUCCEEDED"}}""")
            )
        }
        createDftd().start().await()
    }

    it should "request for ingestion with the correct flatten spec" in {
        inSequence {
            // Should receive an ingestion request; checks spec, returns ingestion task id.
            (httpClientMock.execute(_: HttpUriRequest)).expects(*).onCall { request: HttpUriRequest =>
                val contentStream = request.asInstanceOf[HttpPost].getEntity.getContent
                val spec = IOUtils.toString(contentStream)

                assertJson(spec, "spec.dataSchema.transformSpec.transforms", Seq(Map(
                    "type" -> "expression",
                    "name" -> "normalized_count",
                    "expression" -> "test_count / event_sampleRatio"
                )))

                createHttpResponse(200, """{"task": "test-task-1"}""")
            }
            // Should receive a status request; returns succeeded.
            (httpClientMock.execute(_: HttpUriRequest)).expects(*).returning(
                createHttpResponse(200, """{"status": {"status": "SUCCEEDED"}}""")
            )
        }
        createDftd().start().await()
    }
    it should "request for ingestion with the correct data" in {
        inSequence {
            // Should recevie an ingestion request; checks data, returns ingestion task id.
            (httpClientMock.execute(_: HttpUriRequest)).expects(*).onCall { request: HttpUriRequest =>
                val inputDf = spark.read.json(testTempFile)
                val expectedDf = testDf.withColumn("test_count", lit(1L))
                assert(inputDf.intersect(expectedDf).take(1).isEmpty)
                createHttpResponse(200, """{"task": "test-task-1"}""")
            }
            // Should receive a status request; returns succeeded.
            (httpClientMock.execute(_: HttpUriRequest)).expects(*).returning(
                createHttpResponse(200, """{"status": {"status": "SUCCEEDED"}}""")
            )
        }
        createDftd().start().await()
    }

    it should "request for status check with correct method and url" in {
        inSequence {
            // Should recevie an ingestion request; returns ingestion task id.
            (httpClientMock.execute(_: HttpUriRequest)).expects(*).returning(
                createHttpResponse(200, """{"task": "test-task-1"}""")
            )
            // Should receive a status request; checks method and url, returns succeeded.
            (httpClientMock.execute(_: HttpUriRequest)).expects(*).onCall { request: HttpUriRequest =>
                val uri = request.getURI
                assert(uri.getHost == "test.druid.host")
                assert(uri.getPort == 8090)
                assert(uri.getPath == "/druid/indexer/v1/task/test-task-1/status")
                assert(request.getMethod == "GET")
                createHttpResponse(200, """{"status": {"status": "SUCCEEDED"}}""")
            }
        }
        createDftd().start().await()
    }

    it should "delete the temporary file" in {
        inSequence {
            // Should recevie an ingestion request; returns ingestion task id.
            (httpClientMock.execute(_: HttpUriRequest)).expects(*).returning(
                createHttpResponse(200, """{"task": "test-task-1"}""")
            )
            // Should receive a status request; returns succeeded.
            (httpClientMock.execute(_: HttpUriRequest)).expects(*).returning(
                createHttpResponse(200, """{"status": {"status": "SUCCESS"}}""")
            )
        }
        createDftd().start().await()

        // Check that the temp file has been deleted.
        val path = new Path(testTempFile)
        val fs = FileSystem.get(sc.hadoopConfiguration)
        assert(!fs.exists(path))
    }

    it should "call the user callback once finished" in {
        inSequence {
            // Should recevie an ingestion request; returns ingestion task id.
            (httpClientMock.execute(_: HttpUriRequest)).expects(*).returning(
                createHttpResponse(200, """{"task": "test-task-1"}""")
            )
            // Should receive a status request; returns succeeded.
            (httpClientMock.execute(_: HttpUriRequest)).expects(*).returning(
                createHttpResponse(200, """{"status": {"status": "SUCCESS"}}""")
            )
        }

        // Check that callback is actually executed.
        var callbackExecuted = false
        createDftd().start(Some((status: IngestionStatus.Value) => {
            assert(status == IngestionStatus.Done)
            callbackExecuted = true
        })).await()
        assert(callbackExecuted)
    }
}
