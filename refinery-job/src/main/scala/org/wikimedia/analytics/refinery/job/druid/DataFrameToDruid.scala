package org.wikimedia.analytics.refinery.job.druid

import java.io.InputStream

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{DefaultHttpClient, LaxRedirectStrategy}
import org.apache.log4j.LogManager
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, LongType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime

import scala.io.Source
import scala.util.Random
import scala.util.parsing.json.JSON

/**
 * Ingestion status enumeration object
 */
object IngestionStatus extends Enumeration {
    val Initial, Loading, Done, Error = Value
}

/**
 * DataFrameToDruid companion object
 */
object DataFrameToDruid {
    // Constant: URL path to launch Druid ingestion.
    val LaunchTaskPath = "/druid/indexer/v1/task"

    // Constant: URL path to check task status.
    val CheckTaskPath = "/druid/indexer/v1/task/{{DRUID_TASK_ID}}/status"

    // Constant: DateTime format for Druid interval specs.
    val IntervalDateTimeFormat = "yyyy-MM-dd'T'HH:mm'Z'"

    // Constant: Name of the additional metric field that counts events.
    val EventCountMetricName = "eventCount"

    // Constant: Template to build Druid ingestion specs.
    val stream: InputStream = getClass.getResourceAsStream("/ingestion_spec_template.json")

    val IngestionSpecTemplate: String = Source.fromInputStream(stream).getLines.mkString
}
/**
 * DataFrame to Druid transaction class
 *
 * This class serves as a transaction helper to load data into Druid.
 * The source data should be passed in as a DataFrame instance.
 * Then an ingestion spec is created from the DataFrame schema.
 * And finally a request is sent to Druid to trigger data ingestion.
 * The process can be followed in 3 ways: callback, waiting and polling.
 *
 * Constructor example:
 *     val dftd = new DataFrameToDruid(
 *         dataSource = "some_dataset",
 *         inputDf = hiveContext.sql("select..."),
 *         dimensions = Seq("project", "language", ...),
 *         metrics = Seq("edits", "views", ...),
 *         intervals = Seq((new DateTime(2017, 1, 1), new DateTime(2017, 2, 1))),
 *         numShards = 4,
 *         hadoopQueue = "production"
 *     )
 *
 * Callback example:
 *     dftd.start((status: IngestionStatus.Value) => {
 *         // DataFrameToDruid will call this function asynchronously when the
 *         // process has ended, and pass it the resulting status.
 *     })
 *
 * Waiting example:
 *     dftd.start().await()
 *     // The code will only proceed when the process is done.
 *
 * Polling example:
 *     dftd.start()
 *     while (dftd.status() == IngestionStatus.Loading) Thread.sleep(10000)
 *     // The process is finished at this point.
 *
 * Parameters:
 *     sc  SparkContext.
 *     dataSource  Name of the target Druid data set (snake_case).
 *     inputDf  DataFrame containing the data to be loaded. The data must already
 *              be sliced to only contain the desired time intervals. It must also
 *              be flat (no nested fields) and contain only the fields that are to
 *              ingested into Druid.
 *     dimensions  Sequence of field names that Druid should index as dimensions.
 *     metrics  Sequence of field names that Druid should ingest as metrics.
 *              Those fields have to be of numerical type.
 *     intervals  Sequence of pairs (startDateTime, endDateTime) delimiting the
 *                intervals where the input DataFrame contains data.
 *     timestampColumn  Name of the field containing the timestamp. This field is
 *                      mandatory for Druid ingestion spec.
 *     timestampFormat  A string indicating the format of the timestamp field
 *                      (iso|millis|posix|auto|or any Joda time format).
 *     segmentGranularity  A string indicating the granularity of Druid's segments
 *                         for the data to be loaded (quarter|month|week|day|hour).
 *     queryGranularity  A string indicating the granularity of Druid's queries
 *                       for the data to be loaded (week|day|hour|minute|second).
 *     numShards  Number of shards for Druid ingestion [optional].
 *     reduceMemory  Memory to be used for Druid ingestion (string).
 *     hadoopQueue  Name of Hadoop queue to launch the ingestion.
 *     druidHost  String with Druid host.
 *     druidPort  String with Druid port.
 *     checkInterval  Integer with the number of milliseconds to wait between checks.
 *     tempFilePathOver  Optional string that overrides path to temporary file.
 *     httpClientOver  Optional HttpClient instance (only for testing purposes).
 */
class DataFrameToDruid(
                        spark: SparkSession,
                        dataSource: String,
                        inputDf: DataFrame,
                        dimensions: Seq[String],
                        metrics: Seq[String],
                        intervals: Seq[(DateTime, DateTime)],
                        timestampColumn: String,
                        timestampFormat: String,
                        segmentGranularity: String,
                        queryGranularity: String,
                        numShards: Int,
                        reduceMemory: String,
                        hadoopQueue: String,
                        druidHost: String,
                        druidPort: String,
                        checkInterval: Int = 10000,
                        tempFilePathOver: String = null.asInstanceOf[String],
                        httpClientOver: HttpClient = null.asInstanceOf[HttpClient]
) {
    private val log = LogManager.getLogger("DataFrameToDruid")

    // Create a temporary file path for Druid data.
    private val tempFilePath: String = if (tempFilePathOver != null) tempFilePathOver else {
        val randomId = Random.alphanumeric.take(5).mkString("")
        val timestamp = DateTime.now.toString("yyyyMMddHHmmss")
        s"/tmp/DataFrameToDruid/$dataSource/$timestamp/$randomId"
    }

    // Add the event count to the DataFrame.
    private val inputDfWithCount = inputDf.withColumn(DataFrameToDruid.EventCountMetricName, lit(1L))
    private val metricsWithCount = metrics :+ DataFrameToDruid.EventCountMetricName

    // Initialize Druid ingestion spec.
    log.info(s"Creating ingestion spec for $dataSource.")
    private val ingestionSpec: String = createIngestionSpec()
    log.info(ingestionSpec)

    // Create a runnable that will execute the ingestion when launched.
    private val statusUpdater: Thread = getStatusUpdater

    // Instance variables: need to be modified after constructor.
    private var ingestionStatus: IngestionStatus.Value = IngestionStatus.Initial
    private var userCallback: Option[(IngestionStatus.Value) => Unit] = None
    private var druidTaskId: String = ""

    // Initialize httpClient, instruct it to follow redirects.
    private val httpClient: HttpClient = if (httpClientOver != null) httpClientOver else {
        val client = new DefaultHttpClient()
        client.setRedirectStrategy(new LaxRedirectStrategy())
        client
    }

    /**
     * Starts the process of loading the DataFrame to Druid.
     *
     * Params:
     *     callback  Function to be executed once the process is finished [optional].
     *               It should accept a parameter of type IngestionStatus.Value,
     *               which will be passed the final status of the process.
     * Returns:
     *     This DataFrameToDruid instance
     *     (to allow things like `dftd.start().await()`).
     */
    def start(
        callback: Option[(IngestionStatus.Value) => Unit] = None
    ): DataFrameToDruid = {
        if (ingestionStatus == IngestionStatus.Initial) {
            userCallback = callback

            log.info(s"Writing temporary file for $dataSource.")
            inputDfWithCount
              .write
              .json(tempFilePath)

            log.info(s"Launching indexation task for $dataSource.")
            druidTaskId = sendIngestionRequest
            log.info(s"Indexation task for $dataSource launched successfully. " +
                     s"Task ID: $druidTaskId")
            ingestionStatus = IngestionStatus.Loading
            statusUpdater.start()
        } else {
            log.warn("Can not call start more than once. Ignoring.")
        }
        this
    }

    /**
     * Blocks execution until the loading process has finished.
     *
     * This method has the ugly name 'await', because scala classes
     * automatically define a method wait, which is not overridable.
     *
     * Returns:
     *     This DataFrameToDruid instance
     *     (to allow things like `dftd.start().await().status()`).
     */
    def await(): DataFrameToDruid = {
        if (ingestionStatus == IngestionStatus.Initial) {
            log.warn("Can not call await before calling start. Ignoring.")
        } else {
            statusUpdater.join()
        }
        this
    }

    /**
     * Returns the status of the loading process.
     *
     * Returns:
     *     IngestionStatus.Value (Initial|Loading|Done|Error).
     */
    def status(): IngestionStatus.Value = {
        ingestionStatus
    }

    // Creates the ingestion spec string by filling in the ingestion spec template
    // with the passed parameters. Returns the resulting string.
    private def createIngestionSpec(): String = {
        DataFrameToDruid.IngestionSpecTemplate
            .replace("{{INPUT_PATH}}", tempFilePath)
            .replace("{{DATA_SOURCE}}", dataSource)
            .replace("{{SEGMENT_GRANULARITY}}", segmentGranularity)
            .replace("{{QUERY_GRANULARITY}}", queryGranularity)
            .replace("{{INTERVALS_ARRAY}}", formatIntervals())
            .replace("{{DIMENSIONS}}", formatDimensions())
            .replace("{{TIMESTAMP_FORMAT}}", timestampFormat)
            .replace("{{TIMESTAMP_COLUMN}}", timestampColumn)
            .replace("{{METRICS}}", formatMetrics())
            .replace("{{NUM_SHARDS}}", numShards.toString)
            .replace("{{REDUCE_MEMORY}}", reduceMemory)
            .replace("{{HADOOP_QUEUE}}", hadoopQueue)
    }

    // Formats a sequence of pairs of DateTime objects into Druid intervals.
    private def formatIntervals(): String = {
        val formattedIntervals = intervals.map((interval) => {
            val startStr = interval._1.toString(DataFrameToDruid.IntervalDateTimeFormat)
            val endStr = interval._2.toString(DataFrameToDruid.IntervalDateTimeFormat)
            "\"" + startStr + "/" + endStr + "\""
        })
        "[" + formattedIntervals.mkString(", ") + "]"
    }

    // Formats a sequence of field names into Druid dimensions.
    private def formatDimensions(): String = {
        val formattedDimensions = dimensions.map((d) => "\"" + d + "\"")
        "[" + formattedDimensions.mkString(", ") + "]"
    }

    // Formats a sequence of field names into Druid metrics.
    // Only longSum and doubleSum metrics are supported, so metric fields with types
    // other than Integer, Long, Float and Double will raise an error.
    private def formatMetrics(): String = {
        val formattedMetrics = metricsWithCount.map((field) => {
            val fieldType = inputDfWithCount.schema.apply(field).dataType match {
                case IntegerType | LongType => "longSum"
                case FloatType | DoubleType => "doubleSum"
            }
            s"""{\"name\": \"$field\", \"fieldName\": \"$field\", \"type\": \"$fieldType\"}"""
        })
        "[" + formattedMetrics.mkString(", ") + "]"
    }

    // Returns a thread that keeps polling Druid to check the status of the
    // indexation task and updates the ingestionStatus var accordingly.
    // When the task is finished, executes finalizations.
    private def getStatusUpdater: Thread = {
        new Thread(
            new Runnable {
                def run() {
                    if (druidTaskId == "ERROR") {
                        ingestionStatus = IngestionStatus.Error
                    } else {
                        while (ingestionStatus == IngestionStatus.Loading) {
                            Thread.sleep(checkInterval)
                            log.info(s"Checking status of task $druidTaskId for $dataSource.")
                            ingestionStatus = getDruidTaskStatus match {
                                case "RUNNING" => IngestionStatus.Loading
                                case "SUCCESS" => IngestionStatus.Done
                                case "FAILED" | "ERROR" => IngestionStatus.Error
                            }
                        }
                    }
                    conclude()
                }
            }
        )
    }

    // Sends an http post request to Druid to trigger ingestion.
    // Returns the Druid task id.
    private def sendIngestionRequest: String = {
        val url = s"http://$druidHost:$druidPort${DataFrameToDruid.LaunchTaskPath}"
        val post = new HttpPost(url)
        post.addHeader("Content-type", "application/json")
        post.setEntity(new StringEntity(ingestionSpec))
        val response = httpClient.execute(post)
        val statusCode = response.getStatusLine.getStatusCode
        if (statusCode == 200) {
            val contentStream = response.getEntity.getContent
            val responseStr = IOUtils.toString(contentStream)
            val responseObj = JSON.parseFull(responseStr).get.asInstanceOf[Map[String, Any]]
            responseObj("task").asInstanceOf[String]
        } else "ERROR"
    }

    // Sends an http get request to Druid to check the ingestion task.
    // Returns the resulting task status.
    private def getDruidTaskStatus: String = {
        val path = DataFrameToDruid.CheckTaskPath.replace("{{DRUID_TASK_ID}}", druidTaskId)
        val url = s"http://$druidHost:$druidPort$path"
        val get = new HttpGet(url)
        val response = httpClient.execute(get)
        val statusCode = response.getStatusLine.getStatusCode
        if (statusCode == 200) {
            val contentStream = response.getEntity.getContent
            val responseStr = IOUtils.toString(contentStream)
            val responseObj = JSON.parseFull(responseStr).get.asInstanceOf[Map[String, Any]]
            val statusObj = responseObj("status").asInstanceOf[Map[String, Any]]
            statusObj("status").asInstanceOf[String]
        } else "ERROR"
    }

    // Deletes the temporary file and calls user callback.
    // Note that scala classes have dibs on the method name 'finalize', see: await().
    private def conclude(): Unit = {
        ingestionStatus match {
            case IngestionStatus.Done => log.info(
                s"Druid ingestion task $druidTaskId for $dataSource succeeded.")
            case IngestionStatus.Error => log.error(
                s"Druid ingestion task $druidTaskId for $dataSource failed.")
        }
        val path = new Path(tempFilePath)
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        if (fs.exists(path)) fs.delete(path, true)
        if (userCallback.isDefined) userCallback.get(ingestionStatus)
    }
}
