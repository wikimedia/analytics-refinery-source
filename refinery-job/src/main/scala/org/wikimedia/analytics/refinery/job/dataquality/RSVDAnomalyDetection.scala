package org.wikimedia.analytics.refinery.job.dataquality

import java.lang.Math.{abs, ceil, max, min}
import com.criteo.rsvd._
import org.apache.commons.math3.stat.descriptive.rank.Percentile
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, Days, Hours, Months}
import org.joda.time.format.DateTimeFormat
import org.wikimedia.analytics.refinery.tools.LogHelper
import scopt.OptionParser

/**
 * RSVD Anomaly Detection
 *
 * Given an anomaly detection metric source, outputs a line with the metric name
 * and its normalized deviation (comma separated i.e. "response_time,12.34") for
 * each metric whose last data point has an absolute normalized deviation higher
 * than the specified threshold. Doesn't output anything, if no metric in the
 * metric source meets this condition.
 *
 * == Source data parameters ==
 * The parameters source-table and metric-source identify the anomaly detection
 * table to use and the hive partition (metric source) to analyze. Those just
 * specify the input data set.
 *
 * == Last data point parameter ==
 * The parameter last-data-point-dt gives the date of the last data point
 * of the timeseries to be observed. This is also the data point that will be
 * subject to anomaly detection analysis. All data being older than that
 * will be used as reference data. All data being more recent than that
 * will be ignored.
 *
 * == Default filler parameter ==
 * Some time series might contain gaps (missing data points). But the RSVD
 * algorithm depends highly on the seasonality of the data, and therefore it is
 * preferrable to fill in all gaps before analyzing the time series. The
 * parameter default-filler is the value that will be used to fill in those
 * missing data points.
 *
 * == Max sparsity parameter ==
 * Sparse time series (time series containing lots of default values) are
 * difficult to analyze and are more prone to false positives. The parameter
 * max-sparsity indicates the sparsity threshold above which time series will
 * be ignored.
 *
 * == Threshold parameter ==
 * The parameter deviation-threshold tells this job what should be considered
 * as an anomaly and reported accordingly. All metrics whose last data point's
 * absolute normalized deviation is higher than this threshold will be flagged
 * as anomalous. Read below for the definition of the normalized deviation.
 *
 * == RSVD parameters ==
 * The parameters rsvd-dimensions, rsvd-oversample, rsvd-iterations and
 * rsvd-random-seed are specific to the RSVD algorithm used. See:
 * https://github.com/criteo/Spark-RSVD for a full reference.
 * Some parameters needed for the RSVD call are hardcoded, because they are
 * unlikely to change for this job (see BlockMatrix constants).
 *
 * == Normalized deviation ==
 * The regular deviation of a potentially anomalous data point is:
 *
 *   deviation = anomaly - median
 *
 * However, to determine whether this deviation indicates an anomaly, one must
 * know the context of the corresponding timeseries. A deviation of i.e. 5.3
 * may mean very different things depending on the magnitude and variation
 * of the reference data.
 * Now, the normalized deviation is more robust: doesn't need context and can
 * be compared across different metrics. Considering the chart:
 *
 *   (y axis)
 *     ^
 *     |
 *   --o-- highQuartile (75% pctl)
 *     |
 *   --o-- median (50% percentile)   --.
 *     |                               |-- deviationUnit (quartile - median)
 *   --o-- lowQuartile (25% pctl)    --'
 *     |
 *     |
 *     |
 *     x-- anomaly
 *     |
 *     v
 *
 * The normalized deviation is:
 *
 *   normalized deviation = (anomaly - median) / deviationUnit
 *
 * Where the deviation unit is the range between the quartile and the median
 * (if the anomaly is below median, use the low quartile; if it is above the
 * median, use the high quartile). This way, the normalized deviation is not
 * affected by the magnitude or variation of the reference timeseries.
 *
 * == Usage example ==
 *
 *   /usr/bin/spark2-submit \
 *     --master yarn \
 *     --deploy-mode cluster \
 *     --queue production \
 *     --driver-memory 2G \
 *     --executor-memory 2G \
 *     --executor-cores 4 \
 *     --class org.wikimedia.analytics.refinery.job.dataquality.RSVDAnomalyDetection \
 *     /srv/deployment/analytics/refinery/artifacts/org/wikimedia/analytics/refinery/refinery-job-0.1.22.jar \
 *     --source-table wmf.anomaly_detection \
 *     --metric-source traffic_entropy_by_country \
 *     --last-data-point-dt 2020-01-01 \
 *     --deviation-threshold 10 \
 *     --output-path /tmp/anomalies/testing
 *
 * == Output example ==
 *
 * The output of this job is written to a given output path, so that it can
 * be checked for existence and read programmatically. The format looks like:
 *
 *   MetricName,10.808835541792956
 *   AnotherMetricName,-14.644874464354071
 *   YetAnotherMetricName,14.10441303567644
 */

// The companion object contains constants, parameter definitions
// and the main method for executing from the command line.
object RSVDAnomalyDetection {

    // Constants for BlockMatrix partitioning.
    val MatrixBlockSize = 100
    val MatrixPartitionWidth = 10
    val MatrixPartitionHeight = 10

    // Maximum normalized deviation.
    // With some inputs (very short discrete time series) the formula for the
    // normalized deviation might return very high numbers or even infinite
    // (division by zero). This constant defines a cap for this value.
    val MaxNormalizedDeviation = 100.0

    // For timestamp formatting.
    val DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")

    // Helper types.
    case class TimeSeriesDecomposition(
        signal: Array[Double],
        noise: Array[Double]
    )

    // Definition of job parameters.
    case class Params(
        sourceTable: String = "",
        metricSource: String = "",
        lastDataPointDt: String = "",
        seasonalityCycle: Int = 7,
        outputPath: String = "",
        defaultFiller: Double = 0.0,
        maxSparsity: Double = 0.2,
        rsvdDimensions: Int = 1,
        rsvdOversample: Int = 5,
        rsvdIterations: Int = 3,
        rsvdRandomSeed: Int = 0,
        deviationThreshold: Double = 10.0
    )

    val argsParser = new OptionParser[Params]("RSVD Anomaly Detection") {
        help("help") text ("Print this usage text and exit.")

        opt[String]("source-table") required() valueName ("<table_name>") action { (x, p) =>
            p.copy(sourceTable = x)
        } text ("Fully qualified name of the table used to calculate the metrics to be analyzed.")

        opt[String]("metric-source") required() valueName ("<metric_source>") action { (x, p) =>
            p.copy(metricSource = x)
        } text ("Name of the job that produced the metrics to analyze.")

        opt[String]("last-data-point-dt") required() valueName ("<timestamp>") action { (x, p) =>
            p.copy(lastDataPointDt = x)
        } text ("Datetime of the last data point to analyze in ISO-8601 format (2020-01-01T00:00:00Z).")

        opt[Int]("seasonality-cycle") optional() valueName ("<integer>") action { (x, p) =>
            p.copy(seasonalityCycle = x)
        } text ("Number of days that conform a full seasonality cycle for the timeseries. Default: 7.")

        opt[String]("output-path") required() valueName ("<path>") action { (x, p) =>
            p.copy(outputPath = x)
        } text ("HDFS path where to write the output in case there are anomalies.")

        opt[Double]("default-filler") optional() valueName ("<double>") action { (x, p) =>
            p.copy(defaultFiller = x)
        } text ("Value that will be used to fill in gaps in time series. Default: 0.0.")

        opt[Double]("max-sparsity") optional() valueName ("<double>") action { (x, p) =>
            p.copy(maxSparsity = x)
        } text ("Time series with higher sparsity than this threshold will be ignored. Default: 0.2.")

        opt[Int]("rsvd-dimensions") optional() valueName ("<integer>") action { (x, p) =>
            p.copy(rsvdDimensions = x)
        } text ("Number of dimensions (singular values) to use for the RSVD algorithm. Default: 1.")

        opt[Int]("rsvd-oversample") optional() valueName ("<integer>") action { (x, p) =>
            p.copy(rsvdOversample = x)
        } text ("Oversample parameter to use for the RSVD algorithm. Default: 5.")

        opt[Int]("rsvd-iterations") optional() valueName ("<integer>") action { (x, p) =>
            p.copy(rsvdIterations = x)
        } text ("Number of iterations to use for the RSVD algorithm. Default: 3.")

        opt[Int]("rsvd-random-seed") optional() valueName ("<integer>") action { (x, p) =>
            p.copy(rsvdRandomSeed = x)
        } text ("Random seed to use for the RSVD algorithm. Default: 0.")

        opt[Double]("deviation-threshold") optional() valueName ("<float>") action { (x, p) =>
            p.copy(deviationThreshold = x)
        } text ("Metrics with absolute normalized deviation greater than this will be output. Default: 10.")
    }

    // Parse command line arguments and call run.
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName(s"RSVDAnomalyDetection")
        val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
        val params = argsParser.parse(args, Params()).getOrElse(sys.exit(1))
        new RSVDAnomalyDetection(spark, params).run()
    }
}

// The class contains the functionality of the job and holds the state of the
// spark session that can be reused across functions.
class RSVDAnomalyDetection(
    spark: SparkSession,
    params: RSVDAnomalyDetection.Params
) extends LogHelper {

    import spark.implicits._

    // Main method: Analyze metrics and output anomalous ones.
    def run(): Unit = {

        val metrics = getInputMetrics()
        log.info(s"Read ${metrics.length} metrics to analyze")

        val outputText = metrics.foldLeft("") { case (text, metric) =>

            val inputTimeSeries = readTimeSeries(metric)
            log.info(s"Read timeseries of length ${inputTimeSeries.length} for metric = $metric")

            if (timeSeriesCanBeAnalyzed(inputTimeSeries)) {
                val noiseTimeSeries = decomposeTimeSeries(inputTimeSeries).noise
                log.info(s"Extracted noise-timeseries of length ${noiseTimeSeries.length} for metric = $metric")
                val deviation = getLastDataPointDeviation(noiseTimeSeries)
                log.info(s"Computed Last-datapoint-deviation of $deviation for metric = $metric")
                if (abs(deviation) > params.deviationThreshold) {
                    text + s"$metric,$deviation\n"
                } else text
            } else text
        }

        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val outputPath = new Path(params.outputPath)
        if (outputText == "") {
            // If no anomalies detected, make sure output file does not exist.
            fs.delete(outputPath, false)
        } else {
            fs.create(outputPath).write(outputText.getBytes)
        }
    }

    /**
     * Queries the given anomaly detection metrics table and gets the name of
     * all metrics that belong to the specified metric source.
     */
    def getInputMetrics(): Array[String] = {

        val df = spark.sql(s"""
            SELECT DISTINCT metric
            FROM ${params.sourceTable}
            WHERE source = '${params.metricSource}'
        """)

        df.map(_.getString(0)).collect
    }

    /**
     * Queries the given anomaly detection metrics table and gets the timeseries
     * corresponding to the indicated metric and last data point datetime.
     */
    def readTimeSeries(metric: String): Array[Double] = {

        val df = spark.sql(s"""
            SELECT
                dt,
                value
            FROM ${params.sourceTable}
            WHERE
                source = '${params.metricSource}' AND
                metric = '$metric' AND
                dt <= '${params.lastDataPointDt}'
            ORDER BY dt
            LIMIT 1000000
        """)

        val rawTimeSeries = df.map(r => (r.getString(0), r.getDouble(1))).collect
        val timeSeries = fillInGaps(rawTimeSeries).map(_._2)

        // The input for the BlockMatrix needs to be rectangular, so the
        // time series length needs to be a multiple of the matrixHeight,
        // which is indicated by the seasonality cycle. The following lines
        // crop the oldest values of the time series to fit the matrix.
        val adjustedSize = timeSeries.length - (timeSeries.length % params.seasonalityCycle)
        timeSeries.takeRight(adjustedSize)
    }

    /**
     * Given a time series with both timestamp and value, return a new analog
     * time series with all potential gaps (missing data points) filled in with
     * the given default value.
     */
    def fillInGaps(
        rawTimeSeries: Array[(String, Double)]
    ): Array[(String, Double)] = {

        if (rawTimeSeries.length == 0) {
            // If the raw time series is empty we do not fill it in, because
            // it would be all default values, and never generate an anomaly.
            // We return the empty array, which will be ignored.
            rawTimeSeries
        } else {
            val minDt = DateTime.parse(rawTimeSeries(0)._1)
            val maxDt = DateTime.parse(params.lastDataPointDt)

            val rangeDays = Days.daysBetween(minDt, maxDt).getDays
            val fullDtRange = (0 to rangeDays).map(minDt.plusDays(_))

            // Create a time series of the desired length,
            // full of default values.
            val defaultFullTimeSeries = fullDtRange.map(
                dt => (RSVDAnomalyDetection.DateTimeFormatter.print(dt), params.defaultFiller)
            )

            // Merge the original time series on top of the default full
            // time series, to override default values with actual values
            // whenever available, returning a time series with no gaps.
            (defaultFullTimeSeries.toMap ++ rawTimeSeries.toMap).toArray.sortBy(_._1)
        }
    }

    /**
     * Returns true if the given time series is fit to be analyzed with
     * RSVD to detect anomalies (minimum-length and maximum-sparsity checks).
     * Returns false otherwise.
     */
    def timeSeriesCanBeAnalyzed(timeSeries: Array[Double]): Boolean = {

        // The anomaly detection algorithm needs a time series with enough
        // data points, for the RSVD algorithm to be effective. The miminum
        // length is defined by the height and width of the RSVD matrices.
        val minLength = params.seasonalityCycle * (params.rsvdDimensions + params.rsvdOversample)
        if (timeSeries.length >= minLength) {

            // Time series that are too sparse are filtered out, because they
            // are more difficult to extract signal from, and are more prone
            // to inaccuracies and false alarms.
            val sparsity = timeSeries.count(_ == params.defaultFiller) / timeSeries.length.toDouble
            sparsity <= params.maxSparsity

        } else false
    }

    /**
     * Decomposes the given time series into two: a signal time series and
     * a noise time series. For that, it uses the randomized singular value
     * decomposition algorithm (RSVD).
     */
    def decomposeTimeSeries(
        timeSeries: Array[Double]
    ): RSVDAnomalyDetection.TimeSeriesDecomposition = {

        val config = RSVDConfig(
            embeddingDim = params.rsvdDimensions,
            oversample = params.rsvdOversample,
            powerIter = params.rsvdIterations,
            seed = params.rsvdRandomSeed,
            blockSize = RSVDAnomalyDetection.MatrixBlockSize,
            partitionWidthInBlocks = RSVDAnomalyDetection.MatrixPartitionWidth,
            partitionHeightInBlocks = RSVDAnomalyDetection.MatrixPartitionHeight,
            computeLeftSingularVectors = true,
            computeRightSingularVectors = true
        )

        // The time series needs to be transformed into a matrix for RSVD to work.
        val matrix = timeSeriesToBlockMatrix(timeSeries, params.seasonalityCycle, config)

        val rsvdResults = RSVD.run(matrix, config, spark.sparkContext)
        // RSVD results need to be transformed into signal and noise time series.
        val signalTimeSeries = getSignalFromRsvdResults(rsvdResults, config)
        val noiseTimeSeries = timeSeries.zip(signalTimeSeries).map{
            // TODO: Consider using a corrected noise for negative values:
            //   -(signal / value - 1) * signal
            case (value, signal) => value - signal
        }

        RSVDAnomalyDetection.TimeSeriesDecomposition(
            signal = signalTimeSeries,
            noise = noiseTimeSeries
        )
    }

    /**
     * Returns a BlockMatrix containing the values of the given time series,
     * in the input format required by the RSVD algorithm.
     */
    def timeSeriesToBlockMatrix(
        timeSeries: Array[Double],
        matrixHeight: Int,
        config: RSVDConfig
    ): BlockMatrix = {

        // The way an RSVD BlockMatrix is defined is by using MatrixEntries.
        // Each MatrixEntry represents a position of the matrix, it has a
        // vertical coordinate I, a horizontal coordinate J and a value V.
        // The next loop transforms the time series into a set of MatrixEntries
        // with the following distribution:
        //
        //   timeSeries = [v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12]
        //
        //                 |  v1  v4  v7  v10  |
        //   blockMatrix = |  v2  v5  v8  v11  |
        //                 |  v3  v6  v9  v12  |
        //
        // The matrix height is given (fixed), while the matrix width depends
        // on the length of the time series. It's important to choose the right
        // matrix height when using RSVD for timeseries. You should use a
        // matrix height that corresponds to a seasonality cycle.
        val matrixEntries = timeSeries.zipWithIndex.map { case (value, idx) =>
            MatrixEntry(
                idx % matrixHeight,
                idx / matrixHeight,
                value
            )
        }

        // Constructs the BlockMatrix.
        BlockMatrix.fromMatrixEntries(
            spark.sparkContext.parallelize(matrixEntries),
            matHeight = matrixHeight,
            matWidth = ceil(timeSeries.length.toDouble / matrixHeight).toInt,
            config.blockSize,
            config.partitionHeightInBlocks,
            config.partitionWidthInBlocks
        )
    }

    /**
     * Returns a signal time series from the results of the RSVD calculation.
     *
     * To do this, the 3 decomposed matrices of the RSVD results are multiplied.
     * Usually, this would result in the original time series. But in our case,
     * the RSVD algorithm is executed with a low number of dimensions (singular
     * values) thus returning an aproximation of the original time series,
     * that includes its main trends, but ingnores its more subtle variations.
     * We consider that to be the signal time series.
     */
    def getSignalFromRsvdResults(
        rsvdResults: RsvdResults,
        config: RSVDConfig
    ): Array[Double] = {

        // Create a diagonal matrix from the singularValues array.
        // It uses matrixEntries to initialize a BlockMatrix as explained before.
        val matrixEntries = rsvdResults.singularValues.toArray.zipWithIndex.map {
            case (value, idx) => MatrixEntry(idx, idx, value)
        }

        // Construct the BlockMatrix.
        val singularValues = BlockMatrix.fromMatrixEntries(
            spark.sparkContext.parallelize(matrixEntries),
            matHeight = config.embeddingDim,
            matWidth = config.embeddingDim,
            config.blockSize,
            config.partitionWidthInBlocks,
            config.partitionHeightInBlocks
        ).toLocalMatrix

        val leftSingularVectors = rsvdResults.leftSingularVectors.get.toLocalMatrix
        val rightSingularVectors = rsvdResults.rightSingularVectors.get.toLocalMatrix

        // Multiply the 3 matrices to get the signal time series.
        (leftSingularVectors * singularValues * rightSingularVectors.t).toArray
    }

    /**
     * Returns the normalized deviation of the time series' last data point.
     *
     * The calculation is done on top of a noise time series, which has no
     * signal, meaning it is a predominantly flat collection of points normally
     * distributed along the x axis (above and below).
     * The 25, 50 (median) and 75 percentiles are calculated on top of the
     * time series excluding the last data point. And then that data point's
     * deviation is returned considering how far that point is from the median.
     */
    def getLastDataPointDeviation(
        noiseTimeSeries: Array[Double]
    ): Double = {
        // TODO: Maybe use statistical bootstrapping to calculate percentiles?
        // This would prevent short discrete (integer) timeseries to produce
        // weird percentile values.

        val referenceData = noiseTimeSeries.dropRight(1)
        val lastDataPoint = noiseTimeSeries.takeRight(1)(0)
        val percentile = new Percentile()
        val median = percentile.evaluate(referenceData, 50)

        if (lastDataPoint > median) {
            val deviationUnit = percentile.evaluate(referenceData, 75) - median

            if (deviationUnit == 0) {
                RSVDAnomalyDetection.MaxNormalizedDeviation
            } else {
                min((lastDataPoint - median) / deviationUnit, RSVDAnomalyDetection.MaxNormalizedDeviation)
            }
        }
        else if (lastDataPoint < median) {
            val deviationUnit = -(percentile.evaluate(referenceData, 25) - median)

            if (deviationUnit == 0) {
                -RSVDAnomalyDetection.MaxNormalizedDeviation
            } else {
                max((lastDataPoint - median) / deviationUnit, -RSVDAnomalyDetection.MaxNormalizedDeviation)
            }
        } else { // lastDataPoint == median
            0.0
        }
    }
}
