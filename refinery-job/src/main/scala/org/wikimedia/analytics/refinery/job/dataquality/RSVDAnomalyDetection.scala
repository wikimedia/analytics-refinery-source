package org.wikimedia.analytics.refinery.job.dataquality

import java.lang.Math.{abs, ceil, max, min}

import com.criteo.rsvd._
import org.apache.commons.math3.stat.descriptive.rank.Percentile
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.sql.SparkSession
import org.wikimedia.analytics.refinery.core.LogHelper
import scopt.OptionParser

/**
 * RSVD Anomaly Detection
 *
 * Given a data quality metric group, outputs a line with the metric name and
 * its normalized deviation (comma separated, i.e. "response_time,12.34") for
 * each metric whose last data point has an absolute normalized deviation higher
 * than the specified threshold. Doesn't output anything, if no metric in the
 * metric group meets this condition.
 *
 * == Source data parameters ==
 * The parameters quality-table, source-table, query-name and granularity
 * identify the data quality stats table to use and the hive partition (metric
 * group) to analyze. Those just specify the input data set.
 *
 * == Last data point parameter ==
 * The parameter last-data-point-dt gives the datetime of the last data point
 * of the timeseries to be observed. This is also the data point that will be
 * subject to anomaly detection analysis. All data being older than that
 * will be used as reference data. All data being more recent than that
 * will be ignored.
 *
 * == Seasonality parameter ==
 * The parameter seasonality-cycle should indicate the strongest seasonality
 * period of the metric group. The seasonality cycle is given in data points.
 * For example: if the data has hourly granularity and daily seasonality
 * (timeseries pattern repeats every day) the seasonality-cycle value should
 * be 24, because it takes 24 data points to complete a seasonality cycle.
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
 *   sudo -u analytics /usr/bin/spark2-submit \
 *     --master yarn \
 *     --deploy-mode cluster \
 *     --queue production \
 *     --driver-memory 2G \
 *     --executor-memory 2G \
 *     --executor-cores 4 \
 *     --class org.wikimedia.analytics.refinery.job.dataquality.RSVDAnomalyDetection \
 *     /srv/deployment/analytics/refinery/artifacts/org/wikimedia/analytics/refinery/refinery-job-0.0.110.jar \
 *     --quality-table wmf.data_quality_stats \
 *     --source-table wmf.pageview_hourly \
 *     --query-name traffic_entropy_by_country \
 *     --granularity daily \
 *     --last-data-point-dt 2020-01-01T00:00:00Z \
 *     --seasonality-cycle 7 \
 *     --deviation-threshold 10
 *
 * == Output example ==
 *
 * The output of this job is written to a given output path, so that it can
 * be checked for existence and read by Oozie. The format is the following:
 *
 *   Myanmar,10.808835541792956
 *   Cuba,-14.644874464354071
 *   Iran,14.10441303567644
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

    // Helper types.
    case class TimeSeriesDecomposition(
        signal: Array[Double],
        noise: Array[Double]
    )

    // Definition of job parameters.
    case class Params(
        qualityTable: String = "",
        sourceTable: String = "",
        queryName: String = "",
        granularity: String = "",
        lastDataPointDt: String = "",
        seasonalityCycle: Int = 1,
        outputPath: String = "",
        rsvdDimensions: Int = 1,
        rsvdOversample: Int = 5,
        rsvdIterations: Int = 3,
        rsvdRandomSeed: Int = 0,
        deviationThreshold: Double = 10.0
    )

    val argsParser = new OptionParser[Params]("RSVD Anomaly Detection") {
        help("help") text ("Print this usage text and exit.")

        opt[String]("quality-table") required() valueName ("<table_name>") action { (x, p) =>
            p.copy(qualityTable = x)
        } text ("Fully qualified name of the data_quality_stats table.")

        opt[String]("source-table") required() valueName ("<table_name>") action { (x, p) =>
            p.copy(sourceTable = x)
        } text ("Fully qualified name of the table used to calculate the metrics to be analyzed.")

        opt[String]("query-name") required() valueName ("<query_name>") action { (x, p) =>
            p.copy(queryName = x)
        } text ("Name of the query file (without .hql) used to calculate the metrics to be analyzed.")

        opt[String]("granularity") required() valueName ("<granularity>") action { (x, p) =>
            p.copy(granularity = x)
        } text ("Granularity of the metrics to be analyzed (hourly|daily|monthly).")

        opt[String]("last-data-point-dt") required() valueName ("<timestamp>") action { (x, p) =>
            p.copy(lastDataPointDt = x)
        } text ("Datetime of the last data point to analyze in ISO-8601 format (2020-01-01T00:00:00Z).")

        opt[Int]("seasonality-cycle") required() valueName ("<integer>") action { (x, p) =>
            p.copy(seasonalityCycle = x)
        } text ("Number of data points that conform a seasonal cycle.")

        opt[String]("output-path") required() valueName ("<path>") action { (x, p) =>
            p.copy(outputPath = x)
        } text ("HDFS path where to write the output in case there are anomalies.")

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
        new RSVDAnomalyDetection(spark).run(params)
    }
}

// The class contains the functionality of the job and holds the state of the
// spark session that can be reused across functions.
class RSVDAnomalyDetection(spark: SparkSession) extends LogHelper {

    import spark.implicits._

    // Main method: Analyze metrics and output anomalous ones.
    def run(params: RSVDAnomalyDetection.Params): Unit = {

        val metrics = getInputMetrics(params)
        log.info(s"Read ${metrics.length} metrics to analyze")
        val outputText = metrics.foldLeft("") { case (text, metric) =>
            val inputTimeSeries = readTimeSeries(params, metric)
            log.info(s"Read timeseries of length ${inputTimeSeries.length} for metric = $metric")

            // Compute only timeseries with enough data points.
            // For the RSVD anomaly detection algorithm to work, it needs a minimum
            // amount of data for matrices multiplication.
            if (inputTimeSeries.length >= params.seasonalityCycle * (params.rsvdDimensions + params.rsvdOversample)) {
                val noiseTimeSeries = decomposeTimeSeries(inputTimeSeries, params).noise
                log.info(s"Extracted noise-timeseries of length ${noiseTimeSeries.length} for metric = $metric")
                val deviation = getLastDataPointDeviation(noiseTimeSeries)
                log.info(s"Computed Last-datapoint-deviation of $deviation for metric = $metric")
                if (abs(deviation) > params.deviationThreshold) {
                    text + s"$metric,$deviation\n"
                } else text
            } else text
        }

        if (outputText != "") {
            val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
            fs.create(new Path(params.outputPath)).write(outputText.getBytes)
        }
    }

    /**
     * Queries the given data quality table and gets the name of all metrics
     * that belong to the metric group corresponding to the specified partition.
     */
    def getInputMetrics(params: RSVDAnomalyDetection.Params): Array[String] = {

        val df = spark.sql(s"""
            SELECT DISTINCT metric
            FROM ${params.qualityTable}
            WHERE
                source_table = '${params.sourceTable}' AND
                query_name = '${params.queryName}' AND
                granularity = '${params.granularity}'
        """)

        df.map(_.getString(0)).collect

    }

    /**
     * Queries the given data quality table and gets the time series
     * corresponding to the indicated metric and last data point datetime.
     */
    def readTimeSeries(
        params: RSVDAnomalyDetection.Params,
        metric: String
    ): Array[Double] = {

        val df = spark.sql(s"""
            SELECT value
            FROM ${params.qualityTable}
            WHERE
                source_table = '${params.sourceTable}' AND
                query_name = '${params.queryName}' AND
                granularity = '${params.granularity}' AND
                metric = '$metric' AND
                dt <= '${params.lastDataPointDt}'
            ORDER BY dt
            LIMIT 1000000
        """)

        val timeSeries = df.map(_.getDouble(0)).collect

        // The input for the BlockMatrix needs to be rectangular, so the
        // time series length needs to be a multiple of the matrixHeight,
        // which is indicated by the seasonality cycle. The following lines
        // crop the oldest values of the time series to fit the matrix.
        val adjustedSize = timeSeries.length - (timeSeries.length % params.seasonalityCycle)

        timeSeries.takeRight(adjustedSize)
    }

    /**
     * Decomposes the given time series into two: a signal time series and
     * a noise time series. For that, it uses the randomized singular value
     * decomposition algorithm (RSVD).
     */
    def decomposeTimeSeries(
        timeSeries: Array[Double],
        params: RSVDAnomalyDetection.Params
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
