package org.wikimedia.analytics.refinery.job.dataquality

import org.scalatest.FlatSpec
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import scala.collection.immutable.ListMap

class TestPageViewMetrics extends FlatSpec with DataFrameSuiteBase {

    import spark.implicits._

    "generateAgentDailyRatioMetrics" should "correctly compute human, automated, and spider ratios" in {
        // Sample input data mimicking the schema expected
        val inputDf = Seq(
            ("user", 100L),
            ("automated", 50L),
            ("spider", 50L)
        ).toDF("agent_type", "view_count")

        // Minimal config to satisfy the function signature
        val conf = PageViewMetrics.Config(
            metrics_table = None,
            alerts_table = None,
            alerts_output_path = None,
            human_bounds = ListMap("lower"-> "0.0", "upper"->"1.0"),
            auto_bounds = ListMap("lower"->"0.0", "upper"->"1.0"),
            run_id = "test_run",
            year = 2025,
            month = 10,
            day = 29
        )

        // Run the function under test
        val resultDf = PageViewMetrics.generateAgentDailyRatioMetrics(spark)(conf, inputDf)

        // Collect the result
        val result = resultDf.collect().head

        val user = result.getAs[Long]("user")
        val automated = result.getAs[Long]("automated")
        val spider = result.getAs[Long]("spider")
        val total = result.getAs[Long]("total_views")
        val humanRatio = result.getAs[Double]("human_ratio")
        val automatedRatio = result.getAs[Double]("automated_ratio")
        val spiderRatio = result.getAs[Double]("spider_ratio")

        // === Assertions ===
        assert(user === 100L)
        assert(automated === 50L)
        assert(spider === 50L)
        assert(total === 200L)

        // Ratio validations
        assert(math.abs(humanRatio - 0.5) < 1e-6)
        assert(math.abs(automatedRatio - 0.25) < 1e-6)
        assert(math.abs(spiderRatio - 0.25) < 1e-6)
    }

    it should "handle missing agent types gracefully and fill with zeros" in {
        val inputDf = Seq(
            ("user", 80L) // only human views
        ).toDF("agent_type", "view_count")

        val conf = PageViewMetrics.Config(
            metrics_table = None,
            alerts_table = None,
            alerts_output_path = None,
            human_bounds = ListMap("lower"->"0.0", "upper"->"1.0"),
            auto_bounds = ListMap("lower"->"0.0", "upper"->"1.0"),
            run_id = "test_run",
            year = 2025,
            month = 10,
            day = 29
        )

        val resultDf = PageViewMetrics.generateAgentDailyRatioMetrics(spark)(conf, inputDf)
        val result = resultDf.collect().head

        val total = result.getAs[Long]("total_views")
        val humanRatio = result.getAs[Double]("human_ratio")
        val automatedRatio = result.getAs[Double]("automated_ratio")
        val spiderRatio = result.getAs[Double]("spider_ratio")

        assert(total === 80L)
        assert(math.abs(humanRatio - 1.0) < 1e-6)
        assert(math.abs(automatedRatio - 0.0) < 1e-6)
        assert(math.abs(spiderRatio - 0.0) < 1e-6)
    }
}