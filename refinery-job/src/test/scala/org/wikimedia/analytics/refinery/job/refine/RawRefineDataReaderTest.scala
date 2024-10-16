package org.wikimedia.analytics.refinery.job.refine

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame

class RawRefineDataReaderTest extends FlatSpec
    with Matchers
    with DataFrameSuiteBase
    with BeforeAndAfterEach {

    val dummy_transform: PartitionedDataFrame => PartitionedDataFrame = partDf => {
        val newPartDf = partDf.df.withColumn("dummy", lit("dummy"))
        PartitionedDataFrame(newPartDf, partDf.partition)
    }

    "applyTransforms" should "apply transforms to a schema" in {
        val transforms = Seq(dummy_transform)
        val schema = StructType(Seq(
            StructField("col1", IntegerType),
            StructField("col2", StringType)
        ))
        val result = RefineHelper.applyTransforms(spark, schema, transforms)
        result should equal(StructType(Seq(
            StructField("col1", IntegerType),
            StructField("col2", StringType),
            StructField("dummy", StringType, false),
        )))
    }

    "readInputDataFrameWithSchemaURI" should "read a json input" in {
        val inputPath = getClass.getResource("/event_data/raw/event/eqiad_table_a/hourly/2021/03/22/19").toString
        val outputPath = getClass.getResource("/event_data") + "refined/event/table_a/datacenter=eqiad/year=2021/month=03/day=22/hour=19"
        val schemasBaseUris = Seq(getClass.getResource("/event_data/schemas").toString)

        val eventSparkSchemaLoader = EventSparkSchemaLoader(
            schemasBaseUris,
            loadLatest = false,
            None
        )

        val rt = RefineTarget(
            spark,
            inputPath,
            outputPath,
            eventSparkSchemaLoader
        )

        val sparkSchema = rt.schema.get

        val reader = RawRefineDataReader(spark = spark, sparkSchema = sparkSchema)

        val df = reader.readInputDataFrameWithSchemaURI(
            Seq(inputPath)
        )

        df.count() should equal(1)
    }
}
