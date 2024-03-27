package org.wikimedia.analytics.refinery.job.refine

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame

class RefineHelperTest extends FlatSpec
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
        val result = RefineHelper.applyTransforms(schema, spark, "db.table", transforms)
        result should equal(StructType(Seq(
            StructField("col1", IntegerType),
            StructField("col2", StringType),
            StructField("dummy", StringType, false),
        )))
    }
}
