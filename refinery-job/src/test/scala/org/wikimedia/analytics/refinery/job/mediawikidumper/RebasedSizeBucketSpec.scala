package org.wikimedia.analytics.refinery.job.mediawikidumper

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{functions, Row}
import org.apache.spark.sql.catalyst.dsl.expressions
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.job.mediawikidumper.RebasedSizeBucket.bucket
import org.wikimedia.analytics.refinery.tools.LogHelper

class RebasedSizeBucketSpec
    extends FlatSpec
    with Matchers
    with BeforeAndAfterEach
    with DataFrameSuiteBase
    with LogHelper {

    import spark.implicits._

    val schema: StructType = StructType(
      Seq(
        StructField(
          "pageId",
          dataType = DataTypes.IntegerType,
          nullable = false
        ),
        StructField(
          "revisionId",
          dataType = DataTypes.IntegerType,
          nullable = false
        ),
        StructField("size", dataType = DataTypes.LongType, nullable = false),
        StructField(
          "expectedBucket",
          dataType = DataTypes.LongType,
          nullable = false
        )
      )
    )

    private val values: Seq[(String, Seq[Array[Any]])] = Seq(
      (
        "… when starting with full row",
        Seq(
          Array(0, 1, 100L, 0L), // 100, full row
          Array(0, 2, 50L, 1L), // 150
          Array(0, 3, 40L, 1L), // 190
          Array(0, 4, 20L, 2L), // 210 -> 220 (rebase)
          Array(0, 5, 79L, 2L), // 289 -> 299
          Array(0, 6, 1L, 2L), // 290 -> 300
          Array(0, 7, 10L, 3L) // 300 -> 310 (rebase, indifferent)
        )
      ),
      (
        "… when adding a full row later on",
        Seq(
          Array(0, 1, 50L, 0L),
          Array(0, 2, 40L, 0L),
          Array(0, 3, 100L, 1L),
          Array(0, 4, 20L, 2L)
        )
      ),
      ("… with a single row", Seq(Array(0, 2, 0L, 0L))),
      (
        "… within a pageId",
        Seq(Array(0, 1, 90L, 0L), Array(0, 2, 30L, 1L), Array(1, 3, 70L, 0L))
      )
    )

    values.foreach { case (desc, values) =>
        "bucket" should s"should calculated a bucket index $desc" in {
            val sourceDf = {
                spark.createDataset(
                  values.map(
                    new GenericRowWithSchema(_, schema).asInstanceOf[Row]
                  )
                )(RowEncoder(schema))
            }
            val bucketSize = 100
            val windowSpec = Window.partitionBy("pageId").orderBy("revisionId")
            val augmentedDf = sourceDf
                .coalesce(1)
                .withColumn(
                  "actualBucket",
                  bucket($"size", bucketSize).over(windowSpec)
                )

            augmentedDf.show()

            val rows = augmentedDf.collect()
            rows.foreach { row =>
                val expectedBucket: Long = row.getAs("expectedBucket")
                val actualBucket: Long = row.getAs("actualBucket")
                actualBucket should equal(expectedBucket)
            }
        }

    }

}
