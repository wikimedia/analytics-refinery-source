package org.wikimedia.analytics.refinery.job.mediawikidumper

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Max
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._

/** A window function for assigning buckets.
  *
  * It works around issues that would arise when working integer division
  * (`floor(sum($"size").over(orderByWindowSpec) / bucketSize)`). In particular,
  * it deals with edge cases that are not covered by this approach:
  *
  *   - A row following a filling row (`size = bucketSize`) does not get its own
  *     bucket. For example, if `bucketSize = 100`, two consecutive rows of size
  *     100 and 5 would both since both, 100 and 105 divided by 100 are 1.
  *   - A row following an oversize row (size > bucketSize) does not get its own
  *     bucket. For example, if `bucketSize = 100`, two consecutive rows of size
  *     105 and 5 would both since both, 105 and 115 divided by 100 are 1.
  */
object RebasedSizeBucket {
    def bucket(column: Column, bucketSize: Long): Column = {
        new Column(RebasedSizeBucket(column.expr, lit(bucketSize).expr))
    }

    case class RebasedSizeBucket(value: Expression, bucketSize: Expression)
        extends AggregateWindowFunction {

        private val resultType: DataType = DataTypes.LongType
        private val sum = AttributeReference("sum", resultType)()
        private val bucket = AttributeReference("bucket", resultType)()

        override def dataType: DataType = resultType
        override def children: Seq[Expression] = Seq(value, bucketSize)
        override def prettyName: String = "size_sum"
        override def nullable: Boolean = true

        override lazy val aggBufferAttributes: Seq[AttributeReference] = Seq(
          sum,
          bucket
        )

        private val sumDefaultValue: Literal = Literal.default(resultType)
        private val bucketDefaultValue: Literal = Literal.default(resultType)
        override lazy val initialValues: Seq[Expression] = Seq(
          sumDefaultValue,
          bucketDefaultValue
        )

        // Support pretty SQL string
        override def sql: String = {
            s"$prettyName(${value.sql}, ${bucketSize.sql})"
        }

        override lazy val updateExpressions: Seq[Expression] = {
            // Next bucket excluding current value
            val nextBucket = sum / bucketSize + 1
            // Number of buckets required to hold the current value
            val requiredChildBuckets = value / bucketSize + 1
            // Calculate plain sum without rebase
            val updatedSum = sum + value

            val rebasedSum = If(
              // Does the current value fit in one bucket?
              value > bucketSize,
              // Continue with margin ('fill' required buckets)
              requiredChildBuckets * bucketSize,
              If(
                // Would the current value spill into the next bucket?
                updatedSum > nextBucket * bucketSize,
                // Continue with rebased sum
                nextBucket * bucketSize + value,
                // Continue with plain sum
                updatedSum
              )
            )

            // We want values (0, bucketSize] to be part of a bucket, but rebasedSum % bucketSize returns [0, bucketSize).
            // To Account for that shift, we subtract 1 from rebasedSum to force a full bucket (rebasedSum % bucketSize == 0) into the previous bucket.
            // Floor rounds negative values (in case of an initial row of size 0) towards 0.
            //
            // This could be implemented using the following checks:
            // If sum == 0 -> 0 (initial value)
            // If rebasedSum % bucketSize == 0 (any multiple of bucketSize) -> assign to previous (-1) bucket
            // for example, of the first 100 bytes, the 100th byte would belong to the first in a 100-byte-bucket.
            //
            // The following line results in the same behaviour
            val updatedBucket = Floor((rebasedSum - 1) / bucketSize)
            Seq(rebasedSum, updatedBucket)
        }

        override lazy val evaluateExpression: Expression = bucket

    }
}
