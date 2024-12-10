package org.wikimedia.analytics.refinery.job.mediawikidumper

import java.time.Instant

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

case class RevisionRange(
    minRevisionTimestamp: Instant,
    maxRevisionTimestamp: Instant
)

object RevisionRange {
    val dataType = StructType(
      Seq(
        StructField(
          "minRevisionTimestamp",
          dataType = DataTypes.TimestampType,
          nullable = false
        ),
        StructField(
          "maxRevisionTimestamp",
          dataType = DataTypes.TimestampType,
          nullable = false
        )
      )
    )
}
