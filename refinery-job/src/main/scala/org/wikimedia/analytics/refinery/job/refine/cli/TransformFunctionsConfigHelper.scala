package org.wikimedia.analytics.refinery.job.refine.cli

import cats.syntax.either._
import io.circe.Decoder
import org.wikimedia.analytics.refinery.core.ReflectUtils
import org.wikimedia.analytics.refinery.job.refine.RefineHelper.TransformFunction
import org.wikimedia.analytics.refinery.job.refine.Refine.log
import org.wikimedia.analytics.refinery.spark.sql.PartitionedDataFrame

trait TransformFunctionsConfigHelper {

    /**
     * Convert from comma separated package.ObjectNames to Object callable apply() TransformFunctions.
     */
    implicit val decodeTransformFunctions: Decoder[Seq[TransformFunction]] = Decoder.decodeString.emap { s =>
        Either.catchNonFatal(
            s.split(",").map { objectName =>
                val transformMirror = ReflectUtils.getStaticMethodMirror(objectName)
                // Lookup the object's apply method as a reflect MethodMirror, and wrap
                // it in a anonymous function that has the signature expected by
                // TableSchemaManager's transformFunction parameter.
                val wrapperFn: TransformFunction = {
                    case (partDf) =>
                        log.debug(s"Applying ${transformMirror.receiver} to ${partDf.partition}")
                        transformMirror(partDf).asInstanceOf[PartitionedDataFrame]
                }
                wrapperFn
            }.toSeq
        ).leftMap(t =>
            throw new RuntimeException(s"Failed parsing '$s' into transform functions.", t)
        )
    }
}
