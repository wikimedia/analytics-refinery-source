package org.wikimedia.analytics.refinery.job.mediawikidumper

import scala.util.matching.Regex

import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{
    OutputWriter,
    OutputWriterFactory
}
import org.apache.spark.sql.execution.datasources.binaryfile.BinaryFileFormat
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{DataTypes, StructType}

/** A custom binary file format.
  *
  * Expects at least a column named "value" of type array of bytes and write the
  * data as is.
  *
  * Allows to customize the output file path with placeholders
  * (`$OTHER_COLUMN_NAME`).
  */
class WmfBinaryFileFormat
    extends BinaryFileFormat
    with DataSourceRegister
    with Serializable {

    private val valueType = DataTypes.createArrayType(DataTypes.ByteType)
    private val valueField = "value"

    override def shortName: String = "wmf-binary"

    override def toString: String = "WMF Binary"

    override def prepareWrite(
        sparkSession: SparkSession,
        job: Job,
        options: Map[String, String],
        schema: StructType
    ): OutputWriterFactory = {
        val valueIndex = verifySchema(schema)
        val extractorsByName = {
            schema
                .fields
                .zipWithIndex
                .filter(_._1.name != valueField)
                .map(fieldWithIndex => {
                    fieldWithIndex._1.name ->
                        ((row: InternalRow) => {
                            row.get(
                                  fieldWithIndex._2,
                                  fieldWithIndex._1.dataType
                                )
                                .toString
                        })
                })
                .toMap
        }
        val validFieldNames = extractorsByName.keys.mkString("|")
        val placeholderExpression = new Regex(s"\\$$\\{($validFieldNames)}")

        val filenameReplacement: Option[InternalRow => String] = options
            .get("filename-replacement")
            .map { replacement => row: InternalRow =>
                {
                    placeholderExpression.replaceAllIn(
                      replacement,
                      m => {
                          val fieldNameToBeReplaced = m.group(1)
                          extractorsByName(fieldNameToBeReplaced)(row)
                      }
                    )
                }
            }

        new OutputWriterFactory {
            override def newInstance(
                path: String,
                dataSchema: StructType,
                context: TaskAttemptContext
            ): OutputWriter = {
                val pathProvider: InternalRow => String = {
                    val basePath = {
                        new Path(path)
                            .getParent // Assume "path" is a required option
                    }

                    filenameReplacement match {
                        case Some(replacement) =>
                            row => new Path(basePath, replacement(row)).toString
                        case None =>
                            _ => path
                    }
                }

                new BinaryFileOutputWriter(valueIndex, pathProvider, context)
            }

            override def getFileExtension(context: TaskAttemptContext): String = {
                options.getOrElse("filename-extension", ".bin")
            }
        }
    }

    private def verifySchema(schema: StructType): Int = {
        schema.fields.zipWithIndex.find(_._1.name == valueField) match {
            case Some((valueField, _)) if valueField.dataType != valueType =>
                throw new IllegalArgumentException(
                  s"$this found unexpected value type ${valueField.dataType}"
                )
            case Some((_, index)) =>
                index
            case None =>
                throw new IllegalArgumentException(
                  s"$this expects a field 'value' of type byte array"
                )
        }
    }

    private class BinaryFileOutputWriter(
        valueIndex: Int,
        pathProvider: InternalRow => String,
        context: TaskAttemptContext
    ) extends OutputWriter {

        private var outputStream: FSDataOutputStream = _

        override def write(row: InternalRow): Unit = {
            if (outputStream == null) {
                val path = pathProvider(row)
                val fsPath = new Path(path)
                val fs = fsPath.getFileSystem(context.getConfiguration)

                outputStream = fs.create(fsPath, true)
            }

            row.getArray(valueIndex)
                .foreach(
                  DataTypes.ByteType,
                  (_, byte) => outputStream.write(byte.asInstanceOf[Byte])
                )
        }

        override def close(): Unit = {
            if (outputStream != null) {
                outputStream.close()
            }
        }
    }
}
