package org.wikimedia.analytics.refinery.spark.io.xml

import java.nio.charset.StandardCharsets

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.{CompressionCodecFactory, SplittableCompressionCodec}
import org.apache.hadoop.io.{DataOutputBuffer, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.wikimedia.analytics.refinery.spark.io.xml.{ByteMatcher, SeekableInputStream, SeekableSplitCompressedInputStream}

/**
  * This class reads a HDFS file looking for records as strings between start and end tags
  * (configured using betweentagsinputformat.record.starttag and betweentagsinputformat.record.endtag),
  * keeping the tags in the record by default (set betweentagsinputformat.record.keeptags = false
  * to remove them).
  * Keys are the position in the file and values are the text in between start and end tags.
  */
object BetweenTagsInputFormat {

  /**
   * This property allows to either keep or discard the start and end tags
   * in every read record.
   */
  val KeepTagsPropertyName = "betweentagsinputformat.record.keeptags"
  /**
   * The default value of the [[KeepTagsPropertyName]] property - true
   * This means that by default tags are kept within the read records.
   */
  val KeepTagsDefaultValue = true
  /**
   * The property defining the start tag of records. Mandatory.
   */
  val StartTagPropertyName = "betweentagsinputformat.record.starttag"
  /**
   * The property defining the end tag of records. Mandatory.
   */
  val EndTagPropertyName = "betweentagsinputformat.record.endtag"
}

class BetweenTagsInputFormat extends FileInputFormat[LongWritable, Text] {

  override def createRecordReader(
    inputSplit: InputSplit,
    context: TaskAttemptContext
  ): RecordReader[LongWritable, Text] = {
    val keepTags = context.getConfiguration.getBoolean(
      BetweenTagsInputFormat.KeepTagsPropertyName,
      BetweenTagsInputFormat.KeepTagsDefaultValue
    )
    val startTag = context.getConfiguration.get(BetweenTagsInputFormat.StartTagPropertyName)
    val endTag = context.getConfiguration.get(BetweenTagsInputFormat.EndTagPropertyName)
    new BetweenTagsRecordReader(keepTags, startTag, endTag)
  }

  /**
    * Defines file-splitability based on compression codec.
    */
  protected override def isSplitable(context: JobContext, file: Path): Boolean = {
    val codec = new CompressionCodecFactory(context.getConfiguration).getCodec(file)
    if (null == codec) true else codec.isInstanceOf[SplittableCompressionCodec]
  }

  /**
    * Actually reading splits and extracting content between start and end tags
    * @param keepTags whether to include start and end tags in the read records or not
    * @param startTag the tag marking the beginning of a record to be read
    * @param endTag the tag marking the end of a record to be read
    */
  private class BetweenTagsRecordReader(
    keepTags: Boolean,
    startTag: String,
    endTag: String
  ) extends RecordReader[LongWritable, Text] {

    private val startTagArray = startTag.getBytes(StandardCharsets.UTF_8)
    private val endTagArray = endTag.getBytes(StandardCharsets.UTF_8)
    private val endTagLength = endTagArray.length

    var key: LongWritable = null.asInstanceOf[LongWritable]
    var value: Text = null.asInstanceOf[Text]

    private var inputStream: SeekableInputStream = null.asInstanceOf[SeekableInputStream]
    private var matcher: ByteMatcher = null.asInstanceOf[ByteMatcher]

    private var inputStart: Long = null.asInstanceOf[Long]
    private var inputEnd: Long = null.asInstanceOf[Long]

    // Used to store in-between-tags content
    private var contentBuffer: DataOutputBuffer = null.asInstanceOf[DataOutputBuffer]

    /**
      * Initializes the reader
      * @param inputSplit the input split to read
      * @param context the task context
      */
    override def initialize(
      inputSplit: InputSplit,
      context: TaskAttemptContext
    ): Unit = {
      val fileSplit = inputSplit.asInstanceOf[FileSplit]
      val configuration = context.getConfiguration

      key = new LongWritable()
      value = new Text()

      inputStream = SeekableInputStream(
        fileSplit,
        fileSplit.getPath.getFileSystem(configuration),
        new CompressionCodecFactory(configuration)
      )

      matcher = new ByteMatcher(inputStream)

      // Adding 1 to end to make sure we don't miss end-tags ending splits
      inputStream match {
        case SeekableSplitCompressedInputStream(sin) =>
          inputStart = sin.getAdjustedStart
          inputEnd = sin.getAdjustedEnd + 1
        case _ =>
          inputStart = fileSplit.getStart
          inputEnd = fileSplit.getStart + fileSplit.getLength + 1
      }

      contentBuffer = new DataOutputBuffer()
    }

    override def getCurrentKey: LongWritable = key
    override def getCurrentValue: Text = value

    override def nextKeyValue(): Boolean = {
      // Look for next startTag - restrict research to split
      if (matcher.getPos < inputEnd && matcher.readUntilMatch(startTagArray, inputEnd)) {
        contentBuffer.reset()
        // Add startTag to the buffer if needed (removed by matcher by default)
        if (keepTags) {
          contentBuffer.write(startTagArray)
        }
        val recordStart = matcher.getPos
        // Look for next endTag without end restriction
        if (matcher.readUntilMatch(endTagArray, Some(contentBuffer))) {
          key.set(recordStart)
          // Remove endTag at the end of the match if needed (kept by matcher by default)
          val bufferLength  = contentBuffer.getLength
          val contentLength = if (keepTags) bufferLength else bufferLength - endTagLength
          value.set(contentBuffer.getData.take(contentLength))
          true
        } else {
          // EOF before endTag
          false
        }
      } else {
        // No startTag in split
        false
      }
    }

    def getPos: Long = matcher.getPos
    override def getProgress: Float = {
      if (inputEnd == inputStart) 1.0f
      else (getPos - inputStart).asInstanceOf[Float] / (inputEnd - inputEnd).asInstanceOf[Float]
    }

    override def close(): Unit = inputStream.close()
  }
}
