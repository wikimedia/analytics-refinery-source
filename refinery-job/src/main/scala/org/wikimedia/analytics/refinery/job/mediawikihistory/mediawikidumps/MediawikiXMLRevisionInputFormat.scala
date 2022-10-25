package org.wikimedia.analytics.refinery.job.mediawikihistory.mediawikidumps

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.{CompressionCodecFactory, SplittableCompressionCodec}
import org.apache.hadoop.io.{DataOutputBuffer, Writable, WritableComparable}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.codehaus.stax2.XMLStreamReader2
import org.wikimedia.analytics.refinery.spark.io.xml.{ByteMatcher, SeekableInputStream, SeekableSplitCompressedInputStream}

import java.io.ByteArrayInputStream

/**
 * Modified from DBPedia distibuted extraction framework (https://github.com/dbpedia/distributed-extraction-framework)
 *
 * Abstract Hadoop InputFormat parsing Wikimedia XML-dump files into revision-objects,
 * adding page data as fields in the revision.
 *
 * The objects used to store revisions and sub-fields User and Page need to be
 * implemented through a factory extending [[MediawikiObjectsFactory]].
 *
 * The Key and Value types of the InputFormat are abstract, and key/value generation
 * functions are expected by extending [[KeyValueFactory]].
 * This trait also allows to filter out revisions from the returned set by implementing
 * the [[KeyValueFactory.filterOut()]] function (doesn't filter out any revision by default).
 *
 * @param mwObjectsFactory The mediawiki-objects factory implementation to use for
 *                         mediawiki-objects
 * @tparam K The type of the key to be generated
 * @tparam V The type of the value to be generated
 * @tparam MwObjectsFactory The type of the [[MediawikiObjectsFactory]] implementation
 *                          to use for mediawiki-objects
 */
abstract class MediawikiXMLRevisionInputFormat
  [
    K<: WritableComparable[K],
    V<: Writable,
    MwObjectsFactory <: MediawikiObjectsFactory
  ](
    val mwObjectsFactory: MwObjectsFactory
  ) extends FileInputFormat[K, V] with KeyValueFactory[K, V, MwObjectsFactory] {

  private val LOG = LogFactory.getLog(
    classOf[MediawikiXMLRevisionInputFormat[K, V, MwObjectsFactory]])

  /**
   * Defines file-splitability based on compression codec.
   */
  protected override def isSplitable(context: JobContext, file: Path): Boolean = {
    val codec = new CompressionCodecFactory(context.getConfiguration).getCodec(file)
    if (null == codec) true else codec.isInstanceOf[SplittableCompressionCodec]
  }

  /**
   * Instantiate a new RecordReader
   */
  override def createRecordReader(
    genericSplit: InputSplit,
    context: TaskAttemptContext
  ): RecordReader[K, V] = {
    val split = genericSplit.asInstanceOf[FileSplit]
    LOG.info("getRecordReader start.....split=" + split)
    context.setStatus(split.toString)

    new MediawikiXMLRevisionRecordReader(split, context, mwObjectsFactory)
  }


  /**
   * Record reader actually reading splits and parsing XML.
   * Given the hierarchical structure of XML (revisions elements are
   * in page elements), the page element is stored in context while
   * reading revisions, and updated when a new page is reached.
   * Also, the reader starts to read at the beginning of a new page
   * in its split, letting the end of the previous page to the reader
   * having started to read the page.
   * @param split The split to read
   * @param context The job context
   * @param mwObjectsFactory The [[MediawikiObjectsFactory]] implementation
   *                         to use for mediawiki-objects
   */
  private class MediawikiXMLRevisionRecordReader(
    split: FileSplit,
    context: TaskAttemptContext,
    mwObjectsFactory: MwObjectsFactory
  ) extends RecordReader[K, V] {

    private var key: K = null.asInstanceOf[K]
    private var value: V = null.asInstanceOf[V]

    private val conf = context.getConfiguration

    private val contentBuffer = new DataOutputBuffer()

    private var isFirstAfterInit = false

    // Assuming files will contain a wiki name in them, as in regular WMF dumps.
    // Wikiname can contain _, but no -, while files use - as delimiter.
    private val wiki = split
      .getPath
      .getName
      .split("[\\-\\.,\\+]")
      .find(_.matches("^[_a-zA-Z0-9]+wik[it][_a-zA-Z0-9]*$"))

    // Initialize inputstream and byteMatcher
    private val inputStream = SeekableInputStream(
      split,
      split.getPath.getFileSystem(conf),
      new CompressionCodecFactory(conf)
    )
    private val matcher = new ByteMatcher(inputStream)

    // Define reused ByteArrayInputStream and XMLStreamReader2
    private var byteArrayInputStream: ByteArrayInputStream = null
    private var xmlStreamReader2: XMLStreamReader2 = null

    // Instantiate MediawikiXMLParser and mediawiki-Page context
    private val mediaWikiXMLParser = new MediawikiXMLParser(
      mwObjectsFactory,
      wiki.getOrElse("UNKNOWN_WIKI")
    )
    private var pageMetaData: mediaWikiXMLParser.mwObjectsFactory.MwPage =
      null.asInstanceOf[mediaWikiXMLParser.mwObjectsFactory.MwPage]

    // Define start and end
    private val (start, end) = {
      inputStream match {
        case SeekableSplitCompressedInputStream(sin) =>
          (sin.getAdjustedStart, sin.getAdjustedEnd + 1)
        case _ =>
          (split.getStart, split.getStart + split.getLength)
      }
    }

    // XML patterns to match in order to extract interesting portions
    private val revisionBeginPattern = "<revision>".getBytes("UTF-8")
    private val revisionEndPattern = "</revision>".getBytes("UTF-8")
    private val pageBeginPattern = "<page>".getBytes("UTF-8")
    private val pageEndPattern = "</page>".getBytes("UTF-8")

    // Trivial functions override
    override def getCurrentKey: K = key
    override def getCurrentValue: V = value
    def getPos: Long = matcher.getPos
    override def close() = inputStream.close()
    override def getProgress: Float = {
      if (end == start) {
        1.0f
      } else {
        (getPos - start).asInstanceOf[Float] / (end - start).asInstanceOf[Float]
      }
    }


    /**
     * Initialize the record reader by reading the split up to the first page
     * encountered in split and set the page-context value.
     * If no page is read on that split, page-context stays null.
     */
    override def initialize(genericInputSplit: InputSplit, context: TaskAttemptContext) = {
      contentBuffer.reset()
      // Loop until page context initialized - Restrict research in current split
      while (pageMetaData == null && matcher.getPos < end) {
        // Look for page beginning
        if (matcher.getPos < end && matcher.readUntilMatch(pageBeginPattern, end)) {
          // Initialize contentBuffer with page beginning tag (removed by parsing)
          contentBuffer.write(pageBeginPattern)
          // Read until next revision beginning (could be in next split)
          // keeping page metadata in between <page> and <revision> tags.
          if (matcher.readUntilMatch(revisionBeginPattern, Some(contentBuffer))) {
            // Get buffer data removing revision tag
            val contentStr = contentBuffer.getData.take(contentBuffer.getLength - revisionBeginPattern.length)
            // Convert metadata xml (adding page end tag) to a map
            byteArrayInputStream = new ByteArrayInputStream(contentStr ++ pageEndPattern)
            xmlStreamReader2 = mediaWikiXMLParser.initializeXmlStreamReader(byteArrayInputStream)
            pageMetaData = mediaWikiXMLParser.parsePage(xmlStreamReader2)
            // Clean buffer
            contentBuffer.reset()
            isFirstAfterInit = true
          }
        }
      }
    }

    /**
     * Reads the next XML-revision in the split and set the key/value accordingly.
     * In case of a change of page and the new page is still in the current split,
     * the page-context is updated so that the future revisions read contain
     * correct page information.
     * This function recurse over itself in case the revision read is to be filtered
     * out (see [[KeyValueFactory.filterOut()]]).
     * @return true if a revision has been read (key and value have been updated),
     *         or false otherwise (no page-context defined, or no revision left to
     *         be read for the split, or error reading split).
     */
    override def nextKeyValue(): Boolean = {
      // Initialize key and value
      if (key == null) key = newKey()
      if (value == null) value = newValue()

      // If pageMetaData not initialized, no page in this split - return false
      (pageMetaData != null) &&
        // Return true if a revision is still to be read in the current split
        findNextRevision() &&
        // Return true if a revision has been successfully read (key and value
        // having been updated (note: revisions may have been filtered out)
        readNextRevision()
    }

    // Read stream until next revision beginning (could be in next split)
    // keeping content between previous revision and next update page if needed
    // Note: Initialization enforces stream to be at beginning of a revision,
    // so no need to do it again in that case.
    private def findNextRevision(): Boolean = {
      // Use a variable to facilitate returning
      var result = true

      // Check if we already are after the end of the split to prevent starting
      // reading new pages. Still finish reading the current one.
      val afterEnd = matcher.getPos >= end

      if (!isFirstAfterInit && matcher.readUntilMatch(revisionBeginPattern, Some(contentBuffer))) {
        // Get content in between revisions
        val contentStr = contentBuffer.getData.take(contentBuffer.getLength)
        // Look for page begin tag in data in between revisions
        val pagePos = contentStr.indexOfSlice(pageBeginPattern)
        // If there is a new page
        if (pagePos >= 0) {
          // If we already are after the end of our split, stop reading
          if (afterEnd) {
            result = false
          } else {
            // update page context getting buffer data starting from pagePos,
            // removing revision begin tag and adding page end tag
            val toRead = contentStr.slice(pagePos, contentStr.length - revisionBeginPattern.length) ++ pageEndPattern
            byteArrayInputStream = new ByteArrayInputStream(toRead)
            xmlStreamReader2 = mediaWikiXMLParser.initializeXmlStreamReader(byteArrayInputStream)
            pageMetaData = mediaWikiXMLParser.parsePage(xmlStreamReader2)
          }
        }
        // Clean buffer
        contentBuffer.reset()
      }
      result
    }

    // Read stream until end-of-revision (could be in next split), keeping revision content.
    // Note: Can recurse over nextKeyValue in case the found revision is to filtered out.
    private def readNextRevision(): Boolean = {
      // Initialize contentBuffer with revision tag (removed by previous parsing)
      contentBuffer.write(revisionBeginPattern)
      isFirstAfterInit = false

      // Read until end of revision (could be in next split) keeping revision content
      if (matcher.readUntilMatch(revisionEndPattern, Some(contentBuffer))) {
        byteArrayInputStream = new ByteArrayInputStream(contentBuffer.getData.take(contentBuffer.getLength))
        xmlStreamReader2 = mediaWikiXMLParser.initializeXmlStreamReader(byteArrayInputStream)
        val rev = mediaWikiXMLParser.parseRevision(xmlStreamReader2, pageMetaData)
        contentBuffer.reset()
        // Recurse over nextKeyValue if the found revision is to be filtered out of result set
        if (filterOut(rev)) {
          nextKeyValue()
        } else {
          // Update key and value and return true
          setKey(key, matcher.getPos, rev)
          setValue(value, rev)
          true
        }
      } else {
        // Clean contentBuffer and stop
        contentBuffer.reset()
        false
      }
    }

  }
}
