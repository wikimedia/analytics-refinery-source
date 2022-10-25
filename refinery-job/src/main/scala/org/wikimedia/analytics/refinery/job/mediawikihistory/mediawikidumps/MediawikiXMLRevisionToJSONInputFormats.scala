package org.wikimedia.analytics.refinery.job.mediawikihistory.mediawikidumps

import org.apache.hadoop.io.{LongWritable, Text}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization


/**
 * Abstract class facilitating the creation of XML-To-JSON InputFormat
 * with different keys.
 * The input-format generates JSON-revision as value
 */
abstract class MediawikiXMLRevisionToJSONInputFormatNoKey
  extends MediawikiXMLRevisionInputFormat[LongWritable, Text, MediawikiObjectsMapsFactory](
    new MediawikiObjectsMapsFactory
  ) {

  // Needed for json serialization
  implicit val formats = DefaultFormats

  override def newKey(): LongWritable = new LongWritable()

  override def newValue(): Text = new Text()
  override def setValue(value: Text, rev: MediawikiObjectsMapsFactory#MwRev): Unit = {
    value.set(Serialization.write(rev.asInstanceOf[mwObjectsFactory.MwRev].m))
  }
}

/**
 * XML-To-JSON InputFormat with file-offset as key and JSON-revision as value
 */
class MediawikiXMLRevisionToJSONInputFormat
  extends  MediawikiXMLRevisionToJSONInputFormatNoKey {
  override def setKey(key: LongWritable, pos: Long, rev: MediawikiObjectsMapsFactory#MwRev): Unit = {
    key.set(pos)
  }
}

/**
 * XML-To-JSON InputFormat with page_id as key and JSON-revision as value
 */
class MediawikiXMLRevisionToJSONInputFormatPageIdKey
  extends  MediawikiXMLRevisionToJSONInputFormatNoKey {
  override def setKey(key: LongWritable, pos: Long, rev: MediawikiObjectsMapsFactory#MwRev): Unit = {
    key.set(rev.getPage.getId)
  }
}


/**
 * Abstract class facilitating the creation of XML-To-JSON InputFormat
 * with different keys.
 * The input-format generates flat-JSON-revision as value
 */
abstract class MediawikiRevisionXMLToFlatJSONInputFormatNoKey
  extends MediawikiXMLRevisionInputFormat
    [LongWritable, Text, MediawikiObjectsMapsFactory](new MediawikiObjectsMapsFactory) {

  // Needed for json serialization
  implicit val formats = DefaultFormats

  def flatten_map_rec(m: Map[String, Any], r: Map[String, Any], header: String): Map[String, Any] = {
    m.foldLeft(r)((fm, kv) => {
      val (k, v) = kv
      if (! v.isInstanceOf[Map[_, _]])
        fm + ((header + k) -> v)
      else
        flatten_map_rec(v.asInstanceOf[Map[String, Any]], fm, k + "_")
    })
  }

  override def newKey(): LongWritable = new LongWritable()

  override def newValue(): Text = new Text()
  override def setValue(value: Text, rev: MediawikiObjectsMapsFactory#MwRev): Unit = {
    value.set(Serialization.write(flatten_map_rec(rev.asInstanceOf[mwObjectsFactory.MwRev].m, Map(), "")))
  }
}

/**
 * XML-To-JSON InputFormat with file-offset as key and flat-JSON-revision as value
 */
class MediawikiRevisionXMLToFlatJSONInputFormat
  extends MediawikiRevisionXMLToFlatJSONInputFormatNoKey {

  override def setKey(key: LongWritable, pos: Long, rev: MediawikiObjectsMapsFactory#MwRev): Unit = {
    key.set(pos)
  }
}

/**
 * XML-To-JSON InputFormat with page_id as key and flat-JSON-revision as value
 */
class MediawikiRevisionXMLToFlatJSONInputFormatPageIdKey
  extends MediawikiRevisionXMLToFlatJSONInputFormatNoKey {

  override def setKey(key: LongWritable, pos: Long, rev: MediawikiObjectsMapsFactory#MwRev): Unit = {
    key.set(rev.getPage.getId)
  }
}