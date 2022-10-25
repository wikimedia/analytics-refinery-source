package org.wikimedia.analytics.refinery.job.mediawikihistory.mediawikidumps

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.util.ReflectionUtils
import org.scalatest.{FlatSpec, Matchers}

import java.io.File


class MediaWikiRevisionXMLTransformerInputFormatSpec extends FlatSpec with Matchers {

  "MediawikiXMLRevisionToJSONInputFormat" should " read XML dump file in json revisions " in {

    val hadoopConf :Configuration = new Configuration(false)
    hadoopConf.set("fs.default.name", "file:///")

    val testFilePath = getClass.getResource("/mediawiki-dump-test.xml").getFile
    val path = new Path(testFilePath)
    val split = new FileSplit(path, 0, new File(testFilePath).length(), null)

    val inputFormat = ReflectionUtils.newInstance(classOf[MediawikiXMLRevisionToJSONInputFormat], hadoopConf)
    val context = new TaskAttemptContextImpl(hadoopConf, new TaskAttemptID())
    val reader= inputFormat.createRecordReader(split, context)

    reader.initialize(split, context)

    var revisionsCount = 0
    val expectedRevisions = List(
      "{\"sha1\":\"\",\"format\":\"\",\"timestamp\":\"2001-01-15T13:15:00Z\",\"model\":\"\",\"parent_id\":-1,\"page\":{\"redirect\":\"Redirect title\",\"restrictions\":[\"edit=sysop:move=sysop\"],\"id\":1,\"wiki\":\"mediawiki\",\"title\":\"Page title\",\"namespace\":1},\"text\":\"\\n                A bunch of [[text]] here.\\n                With new lines for instance.\\n            \",\"bytes\":100,\"id\":-1,\"comment\":\"I have just one thing to say!\",\"user\":{\"id\":-1,\"text\":\"Foobar\"},\"minor\":true}",
      "{\"sha1\":\"\",\"format\":\"\",\"timestamp\":\"2001-01-15T13:10:27Z\",\"model\":\"\",\"parent_id\":-1,\"page\":{\"redirect\":\"Redirect title\",\"restrictions\":[\"edit=sysop:move=sysop\"],\"id\":1,\"wiki\":\"mediawiki\",\"title\":\"Page title\",\"namespace\":1},\"text\":\"An earlier [[revision]].\",\"bytes\":24,\"id\":-1,\"comment\":\"new!\",\"user\":{\"id\":-1,\"text\":\"10.0.0.2\"},\"minor\":false}",
      "{\"sha1\":\"\",\"format\":\"\",\"timestamp\":\"2001-01-15T14:03:00Z\",\"model\":\"\",\"parent_id\":-1,\"page\":{\"redirect\":\"\",\"restrictions\":[],\"id\":-1,\"wiki\":\"mediawiki\",\"title\":\"Talk:Page title\",\"namespace\":-1},\"text\":\"WHYD YOU LOCK PAGE??!!! i was editing that jerk\",\"bytes\":47,\"id\":-1,\"comment\":\"hey\",\"user\":{\"id\":-1,\"text\":\"10.0.0.2\"},\"minor\":false}"
    )
    while (reader.nextKeyValue()) {
      val key = reader.getCurrentKey
      val value = reader.getCurrentValue
      key should not be null
      value should not be null
      value.toString shouldEqual expectedRevisions(revisionsCount)
      revisionsCount += 1
    }
  }

  "MediawikiRevisionXMLToFlatJSONInputFormat" should " read XML dump file in flatten json revisions " in {

    val hadoopConf :Configuration = new Configuration(false)
    hadoopConf.set("fs.default.name", "file:///")

    val testFilePath = getClass.getResource("/mediawiki-dump-test.xml").getFile
    val path = new Path(testFilePath)
    val split = new FileSplit(path, 0, new File(testFilePath).length(), null)

    val inputFormat = ReflectionUtils.newInstance(classOf[MediawikiRevisionXMLToFlatJSONInputFormat], hadoopConf)
    val context = new TaskAttemptContextImpl(hadoopConf, new TaskAttemptID())
    val reader= inputFormat.createRecordReader(split, context)

    reader.initialize(split, context)

    var revisionsCount = 0
    val expectedRevisions = List(
      "{\"sha1\":\"\",\"format\":\"\",\"timestamp\":\"2001-01-15T13:15:00Z\",\"model\":\"\",\"parent_id\":-1,\"page_title\":\"Page title\",\"text\":\"\\n                A bunch of [[text]] here.\\n                With new lines for instance.\\n            \",\"user_id\":-1,\"user_text\":\"Foobar\",\"page_id\":1,\"bytes\":100,\"id\":-1,\"page_namespace\":1,\"page_redirect\":\"Redirect title\",\"comment\":\"I have just one thing to say!\",\"page_restrictions\":[\"edit=sysop:move=sysop\"],\"page_wiki\":\"mediawiki\",\"minor\":true}",
      "{\"sha1\":\"\",\"format\":\"\",\"timestamp\":\"2001-01-15T13:10:27Z\",\"model\":\"\",\"parent_id\":-1,\"page_title\":\"Page title\",\"text\":\"An earlier [[revision]].\",\"user_id\":-1,\"user_text\":\"10.0.0.2\",\"page_id\":1,\"bytes\":24,\"id\":-1,\"page_namespace\":1,\"page_redirect\":\"Redirect title\",\"comment\":\"new!\",\"page_restrictions\":[\"edit=sysop:move=sysop\"],\"page_wiki\":\"mediawiki\",\"minor\":false}",
      "{\"sha1\":\"\",\"format\":\"\",\"timestamp\":\"2001-01-15T14:03:00Z\",\"model\":\"\",\"parent_id\":-1,\"page_title\":\"Talk:Page title\",\"text\":\"WHYD YOU LOCK PAGE??!!! i was editing that jerk\",\"user_id\":-1,\"user_text\":\"10.0.0.2\",\"page_id\":-1,\"bytes\":47,\"id\":-1,\"page_namespace\":-1,\"page_redirect\":\"\",\"comment\":\"hey\",\"page_restrictions\":[],\"page_wiki\":\"mediawiki\",\"minor\":false}"
    )
    while (reader.nextKeyValue()) {
      val key = reader.getCurrentKey
      val value = reader.getCurrentValue
      key should not be null
      value should not be null
      println(value.toString())
      value.toString shouldEqual expectedRevisions(revisionsCount)
      revisionsCount += 1
    }
  }

  "MediawikiXMLRevisionToJSONInputFormat" should "not return any result for file not containing page" in {

    val hadoopConf :Configuration = new Configuration(false)
    hadoopConf.set("fs.default.name", "file:///")

    val testFilePath = getClass.getResource("/dummy-test-data.txt").getFile
    val path = new Path(testFilePath)
    val split = new FileSplit(path, 0, new File(testFilePath).length(), null)

    val inputFormat = ReflectionUtils.newInstance(classOf[MediawikiXMLRevisionToJSONInputFormat], hadoopConf)
    val context = new TaskAttemptContextImpl(hadoopConf, new TaskAttemptID())
    val reader= inputFormat.createRecordReader(split, context)

    reader.initialize(split, context)

    reader.nextKeyValue shouldEqual false

  }

}