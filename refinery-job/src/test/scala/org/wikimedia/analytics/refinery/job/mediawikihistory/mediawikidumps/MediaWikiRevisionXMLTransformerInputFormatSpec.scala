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
      """{"sha1":"","format":"","timestamp":"2001-01-15T13:15:00Z","model":"","page":{"redirect":"Redirect title","restrictions":["edit=sysop:move=sysop"],"id":1,"wiki":"mediawiki","title":"Page title","namespace":1},"text":"\n                A bunch of [[text]] here.\n                With new lines for instance.\n            ","comment_is_visible":true,"bytes":100,"id":-1,"comment":"I have just one thing to say!","user":{"id":-1,"text":"Foobar"},"user_is_visible":true,"minor":true,"content_is_visible":true}""",
      """{"sha1":"","format":"","timestamp":"2001-01-15T13:10:27Z","model":"","page":{"redirect":"Redirect title","restrictions":["edit=sysop:move=sysop"],"id":1,"wiki":"mediawiki","title":"Page title","namespace":1},"text":"An earlier [[revision]].","comment_is_visible":true,"bytes":24,"id":-1,"comment":"new!","user":{"text":"10.0.0.2"},"user_is_visible":true,"minor":false,"content_is_visible":true}""",
      """{"sha1":"","format":"","timestamp":"2001-01-15T14:03:00Z","model":"","page":{"redirect":"","restrictions":[],"id":-1,"wiki":"mediawiki","title":"Talk:Page title","namespace":-1},"text":"WHYD YOU LOCK PAGE??!!! i was editing that jerk","comment_is_visible":true,"bytes":47,"id":-1,"comment":"hey","user":{"text":"10.0.0.2"},"user_is_visible":true,"minor":false,"content_is_visible":true}""",
      """{"sha1":"61o9wqbehiqpmke7163b1675fic2cri","format":"text/x-wiki","timestamp":"2006-12-13T03:07:14Z","model":"wikitext","page":{"redirect":"","restrictions":[],"id":45046,"wiki":"mediawiki","title":"User:Ryulong - inspired example with deleted parts","namespace":2},"text":"{{babel|en}}","comment_is_visible":true,"bytes":12,"id":266092,"comment":"New page: {{babel|en}}","user":{"id":6629,"text":"Ryulong"},"user_is_visible":true,"minor":false,"content_is_visible":true}""",
      """{"sha1":"phoiac9h4m842xq45sp7s6u21eteeq1","format":"text/x-wiki","timestamp":"2007-03-27T11:27:44Z","model":"wikitext","parent_id":266092,"page":{"redirect":"","restrictions":[],"id":45046,"wiki":"mediawiki","title":"User:Ryulong - inspired example with deleted parts","namespace":2},"text":"","comment_is_visible":true,"bytes":0,"id":360821,"comment":"Removing all content from page","user":{"id":-1,"text":""},"user_is_visible":false,"minor":false,"content_is_visible":true}""",
      """{"sha1":"","format":"text/x-wiki","timestamp":"2009-08-31T10:30:39Z","model":"wikitext","parent_id":360821,"page":{"redirect":"","restrictions":[],"id":45046,"wiki":"mediawiki","title":"User:Ryulong - inspired example with deleted parts","namespace":2},"text":"","comment_is_visible":false,"bytes":26,"id":1714215,"comment":"","user":{"text":"67.159.41.117"},"user_is_visible":true,"minor":false,"content_is_visible":false}""",
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
    val reader = inputFormat.createRecordReader(split, context)

    reader.initialize(split, context)

    var revisionsCount = 0
    val expectedRevisions = List(
      // order: sha1,format,timestamp,model,parent_id,page_title,text,user_text,page_id,comment_is_visible,bytes,id,page_namespace,page_redirect,comment,page_restrictions,page_wiki,mediawiki,user_is_visible,minor,content_is_visible}""",
      """{"sha1":"","format":"","timestamp":"2001-01-15T13:15:00Z","model":"","page_title":"Page title","text":"\n                A bunch of [[text]] here.\n                With new lines for instance.\n            ","user_id":-1,"user_text":"Foobar","page_id":1,"comment_is_visible":true,"bytes":100,"id":-1,"page_namespace":1,"page_redirect":"Redirect title","comment":"I have just one thing to say!","page_restrictions":["edit=sysop:move=sysop"],"page_wiki":"mediawiki","user_is_visible":true,"minor":true,"content_is_visible":true}""",
      """{"sha1":"","format":"","timestamp":"2001-01-15T13:10:27Z","model":"","page_title":"Page title","text":"An earlier [[revision]].","user_text":"10.0.0.2","page_id":1,"comment_is_visible":true,"bytes":24,"id":-1,"page_namespace":1,"page_redirect":"Redirect title","comment":"new!","page_restrictions":["edit=sysop:move=sysop"],"page_wiki":"mediawiki","user_is_visible":true,"minor":false,"content_is_visible":true}""",
      """{"sha1":"","format":"","timestamp":"2001-01-15T14:03:00Z","model":"","page_title":"Talk:Page title","text":"WHYD YOU LOCK PAGE??!!! i was editing that jerk","user_text":"10.0.0.2","page_id":-1,"comment_is_visible":true,"bytes":47,"id":-1,"page_namespace":-1,"page_redirect":"","comment":"hey","page_restrictions":[],"page_wiki":"mediawiki","user_is_visible":true,"minor":false,"content_is_visible":true}""",
      """{"sha1":"61o9wqbehiqpmke7163b1675fic2cri","format":"text/x-wiki","timestamp":"2006-12-13T03:07:14Z","model":"wikitext","page_title":"User:Ryulong - inspired example with deleted parts","text":"{{babel|en}}","user_id":6629,"user_text":"Ryulong","page_id":45046,"comment_is_visible":true,"bytes":12,"id":266092,"page_namespace":2,"page_redirect":"","comment":"New page: {{babel|en}}","page_restrictions":[],"page_wiki":"mediawiki","user_is_visible":true,"minor":false,"content_is_visible":true}""",
      """{"sha1":"phoiac9h4m842xq45sp7s6u21eteeq1","format":"text/x-wiki","timestamp":"2007-03-27T11:27:44Z","model":"wikitext","parent_id":266092,"page_title":"User:Ryulong - inspired example with deleted parts","text":"","user_id":-1,"user_text":"","page_id":45046,"comment_is_visible":true,"bytes":0,"id":360821,"page_namespace":2,"page_redirect":"","comment":"Removing all content from page","page_restrictions":[],"page_wiki":"mediawiki","user_is_visible":false,"minor":false,"content_is_visible":true}""",
      """{"sha1":"","format":"text/x-wiki","timestamp":"2009-08-31T10:30:39Z","model":"wikitext","parent_id":360821,"page_title":"User:Ryulong - inspired example with deleted parts","text":"","user_text":"67.159.41.117","page_id":45046,"comment_is_visible":false,"bytes":26,"id":1714215,"page_namespace":2,"page_redirect":"","comment":"","page_restrictions":[],"page_wiki":"mediawiki","user_is_visible":true,"minor":false,"content_is_visible":false}""",
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