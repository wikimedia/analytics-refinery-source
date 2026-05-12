package org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

import org.scalatest.{FlatSpec, Matchers}

class TestMediawikiEvent extends FlatSpec with Matchers {

  "MediawikiEventRevisionDetails" should "correctly extract revisionDeletedParts from flag" in {

    MediawikiEventRevisionDetails.getRevDeletedParts(1) should equal(Seq("text"))
    MediawikiEventRevisionDetails.getRevDeletedParts(2) should equal(Seq("comment"))
    MediawikiEventRevisionDetails.getRevDeletedParts(3) should equal(Seq("text", "comment"))
    MediawikiEventRevisionDetails.getRevDeletedParts(4) should equal(Seq("user"))
    MediawikiEventRevisionDetails.getRevDeletedParts(5) should equal(Seq("text", "user"))
    MediawikiEventRevisionDetails.getRevDeletedParts(6) should equal(Seq("comment", "user"))
    MediawikiEventRevisionDetails.getRevDeletedParts(7) should equal(Seq("text", "comment", "user"))
  }

  it should "correctly extract revisionDeletedPartsAreSuppressed from flag" in {
    (0 until 8).foreach(i => {
      MediawikiEventRevisionDetails.getRevDeletedPartsAreSuppressed(i) should equal(false)
    })

    (8 until 16).foreach(i => {
      MediawikiEventRevisionDetails.getRevDeletedPartsAreSuppressed(i) should equal(true)
    })

  }

  "MediawikiEvent" should "be unmarshable from a Spark Row that contains null Seqs" in {
    val nulls = Array.fill[Any](MediawikiEvent.schema.length)(null)
    val row = new GenericRowWithSchema(nulls, MediawikiEvent.schema)

    val event = MediawikiEvent.fromRow(row)
    event should not be null
    event.userDetails.userBlocksHistorical should equal(None)
  }

  private val revisionRowSchema = StructType(Seq(
    StructField("wiki_db",           StringType),
    StructField("rev_timestamp",     StringType),
    StructField("comment_text",      StringType),
    StructField("actor_user",        LongType),
    StructField("actor_name",        StringType),
    StructField("actor_is_anon",     BooleanType),
    StructField("rev_page",          LongType),
    StructField("rev_id",            LongType),
    StructField("rev_parent_id",     LongType),
    StructField("rev_minor_edit",    BooleanType),
    StructField("rev_deleted",       IntegerType),
    StructField("rev_len",           LongType),
    StructField("rev_sha1",          StringType),
    StructField("rev_content_model", StringType),
    StructField("rev_content_format",StringType),
    StructField("rev_tags",          ArrayType(StringType))
  ))

  private val archiveRowSchema = StructType(Seq(
    StructField("wiki_db",                  StringType),
    StructField("ar_timestamp",             StringType),
    StructField("comment_text",             StringType),
    StructField("actor_user",               LongType),
    StructField("actor_name",               StringType),
    StructField("actor_is_anon",            BooleanType),
    StructField("ar_page_id",               LongType),
    StructField("ar_title",                 StringType),
    StructField("ar_namespace",             IntegerType),
    StructField("ar_namespace_is_content",  BooleanType),
    StructField("ar_rev_id",                LongType),
    StructField("ar_parent_id",             LongType),
    StructField("ar_minor_edit",            BooleanType),
    StructField("ar_deleted",               IntegerType),
    StructField("ar_len",                   LongType),
    StructField("ar_sha1",                  StringType),
    StructField("ar_content_model",         StringType),
    StructField("ar_content_format",        StringType),
    StructField("ar_tags",                  ArrayType(StringType))
  ))

  "MediawikiEvent.fromRevisionRow" should "populate fields for a registered user" in {
    val row = new GenericRowWithSchema(
      Array("enwiki", "20230115120000", "edit summary", 42L, "SomeUser", false, 100L, 999L, 998L, false, 0, 1500L, "abc123", "wikitext", null, null),
      revisionRowSchema
    )
    val event = MediawikiEvent.fromRevisionRow(row)

    event.wikiDb should equal("enwiki")
    event.eventEntity should equal("revision")
    event.eventType should equal("create")
    event.eventUserDetails.userId should equal(Some(42L))
    event.eventUserDetails.userTextHistorical should equal(Some("SomeUser"))
    event.eventUserDetails.userText should equal(None)
    event.eventUserDetails.userIsAnonymous should equal(Some(false))
    event.pageDetails.pageId should equal(Some(100L))
    event.revisionDetails.revId should equal(Some(999L))
    event.revisionDetails.revParentId should equal(Some(998L))
    event.revisionDetails.revTextBytes should equal(Some(1500L))
    event.revisionDetails.revTextSha1 should equal(Some("abc123"))
    event.revisionDetails.revIsDeletedByPageDeletion should equal(Some(false))
  }

  it should "set userText = userTextHistorical for anonymous users" in {
    val row = new GenericRowWithSchema(
      Array("enwiki", "20230115120000", null, null, "1.2.3.4", true, 100L, 999L, 0L, false, 0, 500L, null, null, null, null),
      revisionRowSchema
    )
    val event = MediawikiEvent.fromRevisionRow(row)

    event.eventUserDetails.userId should equal(None)
    event.eventUserDetails.userIsAnonymous should equal(Some(true))
    event.eventUserDetails.userTextHistorical should equal(Some("1.2.3.4"))
    event.eventUserDetails.userText should equal(Some("1.2.3.4"))
  }

  it should "set userId and userTextHistorical to None when actor join fails" in {
    val row = new GenericRowWithSchema(
      Array("enwiki", "20230115120000", null, null, null, null, 100L, 999L, 0L, false, 0, 500L, null, null, null, null),
      revisionRowSchema
    )
    val event = MediawikiEvent.fromRevisionRow(row)

    event.eventUserDetails.userId should equal(None)
    event.eventUserDetails.userTextHistorical should equal(None)
  }

  it should "set userIsCrossWiki to true for an anonymous actor whose name contains '>'" in {
    val row = new GenericRowWithSchema(
      Array("enwiki", "20230115120000", null, null, "SomeWiki>CrossUser", true, 100L, 999L, 0L, false, 0, 500L, null, null, null, null),
      revisionRowSchema
    )
    val event = MediawikiEvent.fromRevisionRow(row)

    event.eventUserDetails.userIsCrossWiki should equal(Some(true))
  }

  "MediawikiEventUserDetails.userIsCrossWiki" should "return Some(true) for a cross-wiki anonymous non-temporary user" in {
    val details = MediawikiEventUserDetails(
      userTextHistorical = Some("SomeWiki>CrossWikiUser"),
      userIsAnonymous = Some(true),
      userIsTemporary = Some(false)
    )
    details.userIsCrossWiki should equal(Some(true))
  }

  it should "return Some(false) when the username contains no '>'" in {
    val details = MediawikiEventUserDetails(
      userTextHistorical = Some("RegularUser"),
      userIsAnonymous = Some(true),
      userIsTemporary = Some(false)
    )
    details.userIsCrossWiki should equal(Some(false))
  }

  it should "return Some(false) when the user is not anonymous" in {
    val details = MediawikiEventUserDetails(
      userTextHistorical = Some("SomeWiki>User"),
      userIsAnonymous = Some(false),
      userIsTemporary = Some(false)
    )
    details.userIsCrossWiki should equal(Some(false))
  }

  it should "return Some(false) when the user is temporary" in {
    val details = MediawikiEventUserDetails(
      userTextHistorical = Some("SomeWiki>User"),
      userIsAnonymous = Some(true),
      userIsTemporary = Some(true)
    )
    details.userIsCrossWiki should equal(Some(false))
  }

  it should "return Some(false) when userTextHistorical is None" in {
    val details = MediawikiEventUserDetails(
      userTextHistorical = None,
      userIsAnonymous = Some(true),
      userIsTemporary = Some(false)
    )
    details.userIsCrossWiki should equal(Some(false))
  }

  "MediawikiEvent.fromArchiveRow" should "set revIsDeletedByPageDeletion to true and populate page fields" in {
    val row = new GenericRowWithSchema(
      Array("dewiki", "20220301090000", "restore", 7L, "Editor", false, 200L, "SomePage", 0, true, 555L, 554L, false, 0, 800L, "def456", null, null, null),
      archiveRowSchema
    )
    val event = MediawikiEvent.fromArchiveRow(row)

    event.wikiDb should equal("dewiki")
    event.revisionDetails.revIsDeletedByPageDeletion should equal(Some(true))
    event.revisionDetails.revId should equal(Some(555L))
    event.revisionDetails.revTextBytes should equal(Some(800L))
    event.pageDetails.pageId should equal(Some(200L))
    event.pageDetails.pageTitleHistorical should equal(Some("SomePage"))
    event.pageDetails.pageNamespaceHistorical should equal(Some(0))
    event.pageDetails.pageNamespaceIsContentHistorical should equal(Some(true))
  }
}
