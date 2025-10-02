package org.wikimedia.analytics.refinery.job.mediawikidumper

import org.scalatest.{FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.tools.LogHelper

import java.time.Instant

class RevisionSpec
  extends FlatSpec
    with Matchers
    with LogHelper {

  "getXML" should "generate correct XML for a Revision with 3 slots" in {
    val revision = Revision(
      ns = 1,
      pageId = 1,
      title = "title",
      revisionId = 1,
      timestamp = Instant.parse("2025-01-01T00:00:00.00Z"),
      contributor = "abc",
      contributorId = Some(1),
      isEditorVisible = true,
      comment = "comment",
      isCommentVisible = true,
      isContentVisible = true,
      contentSlots = Map(
        "main" -> Slot(
          content_body = Some("abc"),
          content_format = Some("abc"),
          content_model = Some("abc"),
          content_sha1 = Some("abc"),
          content_size = Some(3),
          origin_rev_id = Some(1),
      ),
        "second" -> Slot(
          content_body = Some("abc"),
          content_format = Some("abc"),
          content_model = Some("abc"),
          content_sha1 = Some("abc"),
          content_size = Some(3),
          origin_rev_id = Some(1),
        ),
        "third" -> Slot(
          content_body = Some("abc"),
          content_format = Some("abc"),
          content_model = Some("abc"),
          content_sha1 = Some("abc"),
          content_size = Some(3),
          origin_rev_id = Some(1),
        ),
      ),
      parentId = Some(1)
    )

    val output = revision.getXML

    val reference =
      """|    <revision>
         |      <id>1</id>
         |      <parentid>1</parentid>
         |      <timestamp>2025-01-01T00:00:00Z</timestamp>
         |      <contributor>
         |        <username>abc</username>
         |        <id>1</id>
         |      </contributor>
         |      <comment>comment</comment>
         |      <origin>1</origin>
         |      <model>abc</model>
         |      <format>abc</format>
         |      <text bytes="3" sha1="abc" xml:space="preserve">abc</text>
         |      <content>
         |        <role>second</role>
         |        <origin>1</origin>
         |        <model>abc</model>
         |        <format>abc</format>
         |        <text bytes="3" sha1="abc" xml:space="preserve">abc</text>
         |      </content>
         |      <content>
         |        <role>third</role>
         |        <origin>1</origin>
         |        <model>abc</model>
         |        <format>abc</format>
         |        <text bytes="3" sha1="abc" xml:space="preserve">abc</text>
         |      </content>
         |      <sha1>7y0yq7ys07oiqauqqy623pd51tqg0nv</sha1>
         |    </revision>""".stripMargin

    output should equal(reference)
  }
}
