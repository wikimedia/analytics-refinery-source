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

  "getPage" should "generate correct Page rendering when a Page is a redirect" in {
    val base = Revision(
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
        )
      ),
      parentId = Some(1)
    )

    val redirectNone = base.copy(redirectTarget = None)
    val redirectEmptyString = base.copy(redirectTarget = Some(""))
    val redirectWithTarget = base.copy(redirectTarget = Some("Other Title"))

    val noneOutput = redirectNone.buildPage.getXML
    val emptyStringOutput = redirectEmptyString.buildPage.getXML
    val withTargetOutput = redirectWithTarget.buildPage.getXML

    val referenceNoneAndEmpty =
      """  <page>
        |    <title>title</title>
        |    <ns>1</ns>
        |    <id>1</id>""".stripMargin

    val referenceWithTargetOutput =
      """  <page>
        |    <title>title</title>
        |    <ns>1</ns>
        |    <id>1</id>
        |    <redirect title="Other Title" />""".stripMargin

    noneOutput should equal(referenceNoneAndEmpty)
    emptyStringOutput should equal(referenceNoneAndEmpty)
    withTargetOutput should equal(referenceWithTargetOutput)
  }

  "getXML" should "honor vertical bars (`|`) in content body" in {
    val body = """{{Infobox City
              | |official_name=Apples
              | |subdivision_type=Canton
              | |subdivision_name= Vaud
              | |subdivision_type1=District
              | |subdivision_name1=[[Aubonne (district)|Aubonne]]
              | |latd=06|latm=26
              | |longd=46|longm=33
              | |elevation=635
              | |area_total=12.89
              | |population_total=1142
              | |population_as_of=2004
              | |population_density= 86
              | |leader_title=Mayor
              | |leader_name= Claude-Alain Roulet
              | |website= [www.apples.ch]
              |}}
              |
              |'''Apples''' is a [[commune]] in [[Switzerland]]. It is in [[Vaud]] [[Canton]]. Vaud Canton has 364 communes in it.
              |
              |[[Image:Apples-drapeau.png|thumb|left|Flag of Apples]]
              |
              |
              |{{stub}}
              |[[Category:Cities in Switzerland]]
              |
              |[[de:Apples]]
              |[[en:Apples, Vaud]]
              |[[fr:Apples]]
              |[[it:Apples]]
              |[[nl:Apples]]
              |[[ro:Apples, Elveţia]]""".stripMargin

    val rev = Revision(
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
          content_body = Some(body),
          content_format = Some("abc"),
          content_model = Some("abc"),
          content_sha1 = Some("abc"),
          content_size = Some(3),
          origin_rev_id = Some(1),
        )
      ),
      parentId = Some(1)
    )

    val reference =
      """    <revision>
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
        |      <text bytes="3" sha1="abc" xml:space="preserve">{{Infobox City
        | |official_name=Apples
        | |subdivision_type=Canton
        | |subdivision_name= Vaud
        | |subdivision_type1=District
        | |subdivision_name1=[[Aubonne (district)|Aubonne]]
        | |latd=06|latm=26
        | |longd=46|longm=33
        | |elevation=635
        | |area_total=12.89
        | |population_total=1142
        | |population_as_of=2004
        | |population_density= 86
        | |leader_title=Mayor
        | |leader_name= Claude-Alain Roulet
        | |website= [www.apples.ch]
        |}}
        |
        |'''Apples''' is a [[commune]] in [[Switzerland]]. It is in [[Vaud]] [[Canton]]. Vaud Canton has 364 communes in it.
        |
        |[[Image:Apples-drapeau.png|thumb|left|Flag of Apples]]
        |
        |
        |{{stub}}
        |[[Category:Cities in Switzerland]]
        |
        |[[de:Apples]]
        |[[en:Apples, Vaud]]
        |[[fr:Apples]]
        |[[it:Apples]]
        |[[nl:Apples]]
        |[[ro:Apples, Elveţia]]</text>
        |      <sha1>abc</sha1>
        |    </revision>""".stripMargin

    rev.getXML should equal(reference)
  }
}
