package org.wikimedia.analytics.refinery.job.mediawikidumper

import java.time.Instant

import scala.xml.Utility.escape
import org.wikimedia.analytics.refinery.spark.sql.MediawikiMultiContentRevisionSha1

/** Case class representing a Slot within a MediaWiki page revision. */
case class Slot(
    content_body: Option[String],
    content_format: Option[String],
    content_model: Option[String],
    content_sha1: Option[String],
    content_size: Option[Long],
    origin_rev_id: Option[Long],
)

/** Case class representing a MediaWiki page revision. */
case class Revision(
    ns: Long,
    pageId: Long,
    title: String,
    redirectTarget: Option[String] = None,
    revisionId: Long,
    timestamp: Instant,
    contributor: String,
    contributorId: Option[Long],
    isEditorVisible: Boolean,
    comment: String,
    isCommentVisible: Boolean,
    isContentVisible: Boolean,
    contentSlots: Map[String, Slot],
    parentId: Option[Long] = None,
    isMinor: Boolean = false
) extends XMLProducer {

    def slotsSha1sAsSeq(): Seq[(String, String)] = {
        contentSlots.toSeq.map { case (slot_role, slot) =>
          (slot_role, slot.content_sha1.getOrElse(""))
        }
    }

    // This Rendering follows the same sanitization rules as Wiki Replica views:
    // https://gerrit.wikimedia.org/g/operations/puppet/+/refs/heads/production/modules/profile/templates/wmcs/db/wikireplicas/maintain-views.yaml#744
    // And the schema outlined at
    // https://www.mediawiki.org/xml/export-0.11.xsd
    def getXML: String = {
        val parentIdElement = {
            if (parentId.isDefined) {
                f"\n      <parentid>${parentId.get}</parentid>"
            } else {
                ""
            }
        }

        val contributorElement = {
            if (isEditorVisible && contributor != null) {

                // TODO: change for IP Masking when we have temp user information
                // logged in editor: Non-Zero Positive Integer ID
                // old system import: Zero ID
                // IP editor: Null ID
                val identifier = {
                    if (contributorId.isDefined) {
                        f"""|        <username>${escape(contributor)}</username>
                            |        <id>${contributorId.get}</id>""".stripMargin
                    } else {
                        f"""|        <ip>${escape(contributor)}</ip>""".stripMargin
                    }
                }
                f"""|      <contributor>
                            |${identifier}
                    |      </contributor>""".stripMargin
            } else {
                """      <contributor deleted="deleted" />"""
            }
        }

        val minorElement = {
            if (isMinor) {
                "\n      <minor />"
            } else {
                ""
            }
        }

        val commentElement = {
            if (isCommentVisible) {
                if (comment == null || comment.isEmpty) {
                    ""
                } else {
                    f"""|
                    |      <comment>${escape(comment)}</comment>""".stripMargin
                }
            } else {
                f"""|
                |      <comment deleted="deleted" />""".stripMargin
            }
        }

        def slotTextElement(slot: Slot): String = {
            if (isContentVisible && slot.content_size.getOrElse(0L) > 0L) {
                val size = slot.content_size.getOrElse(0L)
                val sha1 = slot.content_sha1.getOrElse("")
                val text = slot.content_body.getOrElse("")

                f"""<text bytes="${size}" sha1="${sha1}" xml:space="preserve">${escape(text)}</text>"""
            } else if (isContentVisible) {
                f"""<text bytes="0" />"""
            } else {
                """<text bytes="-1" deleted="deleted" />"""
            }
        }

        val mainSlot = contentSlots("main")
        val mainSlotTextElement = slotTextElement(mainSlot)

        val contentElements = {
          val sortedOtherSlots = contentSlots.toSeq.filter(_._1 != "main").sortBy(_._1)
          if (isContentVisible && sortedOtherSlots.nonEmpty) {
              val otherSlotsElements = sortedOtherSlots.map { case (role, slot) =>
                  val slotRole = role
                  val slotOrigin = slot.origin_rev_id.getOrElse(-1L)
                  val slotModel = slot.content_model.getOrElse("error-no-model")
                  val slotFormat = slot.content_format.getOrElse("error-no-format")

                  f"""|      <content>
                      |        <role>$slotRole</role>
                      |        <origin>$slotOrigin</origin>
                      |        <model>$slotModel</model>
                      |        <format>$slotFormat</format>
                      |        ${slotTextElement(slot)}
                      |      </content>""".stripMargin
              }

              f"""${otherSlotsElements.mkString("\n", "\n", "")}"""
          } else {
            ""
          }
        }

        val sha1Element = {
            if (isContentVisible) {
                f"""<sha1>${MediawikiMultiContentRevisionSha1.computeForTuples(slotsSha1sAsSeq())}</sha1>"""
            } else {
                f"""<sha1 />"""
            }
        }

        f"""    <revision>
           |      <id>$revisionId</id>${parentIdElement}
           |      <timestamp>$timestamp</timestamp>
           |$contributorElement$minorElement$commentElement
           |      <origin>${mainSlot.origin_rev_id.getOrElse(-1L)}</origin>
           |      <model>${mainSlot.content_model.getOrElse("error-no-model")}</model>
           |      <format>${mainSlot.content_format.getOrElse("error-no-format")}</format>
           |      $mainSlotTextElement$contentElements
           |      $sha1Element
           |    </revision>""".stripMargin
    }

    def buildPage: Page = Page(pageId = pageId, ns = ns, title = title, redirectTarget = redirectTarget)
}
