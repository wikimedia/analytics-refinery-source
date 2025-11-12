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
        def contributorElement(sb: StringBuilder): StringBuilder = {
            if (isEditorVisible && contributor != null) {
                // logged in editor: Non-Zero Positive Integer ID
                // old system import: Zero ID
                // IP editor: Null ID
                // Temp account (aka IP Masking): username starts with '~YYYY'. See T365693.
                sb.append("      <contributor>\n")
                if (contributorId.isDefined) {
                  sb.append(f"        <username>${escape(contributor)}</username>\n")
                  sb.append(f"        <id>${contributorId.get}</id>\n")
                } else {
                  sb.append(f"        <ip>${escape(contributor)}</ip>\n")
                }
                sb.append("      </contributor>\n")
            } else {
                sb.append("      <contributor deleted=\"deleted\" />\n")
            }
        }

        def commentElement(sb: StringBuilder): StringBuilder = {
            if (isCommentVisible) {
                if (comment == null || comment.isEmpty) {
                    sb
                } else {
                    sb.append(f"      <comment>${escape(comment)}</comment>\n")
                }
            } else {
                sb.append("      <comment deleted=\"deleted\" />\n")
            }
        }

        def slotTextElement(sb: StringBuilder, indent: String, slot: Slot): StringBuilder = {
            if (isContentVisible && slot.content_size.getOrElse(0L) > 0L) {
                val size = slot.content_size.getOrElse(0L)
                val sha1 = slot.content_sha1.getOrElse("")
                val text = slot.content_body.getOrElse("")

                sb.append(f"""${indent}<text bytes="${size}" sha1="${sha1}" xml:space="preserve">""")
                sb.append(escape(text))
                sb.append(f"""</text>\n""")
            } else if (isContentVisible) {
                sb.append(f"""${indent}<text bytes="0" />\n""")
            } else {
                sb.append(f"""${indent}<text bytes="-1" deleted="deleted" />\n""")
            }
        }

        def contentElements(sb: StringBuilder): StringBuilder = {
            val sortedOtherSlots = contentSlots.toSeq.filter(_._1 != "main").sortBy(_._1)
            if (isContentVisible && sortedOtherSlots.nonEmpty) {
                sortedOtherSlots.map { case (role, slot) =>
                    val slotRole = role
                    val slotOrigin = slot.origin_rev_id.getOrElse(-1L)
                    val slotModel = slot.content_model.getOrElse("error-no-model")
                    val slotFormat = slot.content_format.getOrElse("error-no-format")

                    sb.append(f"      <content>\n")
                    sb.append(f"        <role>$slotRole</role>\n")
                    sb.append(f"        <origin>$slotOrigin</origin>\n")
                    sb.append(f"        <model>$slotModel</model>\n")
                    sb.append(f"        <format>$slotFormat</format>\n")
                    slotTextElement(sb, "        ", slot)
                    sb.append(f"      </content>\n")
                }
                sb
            } else {
                sb
            }
          }

        def sha1Element(sb: StringBuilder): StringBuilder = {
            if (isContentVisible) {
                sb.append(f"      <sha1>${MediawikiMultiContentRevisionSha1.computeForTuples(slotsSha1sAsSeq())}</sha1>\n")
            } else {
                sb.append(f"      <sha1 />\n")
            }
        }

        // assemble the XML elements in the proper order
        val sb = new StringBuilder()
        sb.append(f"    <revision>\n")
        sb.append(f"      <id>$revisionId</id>\n")
        parentId.map(pid => sb.append(f"      <parentid>${pid}</parentid>\n"))
        sb.append(f"      <timestamp>$timestamp</timestamp>\n")
        contributorElement(sb)
        if (isMinor) { sb.append("      <minor />\n") }
        commentElement(sb)
        val mainSlot = contentSlots("main")
        sb.append(f"      <origin>${mainSlot.origin_rev_id.getOrElse(-1L)}</origin>\n")
        sb.append(f"      <model>${mainSlot.content_model.getOrElse("error-no-model")}</model>\n")
        sb.append(f"      <format>${mainSlot.content_format.getOrElse("error-no-format")}</format>\n")
        slotTextElement(sb, "      ", mainSlot)
        contentElements(sb)
        sha1Element(sb)
        sb.append("    </revision>")

        sb.toString()
    }

    def buildPage: Page = Page(pageId = pageId, ns = ns, title = title, redirectTarget = redirectTarget)
}
