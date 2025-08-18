package org.wikimedia.analytics.refinery.job.mediawikidumper

import java.time.Instant

import scala.xml.Utility.escape

/** Case class representing a MediaWiki page revision.
  * @param sha1
  *   Useful to create the revision Page instance
  * @param title
  *   Useful to create the revision Page instance
  * @param parentId
  *   Useful to build the page dataset (See MediaWikiDumper)
  * @param partitionId
  *   intermediate partition id calculated from window-accumulated size
  */
case class Revision(
    ns: Long,
    pageId: Long,
    title: String,
    revisionId: Long,
    timestamp: Instant,
    contributor: String,
    contributorId: Option[Long],
    isEditorVisible: Boolean,
    comment: String,
    isCommentVisible: Boolean,
    isContentVisible: Boolean,
    model: Option[String],
    format: Option[String],
    text: Option[String],
    size: Option[Long],
    sha1: Option[String],
    parentId: Option[Long] = None,
    isMinor: Boolean = false
) extends XMLProducer {

    // This Rendering follows the same sanitization rules as Wiki Replica views:
    // https://gerrit.wikimedia.org/g/operations/puppet/+/refs/heads/production/modules/profile/templates/wmcs/db/wikireplicas/maintain-views.yaml#744
    // And the schema outlined at
    // https://www.mediawiki.org/xml/export-0.10.xsd
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
                "      <contributor deleted=\"deleted\" />"
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
                |      <comment deleted=\"deleted\" />""".stripMargin
            }
        }

        val textElement = {
            if (isContentVisible && size.getOrElse(0L) > 0L) {
                f"""      <text bytes="${size
                        .get}" xml:space="preserve">${escape(
                      text.getOrElse("")
                    )}</text>"""
            } else if (isContentVisible) {
                f"""      <text bytes="0" />"""
            } else {
                // TODO: check whether we should be outputting size on suppressed content, might be better not to
                f"""      <text bytes="${size
                        .getOrElse(0L)}" deleted=\"deleted\" />"""
            }
        }

        val sha1Element = {
            if (isContentVisible) {
                f"""      <sha1>${sha1.getOrElse("")}</sha1>"""
            } else {
                f"""      <sha1 />"""
            }
        }

        f"""    <revision>
           |      <id>$revisionId</id>${parentIdElement}
           |      <timestamp>$timestamp</timestamp>
           |$contributorElement$minorElement$commentElement
           |      <model>${model.getOrElse("error-no-model")}</model>
           |      <format>${format.getOrElse("error-no-format")}</format>
           |$textElement
           |$sha1Element
           |    </revision>""".stripMargin
    }

    def buildPage: Page = Page(pageId = pageId, ns = ns, title = title)
}
