package org.wikimedia.analytics.refinery.job.mediawikidumper

import scala.xml.Utility.escape

/**
 * Case class representing a MediaWiki page revision.
 * @param pageId
 * @param revisionId
 * @param timestamp
 * @param contributor
 * @param contributorId
 * @param isEditorVisible
 * @param comment
 * @param isCommentVisible
 * @param isContentVisible
 * @param model
 * @param format
 * @param text
 * @param size
 * @param sha1
 * @param ns Useful to create the revision Page instance
 * @param title Useful to create the revision Page instance
 * @param parentId Useful to build the page dataset (See MediaWikiDumper)
 * @param pagesPartition Optional information about the partition this revision belongs to. Filled in the process.
 */
case class Revision(pageId:Long,
                    revisionId:Long,
                    timestamp: String,
                    contributor:String,
                    contributorId:Option[Long],
                    isEditorVisible:Boolean,
                    comment:String,
                    isCommentVisible:Boolean,
                    isContentVisible:Boolean,
                    model:Option[String],
                    format:Option[String],
                    text:Option[String],
                    size:Option[Long],
                    sha1:Option[String],
                    ns: Option[Long] = None,
                    title: Option[String] = None,
                    parentId: Option[Long] = None,
                    isMinor: Boolean = false,
                    pagesPartition: Option[PagesPartition] = None
) extends XMLProducer {

    // This Rendering follows the same sanitization rules as Wiki Replica views:
    // https://gerrit.wikimedia.org/g/operations/puppet/+/refs/heads/production/modules/profile/templates/wmcs/db/wikireplicas/maintain-views.yaml#744
    // And the schema outlined at
    // https://www.mediawiki.org/xml/export-0.10.xsd
    def getXML: String = {
        val parentIdElement = if (parentId.isDefined) f"\n      <parentid>${parentId.get}</parentid>" else ""

        val contributorElement = if (isEditorVisible) {

            // TODO: change for IP Masking when we have temp user information
            // logged in editor: Non-Zero Positive Integer ID
            // old system import: Zero ID
            // IP editor: Null ID
            val identifier = if (contributorId.isDefined) {
                f"""|        <username>${escape(contributor)}</username>
                    |        <id>${contributorId.get}</id>
                    |""".stripMargin
            } else {
                f"""|        <ip>${escape(contributor)}</ip>
                    |""".stripMargin
            }
            f"""      <contributor>
               |${identifier}
               |      </contributor>
               |""".stripMargin
        } else {
            "      <contributor deleted=\"deleted\" />"
        }

        val minorElement = if (isMinor) "\n      <minor />" else ""

        val commentElement = if (isCommentVisible) {
            if (comment.length == 0) {
                ""
            } else {
                f"""|
                    |      <comment>${escape(comment)}</comment>""".stripMargin
            }
        } else {
            f"""|
                |      <comment deleted=\"deleted\" />""".stripMargin
        }

        val textElement = if (isContentVisible && size.getOrElse(0L) > 0L) {
            f"""      <text bytes="${size.get}" xml:space="preserve">${escape(text.getOrElse(""))}</text>"""
        } else if (isContentVisible) {
            f"""      <text bytes="0" />"""
        } else {
            // TODO: check whether we should be outputting size on suppressed content, might be better not to
            f"""      <text bytes="${size.getOrElse(0L)}" deleted=\"deleted\" />"""
        }

        val sha1Element = if (isContentVisible) {
            f"""      <sha1>${sha1.getOrElse("")}</sha1>"""
        } else {
            f"""      <sha1 />"""
        }

        f"""|    <revision>
            |      <id>${revisionId}</id>${parentIdElement}
            |      <timestamp>${timestamp}</timestamp>
            |${contributorElement}${minorElement}${commentElement}
            |      <model>${model.getOrElse("error-no-model")}</model>
            |      <format>${format.getOrElse("error-no-format")}</format>
            |${textElement}
            |${sha1Element}
            |    </revision>""".stripMargin
    }

    def addPartitionInfo(pagesPartition: PagesPartition): Revision = copy(pagesPartition = Some(pagesPartition))

    def pagesPartitionKey: PagesPartitionKey = {
        PagesPartitionKey(
            sparkPartitionId = pagesPartition.get.sparkPartitionId.get,
            pageId = pageId,
            revisionId = revisionId
        )
    }

    def buildPage: Page = {
        Page(
            pageId = pageId,
            ns = ns,
            title = title,
            pagesPartition = pagesPartition
        )
    }
}
