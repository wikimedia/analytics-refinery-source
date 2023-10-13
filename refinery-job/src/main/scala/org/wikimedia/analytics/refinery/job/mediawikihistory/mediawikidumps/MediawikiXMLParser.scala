package org.wikimedia.analytics.refinery.job.mediawikihistory.mediawikidumps

import java.io.InputStream
import javax.xml.stream.XMLStreamConstants

import com.ctc.wstx.stax.WstxInputFactory
import org.codehaus.stax2.XMLStreamReader2

import scala.annotation.tailrec

/**
 * Parses xml chunks of data from wikimedia dumps (user, page and revision).
 * Builds revision objects using MediaWikiRevisionFactory trait.
 * Page parsing expects pages without revisions or uploads.
 *
 * Uses WoodStox XmlStreamReader2 to stream-parse XML chunks recursively.
 *
 * @param mwObjectsFactory The mediawiki-objects factory to use. It defines
 *                         how parsed-objects are represented internally.
 * @param wiki The wiki to be parsed (to inform mediawiki-objects)
 * @tparam MwObjectsFactory An implementation of [[MediawikiObjectsFactory]]
 */
class MediawikiXMLParser[MwObjectsFactory <: MediawikiObjectsFactory](
  val mwObjectsFactory: MwObjectsFactory,
  val wiki: String
) {

  // Explicitly load WoodStoxInputFactory instead of letting java pick a 'random' one.
  private final val xmlInputFactory = new WstxInputFactory()

  /**
   * Initializes a WoodStox XmlStreamReader2 from an InputStreamReader
   * @param inputStreamReader The InputStreamReader to use
   * @return The XmlStreamReader2
   */
  def initializeXmlStreamReader(inputStreamReader: InputStream): XMLStreamReader2 = {
    xmlInputFactory.createXMLStreamReader(inputStreamReader) match {
      case xmlStreamReader2: XMLStreamReader2 => xmlStreamReader2
      case _ => throw new ClassCastException
    }
  }

  /**
   * Parse an XML-user-chunk into an MwUser object.
   * @param xmlStreamReader The XML stream to parse
   * @return The MwUser object
   */
  def parseUser(xmlStreamReader: XMLStreamReader2): mwObjectsFactory.MwUser =
    parseUser_rec(xmlStreamReader, mwObjectsFactory.makeDummyUser)

  @tailrec private def parseUser_rec(
    xmlStreamReader: XMLStreamReader2,
    user: mwObjectsFactory.MwUser
  ): mwObjectsFactory.MwUser = {
    if (!xmlStreamReader.hasNext) user
    else parseUser_rec(xmlStreamReader, {
      xmlStreamReader.next match {
        case XMLStreamConstants.START_ELEMENT =>
          xmlStreamReader.getName.toString match {
            case "id" => user.setId(Some(xmlStreamReader.getElementText.toLong))
            case "username" => user.setUserText(xmlStreamReader.getElementText)
            case "ip" => user.setId(None).setUserText(xmlStreamReader.getElementText)
            case _ => user
          }
        case XMLStreamConstants.END_ELEMENT =>
          xmlStreamReader.getName.toString match {
            // VERY IMPORTANT:
            // Force returning in case the user element is closed,
            // instead of continuing the recursion
            case "contributor" => return user
            case _ => user
          }
        case _ => user
      }
    })
  }


  /**
   * Parse an XML-page-chunk into an MwPage object.
   * The XML-chunk is expected not to contain revisions or uploads, only the metadata.
   * @param xmlStreamReader The XML stream to parse
   * @return The MwPage object
   */
  def parsePage(xmlStreamReader: XMLStreamReader2): mwObjectsFactory.MwPage =
    parsePage_rec(xmlStreamReader, mwObjectsFactory.makeDummyPage.setWikiDb(wiki))

  @tailrec private def parsePage_rec(
    xmlStreamReader: XMLStreamReader2,
    page: mwObjectsFactory.MwPage
  ): mwObjectsFactory.MwPage = {
    if (!xmlStreamReader.hasNext) page
    else parsePage_rec(xmlStreamReader, {
      xmlStreamReader.next match {
        case XMLStreamConstants.START_ELEMENT =>
          xmlStreamReader.getName.toString match {
            case "title" => page.setTitle(xmlStreamReader.getElementText)
            case "ns" => page.setNamespace(xmlStreamReader.getElementText.toLong)
            case "id" => page.setId(xmlStreamReader.getElementText.toLong)
            case "redirect" => page.setRedirectTitle(xmlStreamReader.getAttributeValue(null, "title"))
            case "restrictions" => page.addRestriction(xmlStreamReader.getElementText)
            case _ => page
          }
        case XMLStreamConstants.END_ELEMENT =>
          xmlStreamReader.getName.toString match {
            // VERY IMPORTANT:
            // Force returning in case the page element is closed,
            // instead of continuing the recursion
            case "page" => return page
            case _ => page
          }
        case _ => page
      }
    })
  }

  /**
   * Parse an XML-revision-chunk into an MwRev object and augment it with its page data
   * @param xmlStreamReader The XML stream to parse
   * @param page The page of the revision
   * @return The MwRev object
   */
  def parseRevision(
    xmlStreamReader: XMLStreamReader2,
    page: mwObjectsFactory.MwPage
  ): mwObjectsFactory.MwRev =
    parseRevision_rec(xmlStreamReader, mwObjectsFactory.makeDummyRevision.setPage(page))

  @tailrec private def parseRevision_rec(
    xmlStreamReader: XMLStreamReader2,
    revision: mwObjectsFactory.MwRev
  ): mwObjectsFactory.MwRev = {
    if (!xmlStreamReader.hasNext) revision
    else parseRevision_rec(xmlStreamReader, {
      xmlStreamReader.next match {
        case XMLStreamConstants.START_ELEMENT =>
          val elName = xmlStreamReader.getName.toString
          elName match {
            case "id" => revision.setId(xmlStreamReader.getElementText.toLong)
            // when parentid is not present, the default of None means this is the first revision in the chain
            case "parentid" => revision.setParentId(Some(xmlStreamReader.getElementText.toLong))
            case "timestamp" => revision.setTimestamp(xmlStreamReader.getElementText)
            case "contributor" =>
              if (xmlStreamReader.getAttributeValue(null, "deleted") == "deleted") {
                revision.setUserIsVisible(false)
                // leave default user value
              } else {
                revision.setUser(parseUser(xmlStreamReader))
              }
            case "minor" => revision.setMinor(true)
            case "comment" =>
              if (xmlStreamReader.getAttributeValue(null, "deleted") == "deleted") {
                revision.setCommentIsVisible(false)
              } else {
                revision.setComment(xmlStreamReader.getElementText)
              }
            case "model" => revision.setModel(xmlStreamReader.getElementText)
            case "format" => revision.setFormat(xmlStreamReader.getElementText)
            case "text" =>
              val bytesAttr = xmlStreamReader.getAttributeValue(null, "bytes")
              val bytes = if (bytesAttr == null || bytesAttr.trim.isEmpty) None else Some(bytesAttr.toLong)
              if (xmlStreamReader.getAttributeValue(null, "deleted") == "deleted") {
                // bytes are set in the dumps even on deleted revisions, this is inconsistent with WikiReplicas on cloud
                revision.setContentIsVisible(false).setBytes(bytes.getOrElse(0L))
              } else {
                val text = xmlStreamReader.getElementText
                revision.setText(text).setBytes(bytes.getOrElse(text.getBytes("utf-8").length.toLong))
              }
            case "sha1" => revision.setSha1(xmlStreamReader.getElementText)
            case _ => revision
          }
        case XMLStreamConstants.END_ELEMENT =>
          xmlStreamReader.getName.toString match {
            // VERY IMPORTANT:
            // Force returning in case the revision element is closed
            // instead of continuing the recursion
            case "revision" => return revision
            case _ => revision
          }
        case _ => revision
      }
    })
  }

}
