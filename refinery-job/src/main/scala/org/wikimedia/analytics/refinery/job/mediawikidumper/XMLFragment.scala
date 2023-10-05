package org.wikimedia.analytics.refinery.job.mediawikidumper

case class XMLFragment(isXMLHeader: Boolean = false,
                       isXMLFooter: Boolean = false,
                       isPageFooter: Boolean = false,
                       pagesPartition: Option[PagesPartition] = None,
                       siteInfo: Option[SiteInfo] = None,
) extends XMLProducer {

    def getXML: String = {
        if (pagesPartition.isEmpty) throw new Exception("XMLFragment must have a PagesPartition")
        if (isPageFooter) {
            "  </page>"
        } else if (isXMLHeader) {
            val si = siteInfo.get
            f"""|<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.10/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.mediawiki.org/xml/export-0.10/ http://www.mediawiki.org/xml/export-0.10.xsd" version="0.10" xml:lang="${si.languageCode}">
                |${si.getXML}""".stripMargin

        } else if (isXMLFooter) {
            "</mediawiki>"
        } else {
            throw new Exception("XMLFragment is not a valid XML fragment")
        }
    }
}

object XMLFragment {

    def xmlHeader(pagesPartition: PagesPartition, siteInfo: SiteInfo): XMLFragment = {
        XMLFragment(
            isXMLHeader = true,
            siteInfo = Option(siteInfo),
            pagesPartition = Some(pagesPartition)
        )
    }

    def xmlFooter(pagesPartition: PagesPartition): XMLFragment = {
        XMLFragment(isXMLFooter = true, pagesPartition = Some(pagesPartition))
    }

    def pageFooter(pagesPartition: PagesPartition): XMLFragment = {
        XMLFragment(isPageFooter = true, pagesPartition = Some(pagesPartition))
    }
}
