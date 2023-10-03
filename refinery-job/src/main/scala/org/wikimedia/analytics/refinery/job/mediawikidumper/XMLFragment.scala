package org.wikimedia.analytics.refinery.job.mediawikidumper

case class XMLFragment(isXMLHeader: Boolean = false,
                       isXMLFooter: Boolean = false,
                       isPageFooter: Boolean = false,
                       wikidb: String = "wikidb",
                       pagesPartition: Option[PagesPartition] = None
) extends XMLProducer {

    def getXML: String = {
        if (pagesPartition.isEmpty) throw new Exception("XMLFragment must have a PagesPartition")
        if (isPageFooter) {
            "  </page>"
        } else if (isXMLHeader) {
            // TODO: how do we get the language here?  Some projects don't have a language code
            // TODO: generate the siteinfo automatically
            f"""|<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.10/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.mediawiki.org/xml/export-0.10/ http://www.mediawiki.org/xml/export-0.10.xsd" version="0.10" xml:lang="en">
                |  <siteinfo>
                |    <sitename>Wikipedia</sitename>
                |    <dbname>${wikidb}</dbname>
                |    <base>https://simple.wikipedia.org/wiki/Main_Page</base>
                |    <generator>MediaWiki 1.41.0-wmf.24</generator>
                |    <case>first-letter</case>
                |    <namespaces>
                |      <namespace key="-2" case="first-letter">Media</namespace>
                |      <namespace key="-1" case="first-letter">Special</namespace>
                |      <namespace key="0" case="first-letter" />
                |      <namespace key="1" case="first-letter">Talk</namespace>
                |      <namespace key="2" case="first-letter">User</namespace>
                |      <namespace key="3" case="first-letter">User talk</namespace>
                |      <namespace key="4" case="first-letter">Wikipedia</namespace>
                |      <namespace key="5" case="first-letter">Wikipedia talk</namespace>
                |      <namespace key="6" case="first-letter">File</namespace>
                |      <namespace key="7" case="first-letter">File talk</namespace>
                |      <namespace key="8" case="first-letter">MediaWiki</namespace>
                |      <namespace key="9" case="first-letter">MediaWiki talk</namespace>
                |      <namespace key="10" case="first-letter">Template</namespace>
                |      <namespace key="11" case="first-letter">Template talk</namespace>
                |      <namespace key="12" case="first-letter">Help</namespace>
                |      <namespace key="13" case="first-letter">Help talk</namespace>
                |      <namespace key="14" case="first-letter">Category</namespace>
                |      <namespace key="15" case="first-letter">Category talk</namespace>
                |      <namespace key="710" case="first-letter">TimedText</namespace>
                |      <namespace key="711" case="first-letter">TimedText talk</namespace>
                |      <namespace key="828" case="first-letter">Module</namespace>
                |      <namespace key="829" case="first-letter">Module talk</namespace>
                |      <namespace key="2300" case="case-sensitive">Gadget</namespace>
                |      <namespace key="2301" case="case-sensitive">Gadget talk</namespace>
                |      <namespace key="2302" case="case-sensitive">Gadget definition</namespace>
                |      <namespace key="2303" case="case-sensitive">Gadget definition talk</namespace>
                |    </namespaces>
                |  </siteinfo>""".stripMargin

        } else if (isXMLFooter) {
            "</mediawiki>"
        } else {
            throw new Exception("XMLFragment is not a valid XML fragment")
        }
    }
}

object XMLFragment {

    def xmlHeader(pagesPartition: PagesPartition, wikidb: String): XMLFragment = {
        XMLFragment(
            isXMLHeader = true,
            wikidb = wikidb,
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
