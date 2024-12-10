package org.wikimedia.analytics.refinery.job.mediawikidumper

import scala.xml.Utility.escape

case class Page(pageId: Long, ns: Long, title: String) extends XMLProducer {

    def getXML: String = {
        f"""|  <page>
            |    <title>${escape(title)}</title>
            |    <ns>$ns</ns>
            |    <id>$pageId</id>""".stripMargin
    }

}
