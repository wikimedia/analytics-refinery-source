package org.wikimedia.analytics.refinery.job.mediawikidumper

import scala.xml.Utility.escape

case class Page(pageId: Long, ns: Long, title: String, redirectTarget: Option[String]) extends XMLProducer {

    def getXML: String = {
        if (redirectTarget.isEmpty || redirectTarget.getOrElse("").isEmpty) {
            f"""|  <page>
                |    <title>${escape(title)}</title>
                |    <ns>$ns</ns>
                |    <id>$pageId</id>""".stripMargin
        } else {
            f"""|  <page>
                |    <title>${escape(title)}</title>
                |    <ns>$ns</ns>
                |    <id>$pageId</id>
                |    <redirect title="${escape(redirectTarget.get)}" />""".stripMargin
        }
    }

}
