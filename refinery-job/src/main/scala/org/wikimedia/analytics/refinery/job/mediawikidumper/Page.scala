package org.wikimedia.analytics.refinery.job.mediawikidumper

import scala.xml.Utility.escape

case class Page(pageId: Long, ns: Long, title: String, redirectTarget: Option[String]) extends XMLProducer {

    def getXML: String = {
        val sb = new StringBuilder()
        sb.append(f"  <page>\n")
        sb.append(f"    <title>${escape(title)}</title>\n")
        sb.append(f"    <ns>$ns</ns>\n")
        sb.append(f"    <id>$pageId</id>")
        redirectTarget.map( rt =>
            if (rt.nonEmpty) {
                sb.append(f"""\n    <redirect title="${escape(rt)}" />""")
            }
        )

        sb.toString()
    }

}
