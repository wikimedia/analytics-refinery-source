package org.wikimedia.analytics.refinery.job.mediawikidumper

import scala.xml.Utility.escape

case class Page(pageId: Long,
                ns: Option[Long] = None,
                title: Option[String] = None,
                pagesPartition: Option[PagesPartition] = None
) extends XMLProducer {

    def getXML: String = {
        f"""|  <page>
            |    <title>${escape(title.getOrElse("error-no-title"))}</title>
            |    <ns>${ns.getOrElse(-999999)}</ns>
            |    <id>${pageId}</id>""".stripMargin
    }

    def addPartitionInfo(pagePartition: PagesPartition): Page = copy(pagesPartition = Some(pagePartition))
}
