package org.wikimedia.analytics.refinery.job.mediawikidumper

trait XMLProducer {

    def pagesPartition: Option[PagesPartition]

    def getXML: String

    def getPartitionStartPageId: Long = pagesPartition.get.startPageId

    def getPartitionEndPageId: Long = pagesPartition.get.endPageId
}
