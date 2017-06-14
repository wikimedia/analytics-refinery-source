package org.wikimedia.analytics.refinery.job.mediawikihistory.page

import java.sql.Timestamp

import org.wikimedia.analytics.refinery.job.mediawikihistory.TestHelpers
import org.wikimedia.analytics.refinery.job.mediawikihistory.TestHelpers._

object TestPageHistoryHelpers {

  def pageEventSet(
    wikiDb: Option[String] = Some("testwiki"),
    timestamp: Option[Timestamp] = None,
    eventType: Option[String] = None,
    causedByUserId: Option[Long] = Some(0L),
    pageId: Option[Long] = None,
    oldTitle: Option[String] = Some("Title"),
    newTitle: Option[String] = Some("Title"),
    newTitlePrefix: Option[String] = Some(""),
    newTitleWithoutPrefix: Option[String] = Some("Title"),
    oldNamespace: Option[Int] = Some(0),
    oldNamespaceIsContent: Option[Boolean] = Some(true),
    newNamespace: Option[Int] = Some(0),
    newNamespaceIsContent: Option[Boolean] = Some(true)
  )(
    headerLine: String,
    eventLines: String*
  ): Seq[PageEvent] = {
    val headers = headerLine.split(" ").filter(_.nonEmpty)
    eventLines.map { line =>
      val values = line.split(" ").filter(_.nonEmpty)
      val valueMap = headers.zip(values).map { case (h, v) =>
        h match {
          case "wiki" | "db" | "wikiDb" => ("wikiDb" -> string(v))
          case "time" | "timestamp" => ("timestamp" -> TestHelpers.timestamp(v))
          case "type" | "eventType" => ("eventType" -> string(v))
          case "adminId" | "causedByUserId" => ("causedByUserId" -> long(v))
          case "id" | "pageId" => ("pageId" -> long(v))
          case "oldTitle" => ("oldTitle" -> string(v))
          case "newTitle" => ("newTitle" -> string(v))
          case "prefix" | "newTitlePrefix" => ("newTitlePrefix" -> string(v))
          case "newTitleWP" | "newTitleWithoutPrefix" => ("newTitleWithoutPrefix" -> string(v))
          case "oldNs" | "oldNamespace" => ("oldNamespace" -> int(v))
          case "oldNsIC" | "oldNamespaceIsContent" => ("oldNamespaceIsContent" -> boolean(v))
          case "newNs" | "newNamespace" => ("newNamespace" -> int(v))
          case "newNsIC" | "newNamespaceIsContent" => ("newNamespaceIsContent" -> boolean(v))
        }
      }.toMap
      new PageEvent(
        wikiDb = valueMap.getOrElse("wikiDb", wikiDb).get.asInstanceOf[String],
        timestamp = valueMap.getOrElse("timestamp", timestamp).get.asInstanceOf[Timestamp],
        eventType = valueMap.getOrElse("eventType", eventType).get.asInstanceOf[String],
        causedByUserId = valueMap.getOrElse("causedByUserId", causedByUserId).asInstanceOf[Option[Long]],
        pageId = valueMap.getOrElse("pageId", pageId).asInstanceOf[Option[Long]],
        oldTitle = valueMap.getOrElse("oldTitle", oldTitle).get.asInstanceOf[String],
        newTitle = valueMap.getOrElse("newTitle", newTitle).get.asInstanceOf[String],
        newTitlePrefix = valueMap.getOrElse("newTitlePrefix", newTitlePrefix).get.asInstanceOf[String],
        newTitleWithoutPrefix = valueMap.getOrElse("newTitleWithoutPrefix", newTitleWithoutPrefix).get.asInstanceOf[String],
        oldNamespace = valueMap.getOrElse("oldNamespace", oldNamespace).get.asInstanceOf[Int],
        oldNamespaceIsContent = valueMap.getOrElse("oldNamespaceIsContent", oldNamespaceIsContent).get.asInstanceOf[Boolean],
        newNamespace = valueMap.getOrElse("newNamespace", newNamespace).get.asInstanceOf[Int],
        newNamespaceIsContent = valueMap.getOrElse("newNamespaceIsContent", newNamespaceIsContent).get.asInstanceOf[Boolean]
      )
    }
  }

  def pageStateSet(
    wikiDb: Option[String] = Some("testwiki"),
    startTimestamp: Option[Timestamp] = None,
    endTimestamp: Option[Timestamp] = None,
    causedByEventType: Option[String] = Some("test"),
    causedByUserId: Option[Long] = Some(0L),
    pageId: Option[Long] = Some(1L),
    pageIdArtificial: Option[String] = None,
    title: Option[String] = Some("Title"),
    titleLatest: Option[String] = None,
    namespace: Option[Int] = Some(0),
    namespaceIsContent: Option[Boolean] = Some(true),
    namespaceLatest: Option[Int] = None,
    namespaceIsContentLatest: Option[Boolean] = None,
    pageCreationTimestamp: Option[Timestamp] = None,
    inferredFrom: Option[String] = None
  )(
    headerLine: String,
    stateLines: String*
  ): Seq[PageState] = {
    val headers = headerLine.split(" ").filter(_.nonEmpty)
    stateLines.map { line =>
      val values = line.split(" ").filter(_.nonEmpty)
      val valueMap = headers.zip(values).map { case (h, v) =>
        h match {
          case "wiki" | "db" | "wikiDb" => ("wikiDb" -> string(v))
          case "start" | "startTimestamp" => ("startTimestamp" -> timestamp(v))
          case "end" | "endTimestamp" => ("endTimestamp" -> timestamp(v))
          case "type" | "eventType" | "causedByEventType" => ("causedByEventType" -> string(v))
          case "adminId" | "causedByUserId" => ("causedByUserId" -> long(v))
          case "id" | "pageId" => ("pageId" -> long(v))
          case "artificial" | "pageIdArtificial" => ("pageIdArtificial" -> string(v))
          case "title" => ("title" -> string(v))
          case "titleL" | "titleLatest" => ("titleLatest" -> string(v))
          case "ns" | "namespace" => ("namespace" -> int(v))
          case "nsIC" | "namespaceIsContent" => ("namespaceIsContent" -> boolean(v))
          case "nsL" | "namespaceLatest" => ("namespaceLatest" -> int(v))
          case "nsLIC" | "namespaceLatestIsContent" => ("namespaceLatestIsContent" -> boolean(v))
          case "creation" | "pageCreationTimestamp" => ("pageCreationTimestamp" -> timestamp(v))
          case "inferred" | "inferredFrom" => ("inferredFrom" -> string(v))
        }
      }.toMap
      val titleVal = valueMap.getOrElse("title", title).get.asInstanceOf[String]
      val namespaceVal = valueMap.getOrElse("namespace", namespace).get.asInstanceOf[Int]
      val namespaceIsContentVal = valueMap.getOrElse("namespaceIsContent", namespaceIsContent).get.asInstanceOf[Boolean]
      new PageState(
        wikiDb = valueMap.getOrElse("wikiDb", wikiDb).get.asInstanceOf[String],
        startTimestamp = valueMap.getOrElse("startTimestamp", startTimestamp).asInstanceOf[Option[Timestamp]],
        endTimestamp = valueMap.getOrElse("endTimestamp", endTimestamp).asInstanceOf[Option[Timestamp]],
        causedByEventType = valueMap.getOrElse("causedByEventType", causedByEventType).get.asInstanceOf[String],
        causedByUserId = valueMap.getOrElse("causedByUserId", causedByUserId).asInstanceOf[Option[Long]],
        pageId = valueMap.getOrElse("pageId", pageId).asInstanceOf[Option[Long]],
        pageIdArtificial = valueMap.getOrElse("pageIdArtificial", pageIdArtificial).asInstanceOf[Option[String]],
        title = titleVal,
        titleLatest = valueMap.getOrElse("titleLatest", Some(titleLatest.getOrElse(titleVal))).get.asInstanceOf[String],
        namespace = namespaceVal,
        namespaceIsContent = namespaceIsContentVal,
        namespaceLatest = valueMap.getOrElse("namespaceLatest", Some(namespaceLatest.getOrElse(namespaceVal))).get.asInstanceOf[Int],
        namespaceIsContentLatest = valueMap.getOrElse("namespaceIsContentLatest", Some(namespaceIsContentLatest.getOrElse(namespaceIsContentVal))).get.asInstanceOf[Boolean],
        pageCreationTimestamp = valueMap.getOrElse("pageCreationTimestamp", pageCreationTimestamp).asInstanceOf[Option[Timestamp]],
        inferredFrom = valueMap.getOrElse("inferredFrom", inferredFrom).asInstanceOf[Option[String]]
      )
    }
  }
}
