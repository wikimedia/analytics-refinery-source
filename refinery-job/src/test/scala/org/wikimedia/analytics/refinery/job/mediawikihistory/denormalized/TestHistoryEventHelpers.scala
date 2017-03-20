package org.wikimedia.analytics.refinery.job.mediawikihistory.denormalized

object TestHistoryEventHelpers {

  import org.wikimedia.analytics.refinery.job.mediawikihistory.user.UserState
  import org.wikimedia.analytics.refinery.job.mediawikihistory.TestHelpers._

  val emptyMwEvent = new MediawikiEvent(wikiDb = "", eventEntity = "", eventType = "")

  def fakeUserState(userId: Long) = new UserState(
    wikiDb = "",
    userId = userId,
    causedByEventType = "",
    userName = "",
    userNameLatest = "")

  def revisionMwEventSet(
                          wikiDb: Option[String] = Some("testwiki"),
                          eventEntity: String = "revision",
                          eventType: String = "create",
                          eventTimestamp: Option[String] = Some("20010115000000"),

                          eventUserId: Option[Long] = Some(1L),

                          pageId: Option[Long] = Some(1L),

                          revId: Option[Long] = Some(1L),
                          revParentId: Option[Long] = Some(0L),
                          revMinorEdit: Option[Boolean] = Some(false),
                          revTextBytes: Option[Long] = Some(100L),
                          revTextBytesDiff: Option[Long] = Some(100L),
                          revTextSha1: Option[String] = Some("falsesha1"),
                          revContentModel: Option[String] = None,
                          revContentFormat: Option[String] = None,
                          revIsDeleted: Option[Boolean] = None,
                          revDeletedTimestamp: Option[String] = None,
                          revIsIdentityReverted: Option[Boolean] = None,
                          revFirstIdentityRevertingRevisionId: Option[Long] = None,
                          revFirstIdentityRevertTimestamp: Option[String] = None,
                          revIsProductive: Option[Boolean] = Some(true),
                          revIsIdentityRevert: Option[Boolean] = None
                        )(
                          headerLine: String,
                          mwEventLines: String*
  ): Seq[MediawikiEvent] = {
    val headers = headerLine.split(" ").filter(_.nonEmpty)
    mwEventLines.map { line =>
      val values = line.split(" ").filter(_.nonEmpty)
      val valueMap = headers.zip(values).map { case (h, v) =>
        h match {
          case "wiki" | "db" | "wikiDb" => ("wikiDb" -> string(v))
          case "ts" | "time" | "timestamp" => ("eventTimestamp" -> string(v))
          case "eventUserId" | "userId" => ("eventUserId" -> long(v))
          case "pageId" => ("pageId" -> long(v))
          case "revId" | "revisionId" => ("revId" -> long(v))
          case "revParentId" | "parentId" => ("revParentId" -> long(v))
          case "bytes" | "revTextBytes" | "textBytes" => ("revTextBytes" -> long(v))
          case "bytesDiff" | "revTextBytesDiff" | "textBytesDiff" => ("revTextBytesDiff" -> long(v))
          case "sha1" | "revSha1" | "revTextSha1" => ("revTextSha1" -> string(v))
          case "isDeleted" | "revIsDeleted" => ("revIsDeleted" -> boolean(v))
          case "deleteTime" | "revDeletedTimestamp" => ("revDeletedTimestamp" -> string(v))
          case "isReverted" | "reverted" | "revIsIdentityReverted" => ("revIsIdentityReverted" -> boolean(v))
          case "revertId" | "revFirstIdentityRevertingRevisionId" => ("revFirstIdentityRevertingRevisionId" -> long(v))
          case "revertTs" | "revFirstIdentityRevertTimestamp" => ("revFirstIdentityRevertTimestamp" -> string(v))
          case "productive" | "revIsProductive" => ("revIsProductive" -> boolean(v))
          case "isRevert" | "revert" | "revIsIdentityRevert" => ("revIsIdentityRevert" -> boolean(v))

        }
      }.toMap

      new MediawikiEvent(
        wikiDb = valueMap.getOrElse("wikiDb", wikiDb).get.asInstanceOf[String],
        eventEntity = eventEntity,
        eventType = eventType,
        eventTimestamp = valueMap.getOrElse("eventTimestamp", eventTimestamp).asInstanceOf[Option[String]],

        eventUserDetails = new MediawikiEventUserDetails(
          userId = valueMap.getOrElse("eventUserId", eventUserId).asInstanceOf[Option[Long]]
        ),

        pageDetails = new MediawikiEventPageDetails(
          pageId = valueMap.getOrElse("pageId", eventUserId).asInstanceOf[Option[Long]]
        ),

        revisionDetails = new MediawikiEventRevisionDetails(
          revId = valueMap.getOrElse("revId", revId).asInstanceOf[Option[Long]],
          revParentId = valueMap.getOrElse("revParentId", revParentId).asInstanceOf[Option[Long]],
          revTextBytes = valueMap.getOrElse("revTextBytes", revTextBytes).asInstanceOf[Option[Long]],
          revTextBytesDiff = valueMap.getOrElse("revTextBytesDiff", revTextBytesDiff).asInstanceOf[Option[Long]],
          revTextSha1 = valueMap.getOrElse("revTextSha1", revTextSha1).asInstanceOf[Option[String]],
          revIsDeleted = valueMap.getOrElse("revIsDeleted", revIsDeleted).asInstanceOf[Option[Boolean]],
          revDeletedTimestamp = valueMap.getOrElse("revDeletedTimestamp", revDeletedTimestamp).asInstanceOf[Option[String]],
          revIsIdentityReverted = valueMap.getOrElse("revIsIdentityReverted", revIsIdentityReverted).asInstanceOf[Option[Boolean]],
          revFirstIdentityRevertingRevisionId = valueMap.getOrElse("revFirstIdentityRevertingRevisionId", revFirstIdentityRevertingRevisionId).asInstanceOf[Option[Long]],
          revFirstIdentityRevertTimestamp = valueMap.getOrElse("revFirstIdentityRevertTimestamp", revFirstIdentityRevertTimestamp).asInstanceOf[Option[String]],
          revIsProductive = valueMap.getOrElse("revIsProductive", revIsProductive).asInstanceOf[Option[Boolean]],
          revIsIdentityRevert = valueMap.getOrElse("revIsIdentityRevert", revIsIdentityRevert).asInstanceOf[Option[Boolean]]
        )
      )
    }
  }
}