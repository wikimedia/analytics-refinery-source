package org.wikimedia.analytics.refinery.job.mediawikihistory.mediawikidumps

/**
 * Implementation of the [[MediawikiObjectsFactory]] using Maps.
 *
 * This implementation is useful to facilitate JSON-conversion
 * from the mediawiki-objects.
 */
class MediawikiObjectsMapsFactory extends MediawikiObjectsFactory {

  type MwRev = MediawikiRevisionMap
  type MwUser = MediawikiUserMap
  type MwPage = MediawikiPageMap

  /**
   * Abstract class defining a map-based object with
   * an updating facilitation function push
   * @param m The map containing the object values
   */
  abstract class MapBasedObject(val m: Map[String, Any]) {
    type M <: MapBasedObject
    def create(m: Map[String, Any]): M
    def push(kvPair: (String, Any)): M = create(this.m + kvPair)
  }

  class MediawikiRevisionMap(m: Map[String, Any]) extends MapBasedObject(m) with MediawikiRevision {
    type M =  MediawikiRevisionMap
    override def create(m: Map[String, Any]) = new MediawikiRevisionMap(m)
    override def setId(id: Long): M = push("id" -> id)
    override def getId: Long = m("id").asInstanceOf[Long]
    override def setTimestamp(timestamp: String): M = push("timestamp" -> timestamp)
    override def getTimestamp: String = m("timestamp").asInstanceOf[String]
    override def setPage(page: MwPage): M = push("page" -> page.m)
    override def getPage = new MediawikiPageMap(m("page").asInstanceOf[Map[String, Any]])
    override def setUser(user: MwUser): M = push("user" -> user.m)
    override def getUser = new MediawikiUserMap(m("user").asInstanceOf[Map[String, Any]])
    override def setMinor(minor: Boolean): M = push("minor" -> minor)
    override def getMinor: Boolean = m("minor").asInstanceOf[Boolean]
    override def setComment(comment: String): M = push("comment" -> comment)
    override def getComment: String = m("comment").asInstanceOf[String]
    override def setBytes(bytes: Long): MediawikiRevisionMap = push("bytes" -> bytes)
    override def getBytes: Long = m("bytes").asInstanceOf[Long]
    override def setText(text: String): M = push("text" -> text)
    override def getText: String = m("text").asInstanceOf[String]
    override def setSha1(sha1: String): M = push("sha1" -> sha1)
    override def getSha1: String = m("sha1").asInstanceOf[String]
    override def setParentId(parentId: Option[Long]): M = push("parent_id" -> parentId)
    override def getParentId: Option[Long] = m("parent_id").asInstanceOf[Option[Long]]
    override def setModel(model: String): M = push("model" -> model)
    override def getModel: String = m("model").asInstanceOf[String]
    override def setFormat(format: String): M = push("format" -> format)
    override def getFormat: String = m("format").asInstanceOf[String]
    override def setUserIsVisible(userIsVisible: Boolean): M = push("user_is_visible" -> userIsVisible)
    override def getUserIsVisible: Boolean = m("user_is_visible").asInstanceOf[Boolean]
    override def setCommentIsVisible(commentIsVisible: Boolean): M = push("comment_is_visible" -> commentIsVisible)
    override def getCommentIsVisible: Boolean = m("comment_is_visible").asInstanceOf[Boolean]
    override def setContentIsVisible(ContentIsVisible: Boolean): M = push("content_is_visible" -> ContentIsVisible)
    override def getContentIsVisible: Boolean = m("content_is_visible").asInstanceOf[Boolean]
  }

  class MediawikiUserMap(m: Map[String, Any]) extends MapBasedObject(m) with MediawikiUser {
    type M =  MediawikiUserMap
    override def create(m: Map[String, Any]) = new MediawikiUserMap(m)
    override def setId(id: Option[Long]): M = push("id" -> id)
    override def getId: Option[Long] = m("id").asInstanceOf[Option[Long]]
    override def setUserText(userText: String): MediawikiUserMap = push("text" -> userText)
    override def getUserText: String = m("text").asInstanceOf[String]
  }

  class MediawikiPageMap(m: Map[String, Any]) extends MapBasedObject(m) with MediawikiPage {
    type M =  MediawikiPageMap
    override def create(m: Map[String, Any]) = new MediawikiPageMap(m)
    override def setWikiDb(db: String): MediawikiPageMap = push("wiki" -> db)
    override def getWikiDb: String = m("wiki").asInstanceOf[String]
    override def setId(id: Long): M = push("id" -> id)
    override def getId: Long = m("id").asInstanceOf[Long]
    override def setNamespace(ns: Long): M = push("namespace" -> ns)
    override def getNamespace: Long = m("namespace").asInstanceOf[Long]
    override def setTitle(title: String): M = push("title" -> title)
    override def getTitle: String = m("title").asInstanceOf[String]
    override def setRedirectTitle(redirectTitle: String): M = push("redirect" -> redirectTitle)
    override def getRedirectTitle: String = m("redirect").asInstanceOf[String]
    override def addRestriction(restriction: String): M = push("restrictions" -> (m("restrictions").asInstanceOf[List[String]] :+ restriction))
    override def getRestrictions: Seq[String] = m("restrictions").asInstanceOf[Seq[String]]
  }

  def makeDummyRevision = new MediawikiRevisionMap(Map(
    "id" -> -1L,
    "timestamp" -> "",
    "page" -> makeDummyPage.m,
    "user" -> makeDummyUser.m,
    "minor" -> false,
    "comment" -> "",
    "bytes" -> 0L,
    "text" -> "",
    "sha1" -> "",
    "parent_id" -> None,
    "model" -> "",
    "format" -> "",
    "user_is_visible" -> true,
    "comment_is_visible" -> true,
    "content_is_visible" -> true,
  ))

  def makeDummyUser = new MediawikiUserMap(Map("id" -> Some(-1L), "text" -> ""))

  def makeDummyPage = new MediawikiPageMap(Map(
    "wiki" -> "",
    "id" -> -1L,
    "namespace" -> -1L,
    "title" -> "",
    "redirect" -> "",
    "restrictions" -> List.empty[String]
  ))

}


