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
    override def setId(id: Long) = push("id" -> id)
    override def getId = m("id").asInstanceOf[Long]
    override def setTimestamp(timestamp: String) = push("timestamp" -> timestamp)
    override def getTimestamp = m("timestamp").asInstanceOf[String]
    override def setPage(page: MwPage) = push("page" -> page.m)
    override def getPage = new MediawikiPageMap(m("page").asInstanceOf[Map[String, Any]])
    override def setUser(user: MwUser) = push("user" -> user.m)
    override def getUser = new MediawikiUserMap(m("user").asInstanceOf[Map[String, Any]])
    override def setMinor(minor: Boolean) = push("minor" -> minor)
    override def getMinor = m("minor").asInstanceOf[Boolean]
    override def setComment(comment: String) = push("comment" -> comment)
    override def getComment = m("comment").asInstanceOf[String]
    override def setBytes(bytes: Long) = push("bytes" -> bytes)
    override def getBytes = m("bytes").asInstanceOf[Long]
    override def setText(text: String) = push("text" -> text)
    override def getText = m("text").asInstanceOf[String]
    override def setSha1(sha1: String) = push("sha1" -> sha1)
    override def getSha1 = m("sha1").asInstanceOf[String]
    override def setParentId(parentId: Long) = push("parent_id" -> parentId)
    override def getParentId = m("parent_id").asInstanceOf[Long]
    override def setModel(model: String) = push("model" -> model)
    override def getModel = m("model").asInstanceOf[String]
    override def setFormat(format: String) = push("format" -> format)
    override def getFormat = m("format").asInstanceOf[String]
  }

  class MediawikiUserMap(m: Map[String, Any]) extends MapBasedObject(m) with MediawikiUser {
    type M =  MediawikiUserMap
    override def create(m: Map[String, Any]) = new MediawikiUserMap(m)
    override def setId(id: Long) = push("id" -> id)
    override def getId = m("id").asInstanceOf[Long]
    override def setUserText(userText: String) = push("text" -> userText)
    override def getUserText = m("text").asInstanceOf[String]
  }

  class MediawikiPageMap(m: Map[String, Any]) extends MapBasedObject(m) with MediawikiPage {
    type M =  MediawikiPageMap
    override def create(m: Map[String, Any]) = new MediawikiPageMap(m)
    override def setWikiDb(db: String) = push("wiki" -> db)
    override def getWikiDb = m("wiki").asInstanceOf[String]
    override def setId(id: Long) = push("id" -> id)
    override def getId = m("id").asInstanceOf[Long]
    override def setNamespace(ns: Long) = push("namespace" -> ns)
    override def getNamespace = m("namespace").asInstanceOf[Long]
    override def setTitle(title: String) = push("title" -> title)
    override def getTitle = m("title").asInstanceOf[String]
    override def setRedirectTitle(redirectTitle: String) = push("redirect" -> redirectTitle)
    override def getRedirectTitle = m("redirect").asInstanceOf[String]
    override def addRestriction(restriction: String) = push("restrictions" -> (m("restrictions").asInstanceOf[List[String]] :+ restriction))
    override def getRestrictions = m("restrictions").asInstanceOf[Seq[String]]
  }

  def makeDummyRevision = new MediawikiRevisionMap(Map(
    "id" -> -1L,
    "timestamp" -> "",
    "page" -> Map().asInstanceOf[Map[String, Any]],
    "user" -> Map().asInstanceOf[Map[String, Any]],
    "minor" -> false, // False by default
    "comment" -> "",
    "bytes" -> 0L,
    "text" -> "",
    "sha1" -> "",
    "parent_id" -> -1L,
    "model" -> "",
    "format" -> ""
  ))

  def makeDummyUser = new MediawikiUserMap(Map("id" -> -1L, "text" -> ""))

  def makeDummyPage = new MediawikiPageMap(Map(
    "wiki" -> "",
    "id" -> -1L,
    "namespace" -> -1L,
    "title" -> "",
    "redirect" -> "",
    "restrictions" -> List.empty[String]
  ))

}


