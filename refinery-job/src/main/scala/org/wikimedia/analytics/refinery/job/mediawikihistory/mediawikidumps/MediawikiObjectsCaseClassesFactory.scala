package org.wikimedia.analytics.refinery.job.mediawikihistory.mediawikidumps

/**
 * Implementation of the [[MediawikiObjectsFactory]] using case classes.
 */
class MediawikiObjectsCaseClassesFactory extends MediawikiObjectsFactory {

  type MwRev = Revision
  type MwUser = User
  type MwPage = Page

  case class Revision(
    id: Long,
    timestamp: String,
    page: Page,
    user: User,
    minor: Boolean,
    comment: String,
    bytes: Long,
    text: String,
    sha1: String,
    parentId: Long,
    model: String,
    format: String
  ) extends MediawikiRevision {
    def setId(id: Long): Revision = this.copy(id = id)
    def getId: Long = id
    def setTimestamp(timestamp: String): Revision = this.copy(timestamp = timestamp)
    def getTimestamp: String = timestamp
    def setPage(page: Page): Revision = this.copy(page = page)
    def getPage: Page = page
    def setUser(user: User): Revision = this.copy(user = user)
    def getUser: User = user
    def setMinor(minor: Boolean): Revision = this.copy(minor = minor)
    def getMinor: Boolean = minor
    def setComment(comment: String): Revision = this.copy(comment = comment)
    def getComment: String = comment
    def setBytes(bytes: Long): Revision = this.copy(bytes = bytes)
    def getBytes: Long = bytes
    def setText(text: String): Revision = this.copy(text = text)
    def getText: String = text
    def setSha1(sha1: String): Revision = this.copy(sha1 = sha1)
    def getSha1: String = sha1
    def setParentId(parentId: Long): Revision = this.copy(parentId = parentId)
    def getParentId: Long = parentId
    def setModel(model: String): Revision = this.copy(model = model)
    def getModel: String = model
    def setFormat(format: String): Revision = this.copy(format = format)
    def getFormat: String = format
  }

  case class User(
    id: Long,
    userText: String
  ) extends MediawikiUser {
    def setId(id: Long): User = this.copy(id = id)
    def getId: Long = id
    def setUserText(userText: String): User = this.copy(userText = userText)
    def getUserText: String = userText
  }

  case class Page(
    wiki: String,
    id: Long,
    namespace: Long,
    title: String,
    redirectTitle: String,
    restrictions: Seq[String]
  ) extends MediawikiPage {
    def setWikiDb(db: String) = this.copy(wiki = db)
    def getWikiDb: String = this.wiki
    def setId(id: Long): Page = this.copy(id = id)
    def getId: Long = id
    def setNamespace(namespace: Long): Page = this.copy(namespace = namespace)
    def getNamespace: Long = namespace
    def setTitle(title: String): Page = this.copy(title = title)
    def getTitle: String = title
    def setRedirectTitle(redirectTitle: String): Page = this.copy(redirectTitle = redirectTitle)
    def getRedirectTitle: String = redirectTitle
    def addRestriction(restriction: String): Page = this.copy(restrictions = this.restrictions :+ restriction)
    def getRestrictions: Seq[String] = restrictions
  }


  def makeDummyRevision: Revision = new Revision(
    -1L,                     // id
    "",                      // timestamp
    makeDummyPage,   // page
    makeDummyUser,    // user
    false,                   // minor
    "",                      // comment
    0L,                      // bytes
    "",                      // text
    "",                      // sha1
    -1L,                     // parent_id
    "",                      // model
    ""                       // format
  )

  def makeDummyUser: User = new User(
    -1L, // id
    ""   // text
  )

  def makeDummyPage: Page = new Page(
    "",                  // wiki
    -1L,                 // id
    -1L,                 // namespace
    "",                  // title
    "",                  // redirect
    List.empty[String]   // restrictions
  )

}
