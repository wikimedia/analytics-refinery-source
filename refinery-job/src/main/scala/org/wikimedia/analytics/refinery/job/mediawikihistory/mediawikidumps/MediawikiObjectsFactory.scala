package org.wikimedia.analytics.refinery.job.mediawikihistory.mediawikidumps

/**
 * Trait defining mediawiki-objects to parse.
 * Subtypes are:
 *  + MwUser for users
 *  + MwPage for pages
 *  + MwRev for revisions (uses MwUser and MwPage)
 */
trait MediawikiObjectsFactory {

  type MwRev <: MediawikiRevision
  type MwUser <: MediawikiUser
  type MwPage <: MediawikiPage

  trait MediawikiRevision {
    def setId(id: Long): MwRev
    def getId: Long
    def setTimestamp(timestamp: String): MwRev
    def getTimestamp: String
    def setPage(page: MwPage): MwRev
    def getPage: MwPage
    def setUser(contributor: MwUser): MwRev
    def getUser: MwUser
    def setMinor(minor: Boolean): MwRev
    def getMinor: Boolean
    def setComment(comment: String): MwRev
    def getComment: String
    def setBytes(bytes: Long): MwRev
    def getBytes: Long
    def setText(text: String): MwRev
    def getText: String
    def setSha1(sha1: String): MwRev
    def getSha1: String
    def setParentId(parentId: Long): MwRev
    def getParentId: Long
    def setModel(model: String): MwRev
    def getModel: String
    def setFormat(format: String): MwRev
    def getFormat: String
  }

  trait MediawikiUser {
    def setId(id: Long): MwUser
    def getId: Long
    def setUserText(userText: String): MwUser
    def getUserText: String
  }

  trait MediawikiPage {
    def setWikiDb(db: String): MwPage
    def getWikiDb: String
    def setId(id: Long): MwPage
    def getId: Long
    def setNamespace(ns: Long): MwPage
    def getNamespace: Long
    def setTitle(title: String): MwPage
    def getTitle: String
    def setRedirectTitle(redirectTitle: String): MwPage
    def getRedirectTitle: String
    def addRestriction(restriction: String): MwPage
    def getRestrictions: Seq[String]
  }

  def makeDummyRevision: MwRev
  def makeDummyUser: MwUser
  def makeDummyPage: MwPage

}