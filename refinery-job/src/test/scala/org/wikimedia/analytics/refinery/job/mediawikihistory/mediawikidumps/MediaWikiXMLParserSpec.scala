package org.wikimedia.analytics.refinery.job.mediawikihistory.mediawikidumps

import java.io.ByteArrayInputStream
import org.scalatest.{FlatSpec, Matchers}


class MediaWikiXMLParserSpec extends FlatSpec with Matchers {

  val wiki = "dummy"
  val user = "<contributor><id>1</id><username>Foobar</username></contributor>"
  val userIp = "<contributor><ip>123.234.432.321</ip></contributor>"
  val page = "<page><id>1</id><title>Page title</title><ns>0</ns><restrictions>restriction 1</restrictions><restrictions>restriction 2</restrictions><redirect title=\"Test redirect title\" /></page>"
  val revision = "<revision><id>1</id><parentid>2</parentid><timestamp>2001-01-15T13:15:00Z</timestamp><contributor><username>Foobar</username></contributor><comment>test comment</comment><text bytes=\"14\">Test text\nWith new line.</text><sha1>test sha1</sha1><model>test model</model><format>test format</format></revision>"
  val revisionIpEditor = "<revision><id>1</id><parentid>2</parentid><timestamp>2001-01-15T13:15:00Z</timestamp><contributor><ip>123.234.432.321</ip></contributor><comment>test comment</comment><text>Test text\nWith new line.</text><sha1>test sha1</sha1><model>test model</model><format>test format</format></revision>"
  val revisionMinor = "<revision><id>1</id><parentid>2</parentid><timestamp>2001-01-15T13:15:00Z</timestamp><contributor><username>Foobar</username></contributor><comment>test comment</comment><text>Test text\nWith new line.</text><sha1>test sha1</sha1><minor /><model>test model</model><format>test format</format></revision>"
  val revisionNoParentId = "<revision><id>1</id><timestamp>2001-01-15T13:15:00Z</timestamp><contributor><username>Foobar</username></contributor><comment>test comment</comment><text>Test text\nWith new line.</text><sha1>test sha1</sha1><model>test model</model><format>test format</format></revision>"
  val revisionUserDeleted = "<revision><id>1</id><parentid>2</parentid><timestamp>2001-01-15T13:15:00Z</timestamp><contributor deleted=\"deleted\" /><comment>test comment</comment><text>Test text\nWith new line.</text><sha1>test sha1</sha1><model>test model</model><format>test format</format></revision>"
  val revisionCommentDeleted = "<revision><id>1</id><parentid>2</parentid><timestamp>2001-01-15T13:15:00Z</timestamp><contributor><username>Foobar</username></contributor><comment deleted=\"deleted\" /><text>Test text\nWith new line.</text><sha1>test sha1</sha1><model>test model</model><format>test format</format></revision>"
  val revisionTextDeleted = "<revision><id>1</id><parentid>2</parentid><timestamp>2001-01-15T13:15:00Z</timestamp><contributor><username>Foobar</username></contributor><comment>test comment</comment><text bytes=\"13\" deleted=\"deleted\" /><sha1 /><model>test model</model><format>test format</format></revision>"


  def parseUser(factory: MediawikiObjectsFactory, user: String): MediawikiObjectsFactory#MwUser = {
    val mediaWikiXMLParser = new MediawikiXMLParser(factory, wiki)
    mediaWikiXMLParser.parseUser(
      mediaWikiXMLParser.initializeXmlStreamReader(
        new ByteArrayInputStream(user.getBytes("UTF-8"))
      )
    )
  }

  def parsePage(factory: MediawikiObjectsFactory, page: String): MediawikiObjectsFactory#MwPage = {
    val mediaWikiXMLParser = new MediawikiXMLParser(factory, wiki)
    mediaWikiXMLParser.parsePage(
      mediaWikiXMLParser.initializeXmlStreamReader(
        new ByteArrayInputStream(page.getBytes("UTF-8"))
      )
    )
  }

  def parseRevision(factory: MediawikiObjectsFactory, revision: String): MediawikiObjectsFactory#MwRev = {
    val mediaWikiXMLParser = new MediawikiXMLParser(factory, wiki)
    mediaWikiXMLParser.parseRevision(
      mediaWikiXMLParser.initializeXmlStreamReader(
        new ByteArrayInputStream(revision.getBytes("UTF-8"))
      ),
      mediaWikiXMLParser.mwObjectsFactory.makeDummyPage
    )
  }

  "MediawikiXMLParser" should "parse users" in {
    val mapsFactory = new MediawikiObjectsMapsFactory
    val caseClassFactory = new MediawikiObjectsCaseClassesFactory

    val userMap = parseUser(mapsFactory, user)
    val userCaseClass = parseUser(caseClassFactory, user)
    val userIpMap = parseUser(mapsFactory, userIp)
    val userIpCaseClass = parseUser(caseClassFactory, userIp)

    userMap.getId should equal(Some(1L))
    userMap.getUserText should equal("Foobar")
    userCaseClass.getId should equal(Some(1L))
    userCaseClass.getUserText should equal("Foobar")

    userIpMap.getId should equal(None)
    userIpMap.getUserText should equal("123.234.432.321")
    userIpCaseClass.getId should equal(None)
    userIpCaseClass.getUserText should equal("123.234.432.321")
  }

  it should "parse pages" in {
    val mapsFactory = new MediawikiObjectsMapsFactory
    val caseClassFactory = new MediawikiObjectsCaseClassesFactory

    val pageMap = parsePage(mapsFactory, page)
    val pageCaseClass = parsePage(caseClassFactory, page)

    pageMap.getWikiDb should equal("dummy")
    pageMap.getId should equal(1L)
    pageMap.getTitle should equal("Page title")
    pageMap.getNamespace should equal(0)
    pageMap.getRedirectTitle should equal("Test redirect title")
    pageMap.getRestrictions should equal(Seq("restriction 1", "restriction 2"))

    pageCaseClass.getWikiDb should equal("dummy")
    pageCaseClass.getId should equal(1L)
    pageCaseClass.getTitle should equal("Page title")
    pageCaseClass.getNamespace should equal(0)
    pageCaseClass.getRedirectTitle should equal("Test redirect title")
    pageCaseClass.getRestrictions should equal(Seq("restriction 1", "restriction 2"))
  }

  it should "parse revisions" in {
    val mapsFactory = new MediawikiObjectsMapsFactory
    val caseClassFactory = new MediawikiObjectsCaseClassesFactory

    val revisionMap = parseRevision(mapsFactory, revision)
    val revisionIpEditorMap = parseRevision(mapsFactory, revisionIpEditor)
    val revisionMinorMap = parseRevision(mapsFactory, revisionMinor)
    val revisionNoParentIdMap = parseRevision(mapsFactory, revisionNoParentId)
    val revisionUserDeletedMap = parseRevision(mapsFactory, revisionUserDeleted)
    val revisionCommentDeletedMap = parseRevision(mapsFactory, revisionCommentDeleted)
    val revisionTextDeletedMap = parseRevision(mapsFactory, revisionTextDeleted)

    val revisionCaseClass = parseRevision(caseClassFactory, revision)
    val revisionIpEditorCaseClass = parseRevision(caseClassFactory, revisionIpEditor)
    val revisionMinorCaseClass = parseRevision(caseClassFactory, revisionMinor)
    val revisionNoParentIdCaseClass = parseRevision(caseClassFactory, revisionNoParentId)
    val revisionUserDeletedCaseClass = parseRevision(caseClassFactory, revisionUserDeleted)
    val revisionCommentDeletedCaseClass = parseRevision(caseClassFactory, revisionCommentDeleted)
    val revisionTextDeletedCaseClass = parseRevision(caseClassFactory, revisionTextDeleted)

    revisionMap.getId should equal(1L)
    revisionMap.getParentId should equal(Some(2L))
    revisionMap.getTimestamp should equal("2001-01-15T13:15:00Z")
    revisionMap.getUser.getId should equal(Some(-1L))
    revisionMap.getUser.getUserText should equal("Foobar")
    revisionMap.getComment should equal("test comment")
    revisionMap.getText should equal("Test text\nWith new line.")
    revisionMap.getBytes should equal(14)
    revisionMap.getSha1 should equal("test sha1")
    revisionMap.getMinor should equal(false)
    revisionMap.getModel should equal("test model")
    revisionMap.getFormat should equal("test format")
    revisionMap.getPage.getId should equal(-1L)
    revisionMap.getPage.getTitle should equal("")
    revisionMap.getUserIsVisible should equal(true)
    revisionMap.getCommentIsVisible should equal(true)
    revisionMap.getContentIsVisible should equal(true)

    revisionCaseClass.getId should equal(1L)
    revisionCaseClass.getParentId should equal(Some(2L))
    revisionCaseClass.getTimestamp should equal("2001-01-15T13:15:00Z")
    revisionCaseClass.getUser.getId should equal(Some(-1L))
    revisionCaseClass.getUser.getUserText should equal("Foobar")
    revisionCaseClass.getComment should equal("test comment")
    revisionCaseClass.getText should equal("Test text\nWith new line.")
    revisionCaseClass.getBytes should equal(14)
    revisionCaseClass.getSha1 should equal("test sha1")
    revisionCaseClass.getMinor should equal(false)
    revisionCaseClass.getModel should equal("test model")
    revisionCaseClass.getFormat should equal("test format")
    revisionCaseClass.getPage.getId should equal(-1L)
    revisionCaseClass.getPage.getTitle should equal("")
    revisionCaseClass.getUserIsVisible should equal(true)
    revisionCaseClass.getCommentIsVisible should equal(true)
    revisionCaseClass.getContentIsVisible should equal(true)

    // only test the exceptional parts of each exceptional revision
    revisionIpEditorMap.getUser.getId should equal(None)
    revisionIpEditorCaseClass.getUser.getId should equal(None)

    revisionMinorMap.getMinor should equal(true)
    revisionMinorCaseClass.getMinor should equal(true)

    revisionNoParentIdMap.getParentId should equal(None)
    revisionNoParentIdCaseClass.getParentId should equal(None)

    revisionUserDeletedCaseClass.getUserIsVisible should equal(false)
    revisionUserDeletedCaseClass.getUser.getId should equal(Some(-1))
    revisionUserDeletedCaseClass.getUser.getUserText should equal("")
    revisionUserDeletedMap.getUserIsVisible should equal(false)
    revisionUserDeletedMap.getUser.getId should equal(Some(-1))
    revisionUserDeletedMap.getUser.getUserText should equal("")

    revisionCommentDeletedCaseClass.getCommentIsVisible should equal(false)
    revisionCommentDeletedCaseClass.getComment should equal("")
    revisionCommentDeletedMap.getCommentIsVisible should equal(false)
    revisionCommentDeletedMap.getComment should equal("")

    revisionTextDeletedCaseClass.getContentIsVisible should equal(false)
    revisionTextDeletedCaseClass.getBytes should equal(13)
    revisionTextDeletedCaseClass.getText should equal("")
    revisionTextDeletedMap.getContentIsVisible should equal(false)
    revisionTextDeletedMap.getBytes should equal(13)
    revisionTextDeletedMap.getText should equal("")
  }
}