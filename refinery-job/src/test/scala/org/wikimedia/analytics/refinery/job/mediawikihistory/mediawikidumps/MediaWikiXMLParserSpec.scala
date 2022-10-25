package org.wikimedia.analytics.refinery.job.mediawikihistory.mediawikidumps

import java.io.ByteArrayInputStream
import org.scalatest.{FlatSpec, Matchers}


class MediaWikiXMLParserSpec extends FlatSpec with Matchers {

  val wiki = "dummy"
  val user = "<contributor><id>1</id><username>Foobar</username></contributor>"
  val page = "<page><id>1</id><title>Page title</title><ns>0</ns><restrictions>restriction 1</restrictions><restrictions>restriction 2</restrictions><redirect title=\"Test redirect title\" /></page>"
  val revision = "<revision><id>1</id><parentid>2</parentid><timestamp>2001-01-15T13:15:00Z</timestamp><contributor><username>Foobar</username></contributor><comment>test comment</comment><text>Test text\nWith new line.</text><sha1>test sha1</sha1><minor /><model>test model</model><format>test format</format></revision>"


  def parseUser[MwObjFact <: MediawikiObjectsFactory](factory: MwObjFact): MediawikiObjectsFactory#MwUser = {
    val mediaWikiXMLParser = new MediawikiXMLParser[MediawikiObjectsMapsFactory](new MediawikiObjectsMapsFactory, wiki)
    mediaWikiXMLParser.parseUser(
      mediaWikiXMLParser.initializeXmlStreamReader(
        new ByteArrayInputStream(user.getBytes("UTF-8"))))
  }

  def parsePage[MwObjFact <: MediawikiObjectsFactory](factory: MwObjFact): MediawikiObjectsFactory#MwPage = {
    val mediaWikiXMLParser = new MediawikiXMLParser[MediawikiObjectsMapsFactory](new MediawikiObjectsMapsFactory, wiki)
    mediaWikiXMLParser.parsePage(
      mediaWikiXMLParser.initializeXmlStreamReader(
        new ByteArrayInputStream(page.getBytes("UTF-8"))))
  }

  def parseRevision[MwObjFact <: MediawikiObjectsFactory](factory: MwObjFact): MediawikiObjectsFactory#MwRev = {
    val mediaWikiXMLParser = new MediawikiXMLParser[MediawikiObjectsMapsFactory](new MediawikiObjectsMapsFactory, wiki)
    mediaWikiXMLParser.parseRevision(
      mediaWikiXMLParser.initializeXmlStreamReader(
        new ByteArrayInputStream(revision.getBytes("UTF-8"))),
      mediaWikiXMLParser.mwObjectsFactory.makeDummyPage)
  }


  /***************************
   * Using MediawikiObjectsMapFactory
   */

  "MediawikiXMLParser" should "parse user as map" in {
    val userMap = parseUser[MediawikiObjectsMapsFactory](new MediawikiObjectsMapsFactory)
    userMap.getId should equal(1L)
    userMap.getUserText should equal("Foobar")
  }

  it should "parse page as map" in {
    val pageMap = parsePage[MediawikiObjectsMapsFactory](new MediawikiObjectsMapsFactory)
    pageMap.getWikiDb should equal("dummy")
    pageMap.getId should equal(1L)
    pageMap.getTitle should equal("Page title")
    pageMap.getNamespace should equal(0)
    pageMap.getRedirectTitle should equal("Test redirect title")
    pageMap.getRestrictions should equal(Seq("restriction 1", "restriction 2"))

  }

  it should "parse revision as map" in {
    val revisionMap = parseRevision[MediawikiObjectsMapsFactory](new MediawikiObjectsMapsFactory)
    revisionMap.getId should equal(1L)
    revisionMap.getParentId should equal(2L)
    revisionMap.getTimestamp should equal("2001-01-15T13:15:00Z")
    revisionMap.getUser.getId should equal(-1L)
    revisionMap.getUser.getUserText should equal("Foobar")
    revisionMap.getComment should equal("test comment")
    revisionMap.getText should equal("Test text\nWith new line.")
    revisionMap.getSha1 should equal("test sha1")
    revisionMap.getMinor should equal(true)
    revisionMap.getModel should equal("test model")
    revisionMap.getFormat should equal("test format")
    revisionMap.getPage.getId should equal(-1L)
    revisionMap.getPage.getTitle should equal("")
  }

  /***************************
   * Using MediawikiCaseClassesFactory
   */

  it should "parse user as class" in {
    val userClass = parseUser[MediawikiObjectsCaseClassesFactory](new MediawikiObjectsCaseClassesFactory)
    userClass.getId should equal(1L)
    userClass.getUserText should equal("Foobar")
  }

  it should "parse page as class" in {
    val pageClass = parsePage[MediawikiObjectsCaseClassesFactory](new MediawikiObjectsCaseClassesFactory)
    pageClass.getWikiDb should equal("dummy")
    pageClass.getId should equal(1L)
    pageClass.getTitle should equal("Page title")
    pageClass.getNamespace should equal(0)
    pageClass.getRedirectTitle should equal("Test redirect title")
    pageClass.getRestrictions should equal(Seq("restriction 1", "restriction 2"))

  }

  it should "parse revision as class" in {
    val revisionClass = parseRevision[MediawikiObjectsCaseClassesFactory](new MediawikiObjectsCaseClassesFactory)
    revisionClass.getId should equal(1L)
    revisionClass.getParentId should equal(2L)
    revisionClass.getTimestamp should equal("2001-01-15T13:15:00Z")
    revisionClass.getUser.getId should equal(-1L)
    revisionClass.getUser.getUserText should equal("Foobar")
    revisionClass.getComment should equal("test comment")
    revisionClass.getText should equal("Test text\nWith new line.")
    revisionClass.getSha1 should equal("test sha1")
    revisionClass.getMinor should equal(true)
    revisionClass.getModel should equal("test model")
    revisionClass.getFormat should equal("test format")
    revisionClass.getPage.getId should equal(-1L)
    revisionClass.getPage.getTitle should equal("")
  }

}