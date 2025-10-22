package org.wikimedia.analytics.refinery.job.mediawikidumper

import org.scalatest.{FlatSpec, Matchers}
import org.wikimedia.analytics.refinery.tools.LogHelper

class SiteInfoSpec
  extends FlatSpec
    with Matchers
    with LogHelper {

  "correctedLanguageCode" should "return a valid languangeCode or default to english" in {
    val siteInfoWithGoodLang = SiteInfo(
      languageCode = "en",
      siteName = "english",
      dbName = "enwiki",
      homePage = "http://...",
      mediaWikiVersion = "123",
      caseSetting = "?",
      namespaces = List()
    )
    siteInfoWithGoodLang.correctedLanguageCode should equal("en")

    val siteInfoWithBadLang = siteInfoWithGoodLang.copy(languageCode = "commons")
    siteInfoWithBadLang.correctedLanguageCode should equal("en")
  }


}
