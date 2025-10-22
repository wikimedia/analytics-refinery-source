package org.wikimedia.analytics.refinery.job.mediawikidumper

import org.wikimedia.analytics.refinery.tools.ProjectInfo

/** Case class representing SiteInfo for a specific wiki Generated with a query
  * similar to: select language as languageCode, sitename as siteName, dbname as
  * dbName, home_page as homePage, mw_version as mediaWikiVersion, case_setting
  * as caseSetting, collect_list(named_struct( 'code', namespace, 'name',
  * namespace_localized_name, 'caseSetting', namespace_case_setting,
  * 'isContent', namespace_is_content = 1 )) as namespaces from
  * mediawiki_project_namespace_map where snapshot='2023-09' and dbname =
  * 'simplewiki' group by language, sitename, dbname, home_page, mw_version,
  * case_setting;
  *
  * @param languageCode
  *   two letter language code
  * @param siteName
  *   also known as the "project family"
  * @param dbName
  *   name of the database
  * @param homePage
  *   each wiki has their own
  * @param mediaWikiVersion
  *   the version this wiki is running right now
  * @param caseSetting
  *   the case setting on this wiki (first-letter or case-sensitive)
  * @param namespaces
  *   the list of namespaces
  */
case class SiteInfo(
    languageCode: String,
    siteName: String,
    dbName: String,
    homePage: String,
    mediaWikiVersion: String,
    caseSetting: String,
    namespaces: List[NamespaceInfo]
) {

    // This Rendering follows the schema outlined at
    // https://www.mediawiki.org/xml/export-0.10.xsd
    def getXML: String = {
        val namespaceTags = namespaces.map { ns =>
            val prefix = {
                f"""<namespace key="${ns.code}" case="${ns.caseSetting}""""
            }
            if (ns.name.isEmpty) {
                prefix + " />"
            } else {
                prefix + f">${ns.name}</namespace>"
            }
        }
        f"""|  <siteinfo>
            |    <sitename>${siteName}</sitename>
            |    <dbname>${dbName}</dbname>
            |    <base>${homePage}</base>
            |    <generator>MediaWiki Content File Export ${ProjectInfo.version}</generator>
            |    <case>${caseSetting}</case>
            |    <namespaces>
            |      ${namespaceTags.mkString("\n      ")}
            |    </namespaces>
            |  </siteinfo>""".stripMargin
    }

    def correctedLanguageCode: String = {
        // Due to https://phabricator.wikimedia.org/T407902, a couple special wikis, such as commonswiki,
        // return a bad language code. Here we use a list of known bad language codes to instead default to en.

        val incorrectLanguageCodes = Set(
            "advisors",
            "board",
            "boardgovcom",
            "commons",
            "electcom",
            "foundation",
            "internal",
            "labs",
            "login",
            "mediawiki",
            "meta",
            "movementroles",
            "nostalgia",
            "outreach",
            "sources",
            "species",
            "techconduct",
            "testcommons",
            "testwikidata",
            "wikidata",
            "wikimania",
            "wikimania2005",
            "wikimania2006",
            "wikimania2007",
            "wikimania2008",
            "wikimania2009",
            "wikimania2010",
            "wikimania2011",
            "wikimania2012",
            "wikimania2013",
            "wikimania2014",
            "wikimania2015",
            "wikimania2016",
            "wikimania2017",
            "wikimania2018"
        )

        if (incorrectLanguageCodes.contains(languageCode))
            "en" // default to english
        else
            languageCode
    }
}

case class NamespaceInfo(
    code: String,
    name: String,
    caseSetting: String,
    isContent: Boolean
)
