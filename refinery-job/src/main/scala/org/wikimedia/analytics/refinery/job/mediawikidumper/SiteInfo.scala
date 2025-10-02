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
}

case class NamespaceInfo(
    code: String,
    name: String,
    caseSetting: String,
    isContent: Boolean
)
