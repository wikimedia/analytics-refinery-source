/**
 * Copyright (C) 2014  Wikimedia Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wikimedia.analytics.refinery.core;

import org.wikimedia.analytics.refinery.core.webrequest.WebrequestData;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Static functions to implement the Wikimedia pageview definition.
 * This class was orignally created while reading https://gist.github.com/Ironholds/96558613fe38dd4d1961
 */
public class PageviewDefinition {

    /*
     * Meta-methods to enable eager instantiation in a singleton-based way.
     * in non-Java terms: you get to only create one class instance, and only
     * when you need it, instead of always having everything (static/eager instantiation)
     * or always generating everything anew (!singletons). So we have:
     * (1) an instance;
     * (2) an empty constructor (to avoid people just calling the constructor);
     * (3) an actual getInstance method to allow for instantiation.
     */
    private static final PageviewDefinition instance = new PageviewDefinition();

    private PageviewDefinition() {}

    public static PageviewDefinition getInstance(){
        return instance;
    }

    public static final Set<String> URI_PORTIONS_TO_REMOVE = new HashSet<String>(Arrays.asList(
        "m",
        "mobile",
        "wap",
        "zero",
        "www",
        "download"
    ));

    /**
     * Static values for project, dialect and article
     */
    public static final String UNKNOWN_PROJECT_VALUE = "-";
    public static final String UNKNOWN_LANGUAGE_VARIANT_VALUE = "-";
    public static final String UNKNOWN_PAGE_TITLE_VALUE = "-";
    public static final String DEFAULT_LANGUAGE_VARIANT_VALUE = "default";

    /**
     * All API request uriPaths will contain this
     */
    public static final String URI_PATH_API = "api.php";


    /*
     * Now back to the good part.
     */
    private final Pattern uriHostWikimediaDomainPattern = Pattern.compile(
        // any of these domain names
        "^(commons|foundation|meta|incubator|species|outreach|usability|strategy|advisory|[a-zA-Z]{2,3})\\."
        + "((m|mobile|wap|zero)\\.)?"  // followed by an optional mobile or zero qualifier
        + "wikimedia\\.org$"    // ending with wikimedia.org
    );

    private final Pattern uriHostProjectDomainPattern = Pattern.compile(
        "^((?!www)(?!donate)(?!arbcom)([a-zA-Z0-9-_]+)\\.)*"     // not starting with "www" "test", "donate" or "arbcom"
        + "(wik(ibooks|"                                                // match project domains ending in .org
        + "inews|ipedia|iquote|isource|tionary|iversity|ivoyage))\\.org$"
    );

    //

    private final Pattern uriHostOtherProjectsPattern = Pattern.compile(
        "^((?!test)([a-zA-Z0-9-_]+)\\.)*"                                          // not starting with "test"
        + "(wikidata|mediawiki|wikimediafoundation)\\.org$"                        // match project domains ending in .org
    );

    private final Pattern uriPathPattern = Pattern.compile(
        "^(/sr(-(ec|el))?|/w(iki)?|/zh(-(cn|hans|hant|hk|mo|my|sg|tw))?)/"
    );

    private final Pattern uriQueryPattern = Pattern.compile(
        "\\?((cur|old)id|title|search)="
    );

    private final Pattern uriPathUnwantedSpecialPagesPattern = Pattern.compile(
        "BannerRandom|HideBanners|CentralAutoLogin|MobileEditor|Undefined"
	+ "MobileMenu|BlankPage|UserLogin|ZeroRatedMobileAccess"
    );

    private final Pattern uriQueryUnwantedSpecialPagesPattern = Pattern.compile(
        "CentralAutoLogin|MobileEditor|UserLogin|ZeroRatedMobileAccess"
    );

    private final Pattern uriQueryUnwantedActions = Pattern.compile(
        /**
         * This action-pattern is used with [[Utilities.patternIsFound]] function,
         * and therefore it covers all three cases of interest:
         *  - action=edit
         *  - veaction=edit
         *  - veaction=editsource
         *  Let's be careful if any of those change.
         */
        "action=edit" +
        /**
         * This action-pattern is included in 2017-02-0X. It removes actions like "previews" from
         * being counted as pageviews.
         */
        "|action=submit"
    );



    /**
     * Given a webrequest URI path, query and user agent,
     * returns true if we consider this an app (API) pageview.
     *
     * If x-analytics header includes pageview=1 we do not do any further check
     * and return true.
     *
     * Note that the logic here is /NOT COMPLETE/. It checks
     * to see if the request is an app pageview, but not
     * (for example) whether it actually completed.
     *
     * See: https://wikitech.wikimedia.org/wiki/X-Analytics#Keys
     * for x-analytics info.
     *
     * Please note that requests tagged as 'preview' are not counted
     * as pageviews.
     *
     * We let apps decide whether a request is a pageview by tagging it as such
     * on x-analytics header, if pageview=1 is present
     * we do not look further at urls.
     *
     * We use the raw xAnalytics header rather than x_analytics_map
     * to make sure this function can be applied
     * to raw data, where the parsing of x-Analytics header into
     * a map has not yet happened.
     *
     * @param   data   WebrequestData object
     *
     * @return  boolean
     */
    private boolean isAppPageview(WebrequestData data) {

        final String appContentType     = "application/json";
        final String appUserAgent       = "WikipediaApp";
        final String appPageURIQuery    = "sections=0";
        final String iosAppPageURIQuery = "sections=all";
        final Pattern iosUserAgentPattern = Pattern.compile("iPhone|iOS");
        final String iOsAppUserAgent    = "Wikipedia/5.0.";


        if (Utilities.getValueForKey(data.getRawXAnalyticsHeader(), "preview").trim().equalsIgnoreCase("1"))
            return false;

        boolean isTaggedPageview = (Utilities.getValueForKey(data.getRawXAnalyticsHeader(), "pageview").trim().equalsIgnoreCase("1"));

        return (Utilities.stringContains(data.getContentType(), appContentType)
                && (Utilities.stringContains(data.getUserAgent(),   appUserAgent)
                    || (Utilities.stringContains(data.getUserAgent(),   iOsAppUserAgent)))

                && (isTaggedPageview ||
                (
                    Utilities.stringContains(data.getUriPath(), PageviewDefinition.URI_PATH_API) &&
                    (Utilities.stringContains(data.getUriQuery(), appPageURIQuery)
                     || (Utilities.stringContains(data.getUriQuery(), iosAppPageURIQuery)
                         && Utilities.patternIsFound(iosUserAgentPattern, data.getUserAgent()))
                    )
               )
            ));
    }


    private boolean isWebPageview(WebrequestData data) {
        return (
                // check for a regular pageview contentType, or a an API contentType
                (Webrequest.isTextHTMLContentType(data.getContentType()) &&
                     !Utilities.stringContains(data.getUriPath(), PageviewDefinition.URI_PATH_API))

                        // Either a pageview's uriPath will match the first pattern,
                        // or its uriQuery will match the second
                        &&  (
                        Utilities.patternIsFound(uriPathPattern, data.getUriPath())
                                || Utilities.patternIsFound(uriQueryPattern, data.getUriQuery())
                )

                        // A pageview will not have these Special: pages in the uriPath or uriQuery
                        && !Utilities.patternIsFound(uriPathUnwantedSpecialPagesPattern, data.getUriPath())
                        && !Utilities.patternIsFound(uriQueryUnwantedSpecialPagesPattern, data.getUriQuery())
                        // Edits now come through as text/html. They should not be included.
                        // Luckily the query parameter does not seem to be localised.
                        && !Utilities.patternIsFound(uriQueryUnwantedActions, data.getUriQuery())
        );

    }



    /**
     * Given a webrequest URI host, path, query user agent http status and content type,
     * returns true if we consider this a 'pageview', false otherwise.
     *
     * If x-analytics header includes pageview=1 we do not do any further check
     * and return true.
     *
     * <p>
     * See: https://meta.wikimedia.org/wiki/Research:Page_view/Generalised_filters
     *      for information on how to classify a pageview.
     *
     * See: https://wikitech.wikimedia.org/wiki/X-Analytics#Keys
     * for x-analytics info.
     *
     * Please note that requests tagged as 'preview' are not counted
     * as pageviews.
     *
     * We use the raw xAnalytics header rather than x_analytics_map
     * to make sure this function can be applied
     * to raw data, where the parsing of x-Analytics header into
     * a map has not yet happened.
     *
     * @param  data
     *
     * @return  boolean
     */
    public boolean isPageview(WebrequestData data) {

        if (Utilities.getValueForKey(data.getRawXAnalyticsHeader(), "preview").trim().equalsIgnoreCase("1"))
            return false;


        boolean successRequestForSupportedProject = Webrequest.isSuccess(data.getHttpStatus())
                // A pageview must be from either a wikimedia.org domain,
                // or a 'project' domain, e.g. en.wikipedia.org
                && ( Utilities.patternIsFound(uriHostWikimediaDomainPattern,  data.getUriHost())
                || Utilities.patternIsFound(uriHostOtherProjectsPattern, data.getUriHost())
                || Utilities.patternIsFound(uriHostProjectDomainPattern, data.getUriHost()));


      //check if it is  an app pageview if it was not a web one

      return successRequestForSupportedProject
          && (isWebPageview(data)
              || isAppPageview(data));

    }

    /**
     * Identifies a project from a pageview uriHost
     * NOTE: Provides correct result only if used with is_pageview = true
     *
     * @param uriHost The url's host
     * @return The project identifier in format [xxx.]xxxx (en.wikipedia or wikisource for instance)
     */
    public String getProjectFromHost(String uriHost) {
        if (uriHost == null) return PageviewDefinition.UNKNOWN_PROJECT_VALUE;
        String[] uri_parts = uriHost.toLowerCase().split("\\.");
        switch (uri_parts.length) {
            // case wikixxx.org
            case 2:
                return uri_parts[0];
            //case xx.wikixxx.org - Remove unwanted parts
            case 3:
                if (PageviewDefinition.URI_PORTIONS_TO_REMOVE.contains(uri_parts[0]))
                    return uri_parts[1];
                else
                    return uri_parts[0] + "." + uri_parts[1];
            //xx.[m|mobile|wap|zero].wikixxx.org - Remove unwanted parts
            case 4:
                if (PageviewDefinition.URI_PORTIONS_TO_REMOVE.contains(uri_parts[0]))
                    return uri_parts[2];
                else
                    return uri_parts[0] + "." + uri_parts[2];
            //xx.[m|mobile|wap|zero].[m|mobile|wap|zero].wikixxx.org - Remove unwanted parts
            case 5:
                if (PageviewDefinition.URI_PORTIONS_TO_REMOVE.contains(uri_parts[0]))
                    return uri_parts[3];
                else
                    return uri_parts[0] + "." + uri_parts[3];
            default:
                return PageviewDefinition.UNKNOWN_PROJECT_VALUE;
        }
    }

    /**
     * Normalize uriPath to maximize dialect and page title extraction correctness
     * Normalization export path if uriPath is a complete URL, and removes double backslashes
     *
     * @param uriPath The url's path
     * @return The normalized uriPath
     */
    private String normalizeUriPath(String uriPath) {
        // Prevent null pointer exception
        String normPath = (uriPath == null) ? "" : uriPath;

        // Special case where full url ends-up in uriPath
        // Extract path manually to prevent url encoding issues
        int idxpathBeginning = 0;
        if (normPath.startsWith("http"))
            idxpathBeginning = normPath.indexOf("/",  9); // look for "/" after http(s)://

        int idxPathEnding = normPath.indexOf("?",  idxpathBeginning); // look for query "?" after path
        idxPathEnding = (idxPathEnding < 0) ? normPath.length() : idxPathEnding;

        normPath = normPath.substring(idxpathBeginning, idxPathEnding);


        // Clean uriPath of double backslashes
        normPath = normPath.replaceAll("//+", "/");
        normPath = normPath.trim();

        return normPath;
    }

    /**
     * Identifies the language variant from a pageview uriPath
     * NOTE: Provides correct result only if used with is_pageview = true
     *
     * @param uriPath The url's path
     * @return The language variant name (if any)
     */
    public String getLanguageVariantFromPath(String uriPath) {
        // Normalize uriPath
        String normPath = normalizeUriPath(uriPath);

        // In case of api, unknown language variant
        if (normPath.startsWith("/w/api.php"))
            return PageviewDefinition.UNKNOWN_LANGUAGE_VARIANT_VALUE;

        // Default wiki urls, default language variant
        if (normPath.equals("/") || normPath.equals("/wiki")
                || normPath.equals("/w") || normPath.startsWith("/api/rest_v1")
                || normPath.startsWith("/wiki/") || normPath.startsWith("/w/"))
            return PageviewDefinition.DEFAULT_LANGUAGE_VARIANT_VALUE;

        // Special language variant case
        // LanguageVariant examples are zh-hans, zh-hk, or sr-rc or sr-el
        // return  language variant value if it contains a "-"
        // or return default language variant if it doesn't (zh alone for instance)
        // uriPath example with language variant /zh-hant/Wikipedia:首页
        // uriPath example with default language variant /zh/Wikipedia:首页
        // Manual extraction instead of regex for performance.
        int startIdx = normPath.indexOf("/");
        startIdx = (startIdx >= 0) ? (startIdx + 1) : startIdx;
        int middleIdx = normPath.indexOf("-", startIdx);
        int endIdx = normPath.indexOf("/", startIdx);
        endIdx = (endIdx > 0) ? endIdx : (normPath.length());
        if ((startIdx >= 0) && (startIdx < endIdx)) {
            if ((middleIdx > 0) && (middleIdx < endIdx))
                return normPath.substring(startIdx, endIdx);
            else
                return PageviewDefinition.DEFAULT_LANGUAGE_VARIANT_VALUE;
        }

        // extraction failed, unknown language variant
        return PageviewDefinition.UNKNOWN_LANGUAGE_VARIANT_VALUE;

    }

    /**
     * Extracts the page title name from uriPath
     * NOTE: - Assumes that the page is not "index.*".
     *       - Provides correct result only if used with
     *       is_pageview = true (this method is supposedly
     *       called only by getPageTitleFromUri.
     *
     * @param path The url's path
     * @return The page title name or UNKNOWN_PAGE_TITLE_VALUE
     */
    private String getPageTitleFromPath(String path) {

        // If the path contains an anchor
        // remove it from the result substring
        int endIdx = path.indexOf("#");
        endIdx = (endIdx > 0)?endIdx:(path.length());

        // If path contains /api/rest_v1/page/, extract substring from there
        // for instance: /wiki/horseshoe_crab -> horseshoe_crab
        int startIdx = path.indexOf("/api/rest_v1/page/mobile-sections-lead/") ;
        startIdx = (startIdx >= 0) ? (startIdx + "/api/rest_v1/page/mobile-sections-lead/".length()) : startIdx;
        if ((startIdx >= 0) && (startIdx < endIdx))
            return path.substring(startIdx, endIdx);

        // If path contains /wiki/, extract substring from there
        // for instance: /wiki/horseshoe_crab -> horseshoe_crab
        startIdx = path.indexOf("/wiki/") ;
        startIdx = (startIdx >= 0) ? (startIdx + "/wiki/".length()) : startIdx;
        if ((startIdx >= 0) && (startIdx < endIdx))
            return path.substring(startIdx, endIdx);

        // If path contains /w/, extract substring from there
        // for instance /w/horseshoe_crab -> horseshoe_crab
        startIdx = path.indexOf("/w/");
        startIdx = (startIdx >= 0) ? (startIdx + "/w/".length()) : startIdx;
        if ((startIdx >= 0) && (startIdx < endIdx))
            return path.substring(startIdx, endIdx);

        // Else assume we are in /language_variant/Page_title case
        // for instance /zh-hant/Wikipedia:首页
        // Find second "/" in path as substring beginning
        startIdx = path.indexOf("/", path.indexOf("/") + 1);
        startIdx = (startIdx >= 0) ? (startIdx + 1) : startIdx;
        if ((startIdx > 0) && (startIdx < endIdx))
            return path.substring(startIdx, endIdx);

        //Case not covered, return unknown value
        return PageviewDefinition.UNKNOWN_PAGE_TITLE_VALUE;
    }

    /**
     * Identifies a page title from a pageview uriPath and uriQuery.
     * Normalize page title by lowering case and decoding URI characters.
     * NOTE: Provides correct result only if used with is_pageview = true
     *
     * @param uriPath The url's path
     * @param uriQuery The url's query
     * @return The decoded page title name or UNKNOWN_PROJECT_VALUE
     */
    public String getPageTitleFromUri(String uriPath, String uriQuery) {
        // Normalize uriPath
        String normPath = normalizeUriPath(uriPath);

        // General case of page title in path
        boolean pathWiki = ((normPath.contains("/wiki/") && (! normPath.contains("index.")))
                || normPath.contains("/w/index.php/")|| normPath.startsWith("/api/rest_v1/page/mobile-sections-lead/"));


        String titleQueryParam = null;
        String pageQueryParam = null;


        // Extract first instance of title and page query parameters
        // Done manually to decode using our own percent decoder
        // Explicitly replace '+' by '_' to get closer to page titles from path
        uriQuery = (uriQuery == null) ? "" : uriQuery; // Prevent null pointer exception
        uriQuery = (uriQuery.startsWith("?")) ? uriQuery.substring(1): uriQuery; // Remove ? if any
        final String[] pairs = uriQuery.split("&");
        for (String pair : pairs) {
            final int idx = pair.indexOf("=");
            final String key = idx > 0 ? pair.substring(0, idx) : pair;
            if ((key.equals("title") && (titleQueryParam == null)))
                titleQueryParam = idx > 0 && pair.length() > idx + 1 ? pair.substring(idx + 1).replace("+", "_") : null;
            if ((key.equals("page")) && (pageQueryParam == null))
                pageQueryParam = idx > 0 && pair.length() > idx + 1 ? pair.substring(idx + 1).replace("+", "_") : null;
            if ((titleQueryParam != null) && (pageQueryParam != null))
                break;
        }

        String pageTitle = PageviewDefinition.UNKNOWN_PAGE_TITLE_VALUE;

        // Depending on case, extract page title from path or query parameter

        if (titleQueryParam != null)
            pageTitle = titleQueryParam;
        else if (pathWiki)
            pageTitle = getPageTitleFromPath(normPath);
        else if (pageQueryParam != null)
            pageTitle = pageQueryParam;
        else if (! normPath.contains("index."))
            pageTitle = getPageTitleFromPath(normPath);

        // Normalize Decoding URL percent characters (if any)
        return PercentDecoder.decode(pageTitle).replaceAll(" ", "_");

    }

    /**
     * Encapsulates the logic to identify redirects requests (301, 302, 307)
     * to a request that will be considered a pageview.
     *
     * For example a request to http://es.wikipedia.org/SomePage made by a mobile device
     * will be redirecteded to http://es.m.wikipedia.org/SomePage
     *
     * Cookies on *.wikimedia.org domain will be set on the 1st request, the redirect
     * Cookies on es.m.wikipedia will be set on teh 2nd (200) request
     *
     * The only difference between a "redirect to pageview"
     * and a "pageview" is that http code is not 200
     * and that content-type header might not be set.
     *
     * @param  data  WebrequestData object
     * @return boolean
     */
    public boolean isRedirectToPageview(WebrequestData data) {
        //we check first whether this is a redirect

        if (Webrequest.isRedirect(data.getHttpStatus()) &&
            (Webrequest.isTextHTMLContentType(data.getContentType()) ||
                data.getContentType().trim().equals("-"))
            ){

            // if the criteria for redirect is met see if everything else
            // mets criteria for a pageview
            // we "swap" the contentType and http status code of the data passed
            // in, we already stablished those are one of a redirect

            WebrequestData fakeData = new WebrequestData(data.getUriHost(),
                data.getUriPath(), data.getUriQuery(), "200",
                "text/html", data.getUserAgent(), data.getRawXAnalyticsHeader());

            return isPageview(fakeData);

        }   else {
            return false;
        }

    }

}
