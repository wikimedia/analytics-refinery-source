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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.Collections;

import org.wikimedia.analytics.refinery.core.webrequest.WebrequestData;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Mostly static functions to implement the Wikimedia pageview definition.
 * This class was originally created while reading https://gist.github.com/Ironholds/96558613fe38dd4d1961
 *
 * Thread-safety: The singleton could be used in a multi-thread context. It doesn't hold mutable states. In the same
 * time, the Caffeine cache is designed to be accessed concurrently and is conveniently shared between threads.
 *
 * TODO: Isolate all static methods into their own class.
 */
@ThreadSafe
public class PageviewDefinition {

    /**
     * Meta-methods to enable lazy instantiation in a singleton-based way.
     * In non-Java terms: you get to only create one class instance, and only when you need it, instead of always having
     * everything (eager instantiation) or always generating everything anew (!singletons). So we have:
     *   1- an instance created in the helper when the helper class is requested;
     *   2- a private constructor (to avoid people just calling the constructor);
     *   3- an actual getInstance method to retrieve the instance through the helper
     *
     * https://codepumpkin.com/double-checked-locking-singleton/#InnerClassSingleton
     */
    private static class SingletonHelper {
        private static final PageviewDefinition INSTANCE = new PageviewDefinition();
    }

    private PageviewDefinition() {}

    public static PageviewDefinition getInstance(){
        return SingletonHelper.INSTANCE;
    }

    // Avoid multiple calls to Webrequest.getInstance from `isPageview`.
    private final Webrequest webrequest = Webrequest.getInstance();

    public static final String UNKNOWN_LANGUAGE_VARIANT_VALUE = "-";
    public static final String UNKNOWN_PAGE_TITLE_VALUE = "-";
    public static final String DEFAULT_LANGUAGE_VARIANT_VALUE = "default";

    /**
     * All API request uriPaths will contain this
     */
    public static final String URI_PATH_API = "api.php";
    public static final String URI_PATH_REST_API = "/api/rest_v";


    /** Most special pages do not denote content consumption **/
    private static final Set<String> SPECIAL_PAGES_ACCEPTED = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "search",
            "recentchanges",
            "version",
            "viewobject",
            "allevents"
    )));

    private final static Pattern URI_PATH_PATTERN = Pattern.compile(
        "^(/sr(-(ec|el))?|/w(iki)?|/v(iew)?|/zh(-(cn|hans|hant|hk|mo|my|sg|tw))?)/"
    );

    private final static Pattern PATTERN = Pattern.compile(
        "\\?((cur|old)id|title|search)="
    );


    private final static Pattern URI_HOST_WIKIMEDIA_DOMAIN_PATTERN = Pattern.compile(
        // any of these domain names
        "^(?!doc)" // subdomains of wikimedia.org included in pageview datasets
            + "(advisory|commons|foundation|incubator|meta|outreach|species|strategy|usability|wikimania|wikitech|[a-zA-Z]{2,3})\\."
            + "((m|mobile|wap|zero)\\.)?"  // followed by an optional mobile or zero qualifier
            + "wikimedia\\.org$"    // ending with wikimedia.org
    );
    private final static Pattern URI_HOST_PROJECT_DOMAIN_PATTERN = Pattern.compile(
        // starting with a letter but not starting with "www" "test", "donate" or "arbcom"
        "^((?!www)(?!donate)(?!arbcom)(?!sysop)([a-zA-Z][a-zA-Z0-9-_]*)\\.)*"
            + "(wik(ibooks|"  // match project domains ending in .org
            + "inews|ipedia|iquote|isource|tionary|iversity|ivoyage))\\.org$"
    );
    private final static Pattern URI_HOST_OTHER_PROJECTS_PATTERN = Pattern.compile(
        "^((?!test)(?!query)([a-zA-Z0-9-_]+)\\.)*"  // not starting with "test" or "query"
            + "(wikifunctions|wikidata|mediawiki|wikimediafoundation)\\.org$"  // match project domains ending in .org
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
     * Simple regex to reject illegal title characters.
     *
     * Note that this regex essentially rejects the inverse of $wgLegalTitleChars.
     * Must be kept in sync with $wgLegalTitleChars.
     * See: https://www.mediawiki.org/wiki/Manual:$wgLegalTitleChars , https://phabricator.wikimedia.org/T245468
     */
    private static final Pattern TITLE_VALID_PATTERN = Pattern.compile(
        "\\A[^#<>\\[\\]{}\\|\\x00-\\x1F\\x7F]+\\z"
    );
    /**
     * Maximum possible title length in bytes.
     * Although a title can only be 255 bytes, some Special pages may be longer
     * because they include a title as a substring.  This comes from the magic
     * number hardcoded into MediaWikiTitleCodec::splitTitleString
     */
    private static final int TITLE_MAX_LENGTH = 512;

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
        final Map<String, String> xAnalyticsHeader = data.getXAnalyticsHeader();
        final boolean isTaggedPageview = xAnalyticsHeader
                .getOrDefault("pageview", "")
                .trim()
                .equalsIgnoreCase("1");
        return (data.isAppUserAgent() && isTaggedPageview);
    }
    
    
    private boolean isWebPageview(WebrequestData data) {
        return (
                // check for a regular pageview contentType, or a an API contentType
                (Webrequest.isTextHTMLContentType(data.getContentType()) &&
                     !Utilities.stringContains(data.getUriPath(), PageviewDefinition.URI_PATH_API))

                        // Either a pageview's uriPath will match the first pattern,
                        // or its uriQuery will match the second
                        &&  (
                        Utilities.patternIsFound(URI_PATH_PATTERN, data.getUriPath())
                                || Utilities.patternIsFound(PATTERN, data.getUriQuery())
                )


                        // Edits now come through as text/html. They should not be included.
                        // Luckily the query parameter does not seem to be localised.
                        && !Utilities.patternIsFound(uriQueryUnwantedActions, data.getUriQuery())
        );

    }


    /**
     * Webrequest has an isWMFDomain method that can be used to filter HTTP hostnames
     * for those controlled by WMF, but this is not enough for pageviews.
     * There are many WMF HTTP hostnames that will never have any pageviews.
     * This function returns true if the hostname is one that might have pageviews.
     * @param hostname
     * @return
     */
    private boolean isPageviewHostname(String hostname) {
        hostname = hostname.toLowerCase();
        // `getUnchecked` is used in place of `get` as the loading function is not throwing exceptions.
        return pageviewHostnameCache.get(hostname);
    }

    private static boolean computeIsPreviewHostname(String hostname) {
        return (
                Utilities.patternIsFound(URI_HOST_WIKIMEDIA_DOMAIN_PATTERN, hostname) ||
                        Utilities.patternIsFound(URI_HOST_OTHER_PROJECTS_PATTERN, hostname) ||
                        Utilities.patternIsFound(URI_HOST_PROJECT_DOMAIN_PATTERN, hostname)
        );
    }

    // Initialization of the Pageview Hostname cache
    //
    // In an analysis of a days worth of webrequest data, there were under 5000 distinct HTTP hostnames. The
    // isPageviewHostname method of this class uses the above regexes. Caching the result of a given hostname check in
    // this Cache speeds up webrequest -> pageview processing.
    private LoadingCache<String, Boolean> pageviewHostnameCache = Caffeine.newBuilder()
            .maximumSize(10_000)
            .build(PageviewDefinition::computeIsPreviewHostname);

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
     * The following tags can show up in X-Analytics.  If they do, they disqualify the
     * request from counting as a pageview:
     *   - page previews tagged as preview=1
     *   - test and load test requests tagged as debug=1
     *
     * We use the raw X-Analytics header rather than x_analytics_map
     * to make sure this function can be applied
     * to raw data, where the parsing of X-Analytics header into
     * a map has not yet happened.
     *
     * @param  data
     *
     * @return  boolean
     */
    public boolean isPageview(WebrequestData data) {
        if (
            Webrequest.isSuccess(data.getHttpStatus()) &&
            webrequest.isWMFHostname(data.getUriHost()) &&
            isPageviewHostname(data.getUriHost()) &&
            pageDenotesContentConsumption(data)
        ) {
            // Check if it is an app pageview if it was not a web one.
            return ((!data.isAppUserAgent() && isWebPageview(data)) || isAppPageview(data)) ;

        } else {
            return false;
        }
    }

    /**
     * False if page at hand is not considered to denote content consumption
     *
     * Have in mind that pageview definition tries
     * to count pageviews of content delivered to users, not actions
     *
     * Most Special pages are excluded as of 2019-07
     * this was a long standing bug:
     * https://phabricator.wikimedia.org/T226730
     *
     * @return
     */
    private boolean pageDenotesContentConsumption(WebrequestData data){

        // if x-analytics header is empty just move on, we cannot infer anything
        if (data.getXAnalyticsHeader().isEmpty())
            return true;

        final Map<String, String> xAnalyticsHeader = data.getXAnalyticsHeader();
        String special = xAnalyticsHeader.getOrDefault("special", "").trim().toLowerCase();
        String preview = xAnalyticsHeader.getOrDefault("preview", "").trim();
        String debug = xAnalyticsHeader.getOrDefault("debug", "").trim();

        // in the absence of these tags, there is little we can infer about the request
        if (preview.isEmpty() && special.isEmpty() && debug.isEmpty()) {
            return true;
        } else if (preview.equals("1") || debug.equals("1")) {
            return false;
        }

        return SPECIAL_PAGES_ACCEPTED.contains(special);
    }

    /**
     * Normalize uriPath to maximize dialect and page title extraction correctness
     * Normalization export path if uriPath is a complete URL, and removes double backslashes
     *
     * @param uriPath The url's path
     * @return The normalized uriPath
     */
    private static String normalizeUriPath(String uriPath) {
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

        return normPath;
    }

    /**
     * Identifies the language variant from a pageview uriPath
     * NOTE: Provides correct result only if used with is_pageview = true
     *
     * @param uriPath The url's path
     * @return The language variant name (if any)
     */
    public static String getLanguageVariantFromPath(String uriPath) {
        // Normalize uriPath
        String normPath = normalizeUriPath(uriPath);

        // In case of api, unknown language variant
        if (normPath.startsWith("/w/api.php"))
            return PageviewDefinition.UNKNOWN_LANGUAGE_VARIANT_VALUE;

        // Default wiki urls, default language variant
        if (normPath.equals("/") || normPath.equals("/wiki")
                || normPath.equals("/w") || normPath.startsWith(URI_PATH_REST_API)
                || normPath.startsWith("/wiki/") || normPath.startsWith("/w/"))
            return PageviewDefinition.DEFAULT_LANGUAGE_VARIANT_VALUE;

        // Wikifunctions style language variant in the path as in:
        //   /view/<language-variant>/<page-title>
        if (normPath.startsWith("/view/")) {
            int langStartIdx = normPath.indexOf("/", 4);
            int langStopIdx = normPath.indexOf("/", langStartIdx + 1);
            if (langStartIdx > 0 && langStopIdx > langStartIdx) {
                return normPath.substring(langStartIdx + 1, langStopIdx);
            } else {
                return PageviewDefinition.DEFAULT_LANGUAGE_VARIANT_VALUE;
            }
        }

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
     * If path contains /something/, extract title after that, like:
     * /something/horseshoe_crab -> horseshoe crab
     *
     * @param path the whole address to search for `find`
     * @param find if not found in `path`, return nothing
     * @return the title, or the empty string if not found
     */
    private static String getTitleAfter(String path, String find) {
        int startIdx = path.indexOf(find);
        int len = find.length();

        if (startIdx >= 0) {
            return path.substring(startIdx + len);
        } else {
            return "";
        }
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
    private static String getPageTitleFromPath(String path) {

        if (path == null || path.isEmpty()) {
            return PageviewDefinition.UNKNOWN_PAGE_TITLE_VALUE;
        }

        // If the path contains an anchor we don't care about it here
        String mainPath = path.substring(0, path.indexOf("#") > 0 ? path.indexOf("#") : (path.length()));

        String found = getTitleAfter(mainPath, "/api/rest_v1/page/mobile-sections-lead/");
        if(found.length() > 0) {
            return found;
        }

        found = getTitleAfter(mainPath, "/api/rest_v1/page/mobile-sections/");
        if(found.length() > 0) {
            return found;
        }

        found = getTitleAfter(mainPath, "/api/rest_v1/page/mobile-html/");
        if(found.length() > 0) {
            return found;
        }

        found = getTitleAfter(mainPath, "/wiki/");
        if(found.length() > 0) {
            return found;
        }

        found = getTitleAfter(mainPath, "/w/");
        if(found.length() > 0) {
            return found;
        }

        found = getTitleAfter(mainPath, "/view/");
        if (found.length() > 0) {
            // Currently this is only wikifunctions and it looks like
            //   /view/<language-variant>/<page-title>
            int startIdx = found.lastIndexOf("/");
            if (startIdx > 0) {
                return found.substring(startIdx + 1);
            }
        }

        // Else assume we are in /language_variant/Page_title case
        // for instance /zh-hant/Wikipedia:首页
        // Find second "/" in path as substring beginning
        int startIdx = mainPath.indexOf("/", mainPath.indexOf("/") + 1);
        if (startIdx > 0) {
            return mainPath.substring(startIdx + 1);
        }

        // Case not covered, return unknown value
        return PageviewDefinition.UNKNOWN_PAGE_TITLE_VALUE;
    }

    /**
     * Identifies a page title from a pageview uriPath and uriQuery.
     * Normalize page title by lowering case and decoding URI characters.
     * NOTE: Provides correct result only if used with is_pageview = true
     *
     * @param uriPath The url's path
     * @param uriQuery The url's query
     * @return The decoded page title name or UNKNOWN_PAGE_TITLE_VALUE
     */
    public static String getPageTitleFromUri(String uriPath, String uriQuery) {
        // Normalize uriPath
        String normPath = normalizeUriPath(uriPath);

        // General case of page title in path
        boolean pathWiki = ((normPath.contains("/wiki/") && (! normPath.contains("index.")))
                || normPath.contains("/w/index.php/")
                || normPath.startsWith("/api/rest_v1/page/mobile-sections-lead/")
                || normPath.startsWith("/api/rest_v1/page/mobile-sections/")
                || normPath.startsWith("/api/rest_v1/page/mobile-html/")
        );

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
        pageTitle = PercentDecoder.decode(pageTitle).replaceAll(" ", "_");

        // Reject invalid titles.  Note that some endpoints allow an "optional"
        // title parameter but ignore it without validation.  The title we see
        // here may be outdated or completely wrong, until T152628 is fixed.
        // We'll just catch the obvious stuff for now.

        int pageTitleLength = pageTitle.getBytes(StandardCharsets.UTF_8).length;
        if (pageTitleLength > TITLE_MAX_LENGTH || !isValidPageTitle(pageTitle)) {
            return PageviewDefinition.UNKNOWN_PAGE_TITLE_VALUE;
        }

        return pageTitle;
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
            // in, we already established those are one of a redirect

            WebrequestData fakeData = new WebrequestData(data.getUriHost(),
                data.getUriPath(), data.getUriQuery(), "200",
                "text/html", data.getUserAgent(), data.getXAnalyticsHeader());

            return isPageview(fakeData);

        }   else {
            return false;
        }

    }

    public static boolean isValidPageTitle(String pageTitle) {
        return Utilities.patternIsFound(TITLE_VALID_PATTERN, pageTitle);
    }
}
