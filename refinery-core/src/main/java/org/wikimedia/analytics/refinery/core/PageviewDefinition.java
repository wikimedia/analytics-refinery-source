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

import java.util.regex.Pattern;
import java.util.HashSet;
import java.util.Arrays;

/**
 * Static functions to work with Wikimedia webrequest data.
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

    private PageviewDefinition() {
    }

    public static PageviewDefinition getInstance(){
        return instance;
    }

    /*
     * Now back to the good part.
     */
    private final Pattern uriHostWikimediaDomainPattern = Pattern.compile(
        "(commons|meta|incubator|species|outreach)\\."   // any of these domain names
        + "((m|mobile|wap|zero)\\.)?"                    // followed by an optional mobile or zero qualifier
        + "wikimedia\\.org$"                             // ending with wikimedia.org
    );

    private final Pattern uriHostProjectDomainPattern = Pattern.compile(
        "(?<!(www\\.|test))"              // not starting with "www." or "test"
        + "(wik(ibooks|"                  // match project domains ending in .org
        + "inews|ipedia|iquote|isource|tionary|iversity|ivoyage))\\.org$"
    );

    private final Pattern uriHostOtherProjectsPattern = Pattern.compile(
        "(?<!test)"                                          // not starting with "test"
        + "(wikidata|mediawiki|wikimediafoundation)\\.org$"  // match project domains ending in .org
    );

    private final Pattern uriPathPattern = Pattern.compile(
        "^(/sr(-(ec|el))?|/w(iki)?|/zh(-(cn|hans|hant|hk|mo|my|sg|tw))?)/"
    );

    private final Pattern uriQueryPattern = Pattern.compile(
        "\\?((cur|old)id|title|search)="
    );

    private final Pattern uriPathUnwantedSpecialPagesPattern = Pattern.compile(
        "BannerRandom|CentralAutoLogin|MobileEditor|Undefined|UserLogin|ZeroRatedMobileAccess"
    );

    private final Pattern uriQueryUnwantedSpecialPagesPattern = Pattern.compile(
        "CentralAutoLogin|MobileEditor|UserLogin|ZeroRatedMobileAccess"
    );

    private final Pattern uriQueryUnwantedActions = Pattern.compile(
        "action=edit"
    );

    private final HashSet<String> contentTypesSet = new HashSet<String>(Arrays.asList(
        "text/html",
        "text/html; charset=iso-8859-1",
        "text/html; charset=ISO-8859-1",
        "text/html; charset=utf-8",
        "text/html; charset=UTF-8"
    ));

    private final HashSet<String> httpStatusesSet = new HashSet<String>(Arrays.asList(
        "200",
        "304"
    ));

    private final HashSet<String> uriPortionsToRemove = new HashSet<String>(Arrays.asList(
            "m",
            "mobile",
            "wap",
            "zero",
            "www",
            "download"
    ));

    /**
     * All API request uriPaths will contain this
     */
    private final String uriPathAPI = "api.php";

    /**
     * Given a webrequest URI path, query and user agent,
     * returns true if we consider this an app (API) pageview.
     * Note that the logic here is /NOT COMPLETE/. It checks
     * to see if the request is an app pageview, but not
     * (for example) whether it actually completed.
     *
     * @param   uriPath     Path portion of the URI
     * @param   uriQuery    Query portion of the URI
     * @param   userAgent   User-Agent of the requestor
     *
     * @return  boolean
     */
    public boolean isAppPageview(
        String uriPath,
        String uriQuery,
        String contentType,
        String userAgent
    ) {

        final String appContentType     = "application/json";
        final String appUserAgent       = "WikipediaApp";
        final String appPageURIQuery    = "sections=0";
        final String iosAppPageURIQuery = "sections=all";
        final String iosUserAgent       = "iPhone";

        return (
               Utilities.stringContains(uriPath,     uriPathAPI)
            && (
                    Utilities.stringContains(uriQuery, appPageURIQuery)
                    || (Utilities.stringContains(uriQuery, iosAppPageURIQuery) && Utilities.stringContains(userAgent, iosUserAgent))
               )
            && Utilities.stringContains(contentType, appContentType)
            && Utilities.stringContains(userAgent,   appUserAgent)
        );
    }

    /**
     * Given a webrequest URI host, path, query user agent http status and content type,
     * returns true if we consider this a 'pageview', false otherwise.
     * <p>
     * See: https://meta.wikimedia.org/wiki/Research:Page_view/Generalised_filters
     *      for information on how to classify a pageview.
     *
     * @param   uriHost     Hostname portion of the URI
     * @param   uriPath     Path portion of the URI
     * @param   uriQuery    Query portion of the URI
     * @param   httpStatus  HTTP request status code
     * @param   contentType Content-Type of the request
     * @param   userAgent   User-Agent of the requestor
     *
     * @return  boolean
     */
    public boolean isPageview(
        String uriHost,
        String uriPath,
        String uriQuery,
        String httpStatus,
        String contentType,
        String userAgent
    ) {
        uriHost = uriHost.toLowerCase();

        return (
            // All pageviews have a 200 or 304 HTTP status
            httpStatusesSet.contains(httpStatus)
            // check for a regular pageview contentType, or a an API contentType
            &&  (
                    (contentTypesSet.contains(contentType) && !Utilities.stringContains(uriPath, uriPathAPI))
                    || isAppPageview(uriPath, uriQuery, contentType, userAgent)
                )
            // A pageview must be from either a wikimedia.org domain,
            // or a 'project' domain, e.g. en.wikipedia.org
            &&  (
                    Utilities.patternIsFound(uriHostWikimediaDomainPattern,  uriHost)
                    || Utilities.patternIsFound(uriHostOtherProjectsPattern, uriHost)
                    || Utilities.patternIsFound(uriHostProjectDomainPattern, uriHost)
                )
            // Either a pageview's uriPath will match the first pattern,
            // or its uriQuery will match the second
            &&  (
                    Utilities.patternIsFound(uriPathPattern, uriPath)
                    || Utilities.patternIsFound(uriQueryPattern, uriQuery)
                )
            // A pageview will not have these Special: pages in the uriPath or uriQuery
            && !Utilities.patternIsFound(uriPathUnwantedSpecialPagesPattern, uriPath)
            && !Utilities.patternIsFound(uriQueryUnwantedSpecialPagesPattern, uriQuery)
            // Edits now come through as text/html. They should not be included.
            // Luckily the query parameter does not seem to be localised.
            && !Utilities.patternIsFound(uriQueryUnwantedActions, uriQuery)
        );
    }

    /**
     * Identifies a project from a pageview uriHost
     * NOTE: Provides correct result only if used with is_pageview = true
     *
     * @param uriHost The url's host
     * @return The project identifier in format [xxx.]xxxx (en.wikipedia or wikisource for instance)
     */
    public String getProjectFromHost(String uriHost) {
        if (uriHost == null) return "-";
        String[] uri_parts = uriHost.toLowerCase().split("\\.");
        switch (uri_parts.length) {
            // case wikixxx.org
            case 2:
                return uri_parts[0];
            //case xx.wikixxx.org - Remove unwanted parts
            case 3:
                if (uriPortionsToRemove.contains(uri_parts[0]))
                    return uri_parts[1];
                else
                    return uri_parts[0] + "." + uri_parts[1];
            //xx.[m|mobile|wap|zero].wikixxx.org - Remove unwanted parts
            case 4:
                if (uriPortionsToRemove.contains(uri_parts[0]))
                    return uri_parts[2];
                else
                    return uri_parts[0] + "." + uri_parts[2];
            //xx.[m|mobile|wap|zero].[m|mobile|wap|zero].wikixxx.org - Remove unwanted parts
            case 5:
                if (uriPortionsToRemove.contains(uri_parts[0]))
                    return uri_parts[3];
                else
                    return uri_parts[0] + "." + uri_parts[3];
            default:
                return "-";
        }
    }
}
