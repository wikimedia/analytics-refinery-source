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
public class Pageview {

    private static final Pattern uriHostWikimediaDomainPattern = Pattern.compile(
        "(commons|meta|incubator|species)\\."   // any of these domain names
        + "((m|mobile|wap|zero)\\.)?"           // followed by an optional mobile or zero qualifier
        + "wikimedia\\.org$"                    // ending with wikimedia.org
    );

    private static final Pattern uriHostProjectDomainPattern = Pattern.compile(
        "(?<!www)\\."                           // not starting with "www"
        + "(wik(ibooks|idata|"                  // match project domains ending in .org
        + "inews|ipedia|iquote|isource|tionary|iversity|ivoyage))\\.org$"
    );

    private static final Pattern uriPathPattern = Pattern.compile(
        "^(/sr(-(ec|el))?|/w(iki)?|/zh(-(cn|hans|hant|hk|mo|my|sg|tw))?)/"
    );

    private static final Pattern uriQueryPattern = Pattern.compile(
        "\\?((cur|old)id|title)="
    );

    private static final Pattern uriPathUnwantedSpecialPagesPattern = Pattern.compile(
        "BannerRandom|CentralAutoLogin|MobileEditor|Undefined|UserLogin|ZeroRatedMobileAccess"
    );

    private static final Pattern uriQueryUnwantedSpecialPagesPattern = Pattern.compile(
        "CentralAutoLogin|MobileEditor|UserLogin|ZeroRatedMobileAccess"
    );

    private static final Pattern uriQueryUnwantedActions = Pattern.compile(
        "action=edit"
    );

    private static final HashSet<String> contentTypesSet = new HashSet<String>(Arrays.asList(
        "text/html",
        "text/html; charset=iso-8859-1",
        "text/html; charset=ISO-8859-1",
        "text/html; charset=utf-8",
        "text/html; charset=UTF-8"
    ));

    private static final HashSet<String> httpStatusesSet = new HashSet<String>(Arrays.asList(
        "200",
        "304"
    ));

    /**
     * All API request uriPaths will contain this
     */
    private static final String uriPathAPI = "api.php";


    /**
     * Check if the target is contained within string.  This is
     * just a convenience method that also makes sure that arguments are not null.
     *
     * @param   string    String to search in
     * @param   target    String to search for
     * @return  boolean
     */
    private static boolean stringContains(String string, String target){
        return (target != null && string != null && string.contains(target));
    }


    /**
     * Convenience method for Using Matcher.find() to check if
     * the given regex Pattern matches the target String.
     * Also called in the LegacyPageview class.
     *
     * @param Pattern pattern
     * @param String  target
     *
     * @return boolean
     */
    public static boolean patternIsFound(Pattern pattern, String target) {
        return pattern.matcher(target).find();
    }


    /**
     * Given a webrequest URI path, query and user agent,
     * returns true if we consider this an app (API) request.
     *
     * @param   uriPath     Path portion of the URI
     * @param   uriQuery    Query portion of the URI
     * @param   userAgent   User-Agent of the requestor
     *
     * @return  boolean
     */
    private static boolean isAppPageRequest(
        String uriPath,
        String uriQuery,
        String contentType,
        String userAgent
    ) {

        final String appContentType  = "application/json";
        final String appUserAgent    = "WikipediaApp";
        final String appPageURIQuery = "sections=0";

        return (
               stringContains(uriPath,     uriPathAPI)
            && stringContains(uriQuery,    appPageURIQuery)
            && stringContains(contentType, appContentType)
            && stringContains(userAgent,   appUserAgent)
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
    public static boolean isPageview(
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
                    (contentTypesSet.contains(contentType) && !stringContains(uriPath, uriPathAPI))
                    || isAppPageRequest(uriPath, uriQuery, contentType, userAgent)
                )
            // A pageview must be from either a wikimedia.org domain,
            // or a 'project' domain, e.g. en.wikipedia.org
            &&  (
                    patternIsFound(uriHostWikimediaDomainPattern, uriHost)
                    || patternIsFound(uriHostProjectDomainPattern, uriHost)
                )
            // Either a pageview's uriPath will match the first pattern,
            // or its uriQuery will match the second
            &&  (
                    patternIsFound(uriPathPattern, uriPath)
                    || patternIsFound(uriQueryPattern, uriQuery)
                )
            // A pageview will not have these Special: pages in the uriPath or uriQuery
            && !patternIsFound(uriPathUnwantedSpecialPagesPattern, uriPath)
            && !patternIsFound(uriQueryUnwantedSpecialPagesPattern, uriQuery)
            // Edits now come through as text/html. They should not be included.
            // Luckily the query parameter does not seem to be localised.
            && !patternIsFound(uriQueryUnwantedActions, uriQuery)
        );
    }
}