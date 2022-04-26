// Copyright 2014 Wikimedia Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wikimedia.analytics.refinery.core;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.google.common.cache.LoadingCache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;

/**
 * Functions to work with Wikimedia webrequest data.
 */
public class Webrequest {

    /**
     * Static values used as qualifiers in uri_host (meaning not TLD, not project_family nor project)
     */
    public static final Set<String> URI_QUALIFIERS = new HashSet<String>(Arrays.asList(
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
    /*
     * Meta-methods to enable eager instantiation in a singleton-based way.
     * in non-Java terms: you get to only create one class instance, and only
     * when you need it, instead of always having everything (static/eager instantiation)
     * or always generating everything anew (!singletons). So we have:
     * (1) an instance;
     * (2) an empty constructor (to avoid people just calling the constructor);
     * (3) an actual getInstance method to allow for instantiation.
     */
    private static final Webrequest instance = new Webrequest();

    private Webrequest() {
    }

    public static Webrequest getInstance() {
        return instance;
    }

    public static final Set<String> SUCCESS_HTTP_STATUSES = new HashSet<String>(Arrays.asList(
        "200",
        "304"
    ));

    public static final Set<String> REDIRECT_HTTP_STATUSES = new HashSet<String>(Arrays.asList(
        "301",
        "302",
        "307"
    ));

    public static final Set<String> TEXT_HTML_CONTENT_TYPES = new HashSet<String>(Arrays.asList(
        "text/html",
        "text/html; charset=iso-8859-1",
        "text/html; charset=ISO-8859-1",
        "text/html; charset=utf-8",
        "text/html; charset=UTF-8"
    ));

    /**
     * List of domains owned and operated by The Wikimedia Foundation
     * and/or affiliates (e.g. wikidata).  This is used
     * by the isWMFHostname method.
     * Best known official source of this as of 2020-12 is
     * this varnish regex:
     * https://gerrit.wikimedia.org/r/plugins/gitiles/operations/puppet/+/refs/heads/production/modules/varnish/templates/wikimedia-frontend.vcl.erb#313
     */
    private static final List<String> WMF_DOMAINS = Arrays.asList(
        "wikimedia.org",
        "wikibooks.org",
        "wikinews.org",
        "wikipedia.org",
        "wikiquote.org",
        "wikisource.org",
        "wiktionary.org",
        "wikiversity.org",
        "wikivoyage.org",
        "wikidata.org",
        "mediawiki.org",
        "wikimediafoundation.org",
        "wikiworkshop.org",
        "wmfusercontent.org",
        "wmflabs.org",
        "wmcloud.org",
        "toolforge.org"
    );

    /* Regex to coarsely match email addresses in the user-agent as part of spider identification,
       as the User-Agent policy - https://meta.wikimedia.org/wiki/User-Agent_policy,
       encourages bot developers to leave contact information in the user agent string.
     */
    private static final String coarseEmailPattern = "\\S+@\\S+\\.[a-zA-Z]{2,3}";

    /*
     * Spiders identification pattern (obviously not perfect...)
     * to be used in addition to ua-parser device_family field
     * being identified as Spider.
     *
     * Implements also the Wikimedia User-Agent policy -
     * https://meta.wikimedia.org/wiki/User-Agent_policy
     */
    private static final Pattern spiderPattern = Pattern.compile("(?i)^(" +
                    ".*(bot|spider|WordPress|AppEngine|AppleDictionaryService|Python-urllib|python-requests|" +
                        "Google-HTTP-Java-Client|[Ff]acebook|[Yy]ahoo|RockPeaks|PhantomJS|http).*" +
                    "|(goo wikipedia|MediaWikiCrawler-Google|wikiwix-bot|Java/|curl|PHP/|Faraday|HTTPC|Ruby|\\.NET|" +
                        "Python|Apache|Scrapy|PycURL|libwww|Zend|wget|nodemw|WinHttpRaw|Twisted|com\\.eusoft|Lagotto|" +
                        "Peggo|Recuweb|check_http|Magnus|MLD|Jakarta|find-link|J\\. River|projectplan9|ADmantX|" +
                        "httpunit|LWP|iNaturalist|WikiDemo|FSResearchIt|livedoor|Microsoft Monitoring|MediaWiki|" +
                        "User:|User_talk:|github|tools.wmflabs.org|Blackboard Safeassign|Damn Small XSS|MeetingRoomApp|" + coarseEmailPattern + ").*" +
                    ")$");


    /**
     * Pattern for automatically-added subdomains that indicate zero,
     * or some similar portal-based interface to MW.
     */
    private static final Pattern uriHostPattern = Pattern.compile(
        "(^(m|zero|wap|mobile)\\.)|(\\.(m|zero|wap|mobile)\\.)"
    );

    /**
     * Consistent fragment of the user agent used by the Wikimedia
     * official mobile apps: used to identify app requests in
     * getAccessMethod.
     */
    private static final Pattern appAgentPattern = Pattern.compile(
        "Wikipedia(App|/5.0.)"
    );

    private static final int MAX_ADDRESS_LENGTH = 800;

    /**
     * Returns true when the hostname is owned and managed by the Wikimedia Foundation
     * or an affiliate.
     * E.g. en.wikipedia.org, wikimediafoundation.org, wikidata.org
     *
     * @param hostname HTTP hostname to check
     * @return boolean
     */
    public static boolean isWMFHostname(String hostname) {
        if (hostname.length() > MAX_ADDRESS_LENGTH) {
            return false;
        }

        // Convert to lower case and remove any trailing . characters
        hostname = StringUtils.stripEnd(hostname.toLowerCase(), ".");

        // `getUnchecked` is used in place of `get` as the loading function is not throwing exceptions.
        return wmfHostnameCache.getUnchecked(hostname);
    }

    /**
     * In an analysis of a days worth of webrequest data,
     * there were under 5000 distinct HTTP hostnames.
     * The isWMFHostname method of this class searches the above list of names
     * for matching hostnames.
     * Caching the result of a given hostname check in this Cache
     * speeds the search.
     */
    private static LoadingCache<String, Boolean> wmfHostnameCache = CacheBuilder.newBuilder()
        .maximumSize(10_000)
        .build(
            new CacheLoader<String, Boolean>() {
                @Override
                public Boolean load(String hostname) {
                    return computeIsWMFHostname(hostname);
                }
            }
        );

    public static boolean computeIsWMFHostname(String hostname) {
        // Split the hostname by .
        String[] parts = hostname.split("\\.");

        boolean matched = false;

        // We need at least the TLD and the domain at the end of the hostname.
        if (parts.length >= 2) {
            // Final domain should be the last 2 elements of hostname parts
            String domain = parts[parts.length - 2] + "." + parts[parts.length - 1];
            matched = WMF_DOMAINS.contains(domain);
        }
        return matched;
    }

    /**
     * Identifies a project from a pageview uriHost
     * NOTE: Provides correct result only if used with is_pageview = true
     * @param uriHost The url's host
     * @return The project identifier in format [xxx.]xxxx (en.wikipedia or wikisource for instance)
     */
    public static String getProjectFromHost(String uriHost) {
        if (uriHost == null || uriHost.length() > MAX_ADDRESS_LENGTH) return UNKNOWN_PROJECT_VALUE;
        String[] uri_parts = uriHost.toLowerCase().split("\\.");
        switch (uri_parts.length) {
        // case wikixxx.org
        case 2:
            return uri_parts[0];
        //case xx.wikixxx.org - Remove unwanted parts
        case 3:
            if (URI_QUALIFIERS.contains(uri_parts[0]))
                return uri_parts[1];
            else
                return uri_parts[0] + "." + uri_parts[1];
            //xx.[m|mobile|wap|zero].wikixxx.org - Remove unwanted parts
        case 4:
            if (URI_QUALIFIERS.contains(uri_parts[0]))
                return uri_parts[2];
            else
                return uri_parts[0] + "." + uri_parts[2];
            //xx.[m|mobile|wap|zero].[m|mobile|wap|zero].wikixxx.org - Remove unwanted parts
        case 5:
            if (URI_QUALIFIERS.contains(uri_parts[0]))
                return uri_parts[3];
            else
                return uri_parts[0] + "." + uri_parts[3];
        default:
            return UNKNOWN_PROJECT_VALUE;
        }
    }

    /**
     * Identify a bunch of spiders; returns TRUE
     * if the user agent matches a known spider.
     *
     * @param userAgent the user agent associated with the request.
     * @return boolean
     */
    public boolean isSpider(String userAgent) {
        if ("-".equals(userAgent))
            return true;
        else {
            // `getUnchecked` is used in place of `get` as the loading function is not throwing exceptions.
            return agentTypeCache.getUnchecked(userAgent);
        }
    }

    /*
     * Spiders identification regexp takes a lot of computation power while there are only
     * a relatively small number of recurrent user_agent values (less than a million).
     * We use a cache to prevent recomputing agentType for frequently seen user agents.
     */
    private LoadingCache<String, Boolean> agentTypeCache = CacheBuilder.newBuilder()
        .maximumSize(10_000)
        .build(
            new CacheLoader<String, Boolean>() {
                @Override
                public Boolean load(String ua) {
                    return spiderPattern.matcher(ua).find();
                }
            }
        );

    /**
     * Kept for backward compatibility.
     */
    @Deprecated
    public boolean isCrawler(String userAgent) {
        return isSpider(userAgent);
    }


    /**
     * Determines the method used for accessing the site - mobile web,
     * desktop, or app. If the user agent is an app agent, it's
     * mobile app; if the user agent is not, but it is to m. or
     * zero. domains, mobile web; otherwise, desktop.
     *
     * @param uriHost   the value in the uri_host field.
     * @param userAgent the user_agent.
     * @return String
     */
    public String getAccessMethod(String uriHost, String userAgent) {
        String accessMethod;

        if (appAgentPattern.matcher(userAgent).find()) {
            accessMethod = "mobile app";
        } else if (uriHostPattern.matcher(uriHost).find()) {
            accessMethod = "mobile web";
        } else {
            accessMethod = "desktop";
        }

        return accessMethod;
    }

    /**
     * Classifies a referer
     *
     * @param url The referer url to classify
     * @return RefererClassification
     */
    public RefererClass classifyReferer(String url) {
        if (url == null || url.isEmpty() || url.equals("-")) {
            return RefererClass.NONE;
        }

        String[] urlParts = StringUtils.splitPreserveAllTokens(url, '/');
        if (urlParts == null || urlParts.length < 3) {
            return RefererClass.UNKNOWN;
        }

        if (!urlParts[0].equals("http:") && !urlParts[0].equals("https:") && !urlParts[0].equals("android-app:")) {
            return RefererClass.UNKNOWN;
        }

        if (!urlParts[1].isEmpty()) {
            return RefererClass.UNKNOWN;
        }

        String[] domainParts = StringUtils.splitPreserveAllTokens(urlParts[2], '.');

        if (domainParts == null || domainParts.length < 2) {
            return RefererClass.UNKNOWN;
        }

        if (domainParts[domainParts.length - 1].equals("org")) {
            switch (domainParts[domainParts.length - 2]) {
            case "":
                return RefererClass.UNKNOWN;
            case "mediawiki":
            case "wikibooks":
            case "wikidata":
            case "wikinews":
            case "wikimedia":
            case "wikimediafoundation":
            case "wikipedia":
            case "wikiquote":
            case "wikisource":
            case "wikiversity":
            case "wikivoyage":
            case "wiktionary":
                return RefererClass.INTERNAL;
            }
        }
        return RefererClass.EXTERNAL;
    }

    /**
     * Extract project information from host by lowering case, splitting
     * and assigning projectClass, project, qualifiers and tld parts based on splits
     * Example: normalizeHost("en.m.zero.wikipedia.org")<br/>
     * Returns:<br/>
     * NormalizedHostInfo(
     * "projectFamily":"wikipedia",
     * "project":"en",
     * "qualifiers":["m", "zero"],
     * "tld":"org",
     * )
     *
     * @param uriHost The url's host
     * @return A NormalizedHostInfo object with projectFamily, project, qualifiers and tld values set.
     */
    public static NormalizedHostInfo normalizeHost(String uriHost) {
        if ((uriHost == null) || (uriHost.trim().isEmpty())) return new NormalizedHostInfo();
        uriHost = uriHost.toLowerCase();
        // `getUnchecked` is used in place of `get` as the loading function is not throwing exceptions.
        return normalizedHostCache.getUnchecked(uriHost);
    }

    /**
     * Used to speed up "normalization of hosts"
     */
    private static LoadingCache<String, NormalizedHostInfo> normalizedHostCache = CacheBuilder.newBuilder()
        .maximumSize(5_000)
        .build(
            new CacheLoader<String, NormalizedHostInfo>() {
                @Override
                public NormalizedHostInfo load(String uriHost) {
                    return computeNormalizeHost(uriHost);
                }
            }
        );

    public static NormalizedHostInfo computeNormalizeHost(String uriHost) {
        NormalizedHostInfo result = new NormalizedHostInfo();

        // TODO fix, the host is split in two different ways on line 185 and this one
        // Remove port if any

        int portIdx = uriHost.indexOf(":");
        uriHost = uriHost.substring(0, ((portIdx < 0) ? uriHost.length() : portIdx));

        // Replace multiple dots by only one
        uriHost = uriHost.replaceAll("[//.]+", ".");

        // Split by the dots
        String[] uriParts = uriHost.toLowerCase().split("\\.");

        // If no split part, return empty
        if (uriParts.length <= 1) return result;

        // Handle special case where TLD is numeric --> assume IP address, don't normalize
        // Length is > 0 because of previous check, so no error case
        if (uriParts[uriParts.length - 1].matches("[0-9]+")) return result;

        // project_family and TLD normalization
        // Line 355 enforces uriParts.length greater than 2
        result.setProjectFamily(uriParts[uriParts.length - 2]);
        result.setTld(uriParts[uriParts.length - 1]);

        // project/qualifier normalization
        if ((uriParts.length > 2) && (!uriParts[0].equals("www"))) {
            if (URI_QUALIFIERS.contains(uriParts[0])) {
                result.addQualifier(uriParts[0]);
            } else {
                result.setProject(uriParts[0]);
            }
        }
        // qualifiers normalization: xx.[q1.q2.q3].wikixxx.xx
        if (uriParts.length > 3) {
            for (int i = 1; i < uriParts.length - 2; i++) {
                result.addQualifier(uriParts[i]);
            }
        }
        return result;
    }

    public static boolean isSuccess(String httpStatus) {
        return SUCCESS_HTTP_STATUSES.contains(httpStatus);

    }

    public static boolean isRedirect(String httpStatus) {
        return REDIRECT_HTTP_STATUSES.contains(httpStatus);
    }

    public static boolean isTextHTMLContentType(String contentType) {
        return TEXT_HTML_CONTENT_TYPES.contains(contentType);
    }
}
