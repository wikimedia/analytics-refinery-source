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

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

/**
 * Functions to work with Wikimedia webrequest data.
 */
public class Webrequest {

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

    public static Webrequest getInstance(){
        return instance;
    }

    /*
     * Spiders identification pattern (obvisouly not perfect...)
     * to be used in addition to ua-parser device_family field
     * being identified as Spider.
     */
    private static final Pattern spiderPattern = Pattern.compile("(?i)^(" +
                    ".*(bot|spider|WordPress|AppEngine|AppleDictionaryService|Python-urllib|python-requests|" +
                        "Google-HTTP-Java-Client|[Ff]acebook|[Yy]ahoo|RockPeaks|http).*" +
                    "|(goo wikipedia|MediaWikiCrawler-Google|wikiwix-bot|Java/|curl|PHP/|Faraday|HTTPC|Ruby|\\.NET|" +
                        "Python|Apache|Scrapy|PycURL|libwww|Zend|wget|nodemw|WinHttpRaw|Twisted|com\\.eusoft|Lagotto|" +
                        "Peggo|Recuweb|check_http|Magnus|MLD|Jakarta|find-link|J\\. River|projectplan9|ADmantX|" +
                        "httpunit|LWP|iNaturalist|WikiDemo|FSResearchIt|livedoor|Microsoft Monitoring|MediaWiki).*" +
                    ")$");

    /*
     * Spiders identification regexp takes a lot of computation power while there only are only
     * a relatively small number of recurrent user_agent values (less than a million).
     * We use a LRU cache to prevent recomputing agentType for frequently seen user agents.
     */
    private Utilities.LRUCache<String, Boolean> agentTypeCache = new Utilities.LRUCache<>(10000);

    /*
     * WikimediaBot identification pattern 
     */
    private static final Pattern wikimediaBotPattern = Pattern.compile("\\bWikimediaBot\\b");

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
        "WikipediaApp"
    );

    /**
     * Identify a bunch of spiders; returns TRUE
     * if the user agent matches a known spider and doesn't
     * match the WikimediaBot convention.
     * @param    userAgent    the user agent associated with the request.
     * @return   boolean
     */
    public boolean isSpider(String userAgent) {
        if ("-".equals(userAgent))
            return true;
        else {
            if (agentTypeCache.containsKey(userAgent))
                return agentTypeCache.get(userAgent);
            else {
                boolean isSpider = (spiderPattern.matcher(userAgent).find()
                                    && !wikimediaBotPattern.matcher(userAgent).find());
                agentTypeCache.put(userAgent, isSpider);
                return isSpider;
            }
        }
    }
    /**
     * Kept for backward compatibility.
     */
    @Deprecated
    public boolean isCrawler(String userAgent) {
        return isSpider(userAgent);
    }

    /**
     * Identify WikimediaBot; returns TRUE
     * if the user agent matches the WikimediaBot convention.
     * @param    userAgent    the user agent associated with the request.
     * @return   boolean
     */
    public boolean isWikimediaBot(String userAgent) {
        return wikimediaBotPattern.matcher(userAgent).find();
    }

    /**
     * Given an x_analytics field and the name of a key, return the
     * value associated with said key, or an empty string if the key
     * is not found.
     *
     * @param xAnalytics the x_analytics field entry.
     * @param key the key to search for the value of.
     * @return String
     */
    public String getXAnalyticsValue(String xAnalytics, String key) {

        String value = "";

        int keyIndex = xAnalytics.indexOf(key);
        if(keyIndex == -1){
            return value;
        }

        int delimiterIndex = xAnalytics.indexOf(";", keyIndex);
        if(delimiterIndex == -1){
            value = xAnalytics.substring(keyIndex + key.length() + 1);
        } else {
            value = xAnalytics.substring(keyIndex + key.length() + 1, delimiterIndex);
        }

        //Done
        return value;
    }
    /**
     * Determines the method used for accessing the site - mobile web,
     * desktop, or app. If the user agent is an app agent, it's
     * mobile app; if the user agent is not, but it is to m. or
     * zero. domains, mobile web; otherwise, desktop.
     *
     * @param uriHost the value in the uri_host field.
     *
     * @param userAgent the user_agent.
     *
     * @return String
     */
    public String getAccessMethod(String uriHost, String userAgent) {
        String accessMethod;

        if(appAgentPattern.matcher(userAgent).find()){
            accessMethod = "mobile app";
        } else if(uriHostPattern.matcher(uriHost).find()){
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
    public String classifyReferer(String url) {
        if (url == null || url.isEmpty() || url.equals("-")) {
            return Referer.NONE.getRefLabel();
        }

        String[] urlParts = StringUtils.splitPreserveAllTokens(url, '/');
        if (urlParts == null || urlParts.length <3) {
            return Referer.UNKNOWN.getRefLabel();
        }

        if (!urlParts[0].equals("http:") && !urlParts[0].equals("https:")) {
            return Referer.UNKNOWN.getRefLabel();
        }

        if (!urlParts[1].isEmpty()) {
            return Referer.UNKNOWN.getRefLabel();
        }

        String[] domainParts = StringUtils.splitPreserveAllTokens(urlParts[2], '.');

        if (domainParts == null || domainParts.length <2) {
            return Referer.UNKNOWN.getRefLabel();
        }

        if (domainParts[domainParts.length-1].equals("org")) {
            switch (domainParts[domainParts.length-2]) {
            case "":
                return Referer.UNKNOWN.getRefLabel();
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
                return Referer.INTERNAL.getRefLabel();
            }
        }
        return Referer.EXTERNAL.getRefLabel();
    }

    /**
     * Extract project information from host by lowering case, splitting
     * and assigning projectClass, project, qualifiers and tld parts based on splits
     * Example: normalizeHost("en.m.zero.wikipedia.org")<br/>
     * Returns:<br/>
     * NormalizedHostInfo(
     * "project_class":"wikipedia",
     * "project":"en",
     * "qualifiers":["m", "zero"],
     * "tld":"org",
     * )
     *
     * @param uriHost The url's host
     * @return A NormalizedHostInfo object with project_class, project, qualifiers and tld values set.
     */
    public NormalizedHostInfo normalizeHost(String uriHost) {
        NormalizedHostInfo result = new NormalizedHostInfo();
        if ((uriHost == null) || (uriHost.isEmpty())) return result;

        // Remove port if any
        int portIdx = uriHost.indexOf(":");
        uriHost = uriHost.substring(0, ((portIdx < 0) ? uriHost.length() : portIdx));

        // Replace multiple dots by only one
        uriHost = uriHost.replaceAll("[//.]+", ".");

        // Split by the dots
        String[] uriParts = uriHost.toLowerCase().split("\\.");

        // If no splitted part, return empty
        if (uriParts.length == 0) return result;

        // Handle special case where TLD is numeric --> assume IP address, don't normalize
        // Length is > 0 because of previous check, so no error case
        if (uriParts[uriParts.length - 1].matches("[0-9]+")) return result;

        if (uriParts.length > 1) {
            // project_class and TLD normalization
            result.setProjectClass(uriParts[uriParts.length - 2]);
            result.setTld(uriParts[uriParts.length - 1]);
        }
        // project normalization
        if ((uriParts.length > 2) && (! uriParts[0].equals("www")))
            result.setProject(uriParts[0]);
        // qualifiers normalization: xx.[q1.q2.q3].wikixxx.xx
        if (uriParts.length > 3) {
            for (int i = 1; i < uriParts.length - 2; i++) {
                result.addQualifier(uriParts[i]);
            }
        }

        return result;

    }

}
