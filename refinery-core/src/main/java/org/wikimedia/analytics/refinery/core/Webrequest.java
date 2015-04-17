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
     * Constant string results for referer classification
     */
    public static final String REFERER_UNKNOWN = "unknown";
    public static final String REFERER_INTERNAL = "internal";
    public static final String REFERER_EXTERNAL = "external";

    /*
     * Now back to the good part.
     * Wikimedia-specific crawlers
     */
    private static final Pattern crawlerPattern = Pattern.compile(
        "(goo wikipedia|MediaWikiCrawler-Google|wikiwix-bot).*"
    );

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
     * Identify Wikimedia-specific crawlers; returns TRUE
     * if the user agent matches a known crawler.
     * @param    userAgent    the user agent associated with the request.
     * @return   boolean
     */
    public boolean isCrawler(String userAgent) {
        return crawlerPattern.matcher(userAgent).find();
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
        String accessMethod = "";

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
            return REFERER_UNKNOWN;
        }

        String[] urlParts = StringUtils.splitPreserveAllTokens(url, '/');
        if (urlParts == null || urlParts.length <3) {
            return REFERER_UNKNOWN;
        }

        if (!urlParts[0].equals("http:") && !urlParts[0].equals("https:")) {
            return REFERER_UNKNOWN;
        }

        if (!urlParts[1].isEmpty()) {
            return REFERER_UNKNOWN;
        }

        String[] domainParts = StringUtils.splitPreserveAllTokens(urlParts[2], '.');

        if (domainParts == null || domainParts.length <2) {
            return REFERER_UNKNOWN;
        }

        if (domainParts[domainParts.length-1].equals("org")) {
            switch (domainParts[domainParts.length-2]) {
            case "":
                return REFERER_UNKNOWN;
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
                return REFERER_INTERNAL;
            }
        }
        return REFERER_EXTERNAL;
    }



}