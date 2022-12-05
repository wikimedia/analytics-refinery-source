/**
 * Copyright (C) 2015  Wikimedia Foundation
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

package org.wikimedia.analytics.refinery.core.referer;

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A class for categorizing referer urls into different classes.
 */
public class RefererClassifier {

    private static final RefererClassifier instance = new RefererClassifier();

    private static Pattern searchEnginePattern;
    private static Pattern mediaSitePattern;

    private RefererClassifier() {

    }

    public static RefererClassifier getInstance() {
        return instance;
    }

    public static class RefererData {
        private RefererClass refererClassLabel;
        private String refererIdentified;

        private RefererData(RefererClass refererType, String refererName) {
            refererClassLabel = refererType;
            refererIdentified = refererName;
        }

        public RefererClass getRefererClassLabel() {
            return refererClassLabel;
        }

        public String getRefererIdentified() {
            return refererIdentified;
        }

    }

    static {
        String sePattern = "";
        for (SearchEngineDefinition se: SearchEngineDefinition.values() ){
            sePattern = sePattern.concat(se.getPattern() +"|");
        }

        searchEnginePattern = Pattern.compile(sePattern.substring(0, sePattern.length()-1));

        String msPattern = "";
        for (MediaSitesDefinition ms:MediaSitesDefinition.values()) {
            msPattern = msPattern.concat(ms.getPattern() + "|");
        }

        mediaSitePattern = Pattern.compile(msPattern.substring(0, msPattern.length() - 1));

    }

    /**
     * Classifies a referer
     *
     * @param url The referer url to classify
     * @return RefererClassification
     */
    public RefererData classifyReferer(String url) {
        String refererUrlName = "";
        
        if (url == null || url.isEmpty() || url.equals("-")) {
            return new RefererData(RefererClass.NONE, "none");
        }

        String[] urlParts = StringUtils.splitPreserveAllTokens(url, '/');
        if (urlParts == null || urlParts.length < 3) {
            return new RefererData(RefererClass.UNKNOWN, "none");
        }

        if (!urlParts[0].equals("http:") && !urlParts[0].equals("https:") && !urlParts[0].equals("android-app:")) {
            return new RefererData(RefererClass.UNKNOWN, "none");
        }

        if (!urlParts[1].isEmpty()) {
            return new RefererData(RefererClass.UNKNOWN, "none");
        }

        String[] domainParts = StringUtils.splitPreserveAllTokens(urlParts[2], '.');

        if (domainParts == null || domainParts.length < 2) {
            return new RefererData(RefererClass.UNKNOWN, "none");
        }

        if (domainParts[domainParts.length - 1].equals("org")) {
            switch (domainParts[domainParts.length - 2]) {
            case "":
                return new RefererData(RefererClass.UNKNOWN, "none");
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
                return new RefererData(RefererClass.INTERNAL, "none");
            }
        }

        if (!Objects.equals(nameSearchEngine(url), "none")) {
            refererUrlName = nameSearchEngine(url);
            return new RefererData(RefererClass.SEARCH_ENGINE, refererUrlName);
        }

        if (!Objects.equals(nameMediaSite(url), "none")) {
            refererUrlName = nameMediaSite(url);
            return new RefererData(RefererClass.MEDIA_SITE, refererUrlName);
        }

        return new RefererData(RefererClass.EXTERNAL, "none");
    }

    /**
     * Determines the search engine that served as a referer
     * for a particular request.
     *
     * If no search engine was found returns "none"
     *
     * @param rawReferer the value in the referer field.
     * @return String
     */
    public String nameSearchEngine(String rawReferer) {
        if (searchEnginePattern.matcher(rawReferer).find()) {
            for (SearchEngineDefinition se : SearchEngineDefinition.values()) {
                Pattern pattern = Pattern.compile(se.getPattern());
                if (pattern.matcher(rawReferer).find()) {
                    return se.getSearchEngineName();
                }
            }
        }

        // for backwards compatibility
        return "none";

    }

    public String nameMediaSite(String rawReferer) {
        if (mediaSitePattern.matcher(rawReferer).find()) {
            for (MediaSitesDefinition ms : MediaSitesDefinition.values()) {
                Pattern pattern = Pattern.compile(ms.getPattern());
                if (pattern.matcher(rawReferer).find()) {
                    return ms.getMediaSitesName();
                }
            }
        }

        return "none";
    }

}
