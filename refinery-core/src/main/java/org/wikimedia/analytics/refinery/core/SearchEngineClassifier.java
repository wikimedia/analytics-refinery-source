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

package org.wikimedia.analytics.refinery.core;

import java.util.regex.Pattern;


/**
 * Functions to identify traffic from external search engines
 */
public class SearchEngineClassifier {

    private static final SearchEngineClassifier instance = new SearchEngineClassifier();

    private static Pattern searchEnginePattern;

    private SearchEngineClassifier() {

    }

    static {
        String pattern = "";
        for (SearchEngine se:SearchEngine.values() ){
            pattern = pattern.concat(se.getPattern() +"|");
        }

        searchEnginePattern = Pattern.compile(pattern.substring(0, pattern.length()-1));
    }

    public static SearchEngineClassifier getInstance() {
        return instance;
    }


    /**
     * Provides a simple classification of requests based
     * on their referer.
     *
     * See RefererClass enum;
     *
     * @param rawReferer the value in the referer field.
     * @return String
     */
    public RefererClass getRefererClass(String rawReferer) {

        RefererClass refererClass = Webrequest.getInstance().classifyReferer(rawReferer);

        if (refererClass.equals(RefererClass.EXTERNAL)) {
            if (SearchEngineClassifier.searchEnginePattern.matcher(rawReferer).find()) {
               refererClass = RefererClass.SEARCH_ENGINE;
            }
        }

        return refererClass;
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
    public String identifySearchEngine(String rawReferer) {

        if (searchEnginePattern.matcher(rawReferer).find()) {
            for (SearchEngine se : SearchEngine.values()) {
                Pattern pattern = Pattern.compile(se.getPattern());
                if (pattern.matcher(rawReferer).find()) {
                    return se.getSearchEngineName();
                }
            }
        }

        // for backwards compatibility
        return "none";

    }

}
