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

/**
 * An enum for categorizing search engines in ExternalSearch, which should
 * make it trivial to add new search engines to check in identifySearchEngine
 * since it iterates through all the entries. The name property allows user
 * to specify a string with spaces and symbols if needed.
 */
public enum SearchEngine {
    GOOGLE("Google", "google\\.", ""),
    GOOGLE_TRANSLATE("Google Translate", "translate\\.googleusercontent\\.", "prev=search|client=srp"),
    YAHOO("Yahoo", "search\\.yahoo\\.", ""),
    BING("Bing", "\\.bing\\.", ""),
    YANDEX("Yandex", "yandex\\.", ""),
    BAIDU("Baidu", "baidu\\.", ""),
    DDG("DuckDuckGo", "duckduckgo\\.", ""),
    ECOSIA("Ecosia", "\\.ecosia\\.", ""),
    STARTPAGE("Startpage", "\\.(startpage|ixquick)\\.", ""),
    NAVER("Naver", "search\\.naver\\.", ""),
    DOCOMO("Docomo", "\\.docomo\\.", ""),
    QWANT("Qwant", "qwant\\.", ""),
    DAUM("Daum", "search\\.daum\\.", ""),
    MYWAY("MyWay", "search\\.myway\\.", ""),
    SEZNAM("Seznam", "\\.seznam\\.", ""),
    AU("AU", "search\\.auone\\.", ""),
    ASK("Ask", "\\.ask\\.", ""),
    LILO("Lilo", "\\.lilo\\.", ""),
    COC_COC("Coc Coc", "coccoc\\.", ""),
    AOL("AOL", "search\\.aol\\.", ""),
    RAKUTEN("Rakuten", "\\.rakuten\\.", ""),
    BRAVE("Brave", "search\\.brave\\.", ""),
    PETAL("Petal", "petalsearch\\.", ""),
    VK("VK", "go\\.mail\\.ru", ""),

    // Maintain PREDICTED_OTHER in last position for `SearchEngineClassifier.identifySearchEngine()`
    PREDICTED_OTHER("Predicted Other", "(^.?|(?<!re)|(^|\\.)(pre|secure))(search|suche)", "");

    private final String searchEngineName;
    private final String hostPattern;
    private final String paramPattern;

    SearchEngine(String name, String hostRegex, String paramRegex) {
        this.searchEngineName = name;
        this.hostPattern = hostRegex;
        this.paramPattern = paramRegex;
    }

    public String getSearchEngineName() {
        return this.searchEngineName;
    }


    /**
     * Constructs regex pattern to apply to entire referer URI.
     *
     * @return String
     */
    public String getPattern() {

        String extendedHostPattern = String.format("(^[^:]*?:\\/\\/[^\\?\\/]*?(%s))", this.hostPattern);

        if (this.paramPattern.isEmpty()) {
            return extendedHostPattern;
        }

        return String.format("(%s.*?\\?.*?(%s))", extendedHostPattern, this.paramPattern);
    }

}
