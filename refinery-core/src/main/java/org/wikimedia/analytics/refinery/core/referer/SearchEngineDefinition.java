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

/**
 * An enum for categorizing search engines in ExternalSearch, which should
 * make it trivial to add new search engines to check in identifySearchEngine
 * since it iterates through all the entries. The name property allows user
 * to specify a string with spaces and symbols if needed.
 */
public enum SearchEngineDefinition {
    //
    AOL("AOL", "search\\.aol\\.", ""),
    ASK("Ask", "ask\\.com", ""),
    AU("AU", "search\\.auone\\.jp", ""),
    BAIDU("Baidu", "baidu\\.com", ""),
    BING("Bing", "bing\\.com", ""),
    BRAVE("Brave", "search\\.brave\\.com", ""),
    COC_COC("Coc Coc", "coccoc\\.com", ""),
    DAUM("Daum", "search\\.daum\\.net", ""),
    DDG("DuckDuckGo", "duckduckgo\\.(com|mobile.android)", ""),
    DOCOMO("Docomo", "docomo\\.ne\\.jp", ""),
    ECOSIA("Ecosia", "ecosia\\.(org|android)", ""),
    GOOGLE("Google", "google\\.([A-Za-z]{2,3}(\\.[A-Za-z]{0,3})?(/.*|$)|android|earth)", ""),
    GOOGLE_TRANSLATE("Google Translate", "translate\\.googleusercontent\\.", "prev=search|client=srp"),
    KAGI("Kagi", "kagi\\.com", ""),
    LILO("Lilo", "lilo\\.(org|mobile.android)", ""),
    MYWAY("MyWay", "search\\.myway\\.com", ""),
    NAVER("Naver", "search\\.naver\\.com", ""),
    PETAL("Petal", "petalsearch\\.com", ""),
    QWANT("Qwant", "qwant\\.(com|liberty)", ""),
    RAKUTEN("Rakuten", "rakuten\\.[A-Za-z]{2,3}(\\.[A-Za-z]{0,3})?(/.*|$)", ""),
    SEZNAM("Seznam", "seznam\\.(cz|sbrowser|mapy)", ""),
    STARTPAGE("Startpage", "(startpage|ixquick)\\.[A-Za-z]{2,3}(\\.[A-Za-z]{0,3})?(/.*|$)", ""),
    VK("VK", "go\\.mail\\.ru", ""),
    YAHOO("Yahoo", "search\\.yahoo\\.", ""),
    YANDEX("Yandex", "ya\\.ru|yandex\\.([A-Za-z]{2,3}(\\.[A-Za-z]{0,3})?(/.*|$)|browser|searchplugin|searchapp|translate)", ""),

    // Maintain PREDICTED_OTHER in last position for `SearchEngineClassifier.identifySearchEngine()`
    PREDICTED_OTHER("Predicted Other", "(^.?|(?<!re)|(^|\\.)(pre|secure))(search|suche)", "");

    private final String searchEngineName;
    private final String hostPattern;
    private final String paramPattern;

    SearchEngineDefinition(String name, String hostRegex, String paramRegex) {
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

        // The PREDICTED_OTHER case need to have more permissive values before its host pattern
        String extendedHostPattern;
        if (this.equals(PREDICTED_OTHER)) {
            // Regexp Explanation:
            //   ^[^:]*?:\/\/ -- Only consider URLS with a protocol
            //   [^\?\/]*? -- Any character not ? nor / (the domain regex is stricter for this case)
            //   (%s) -- The search engine host pattern
            extendedHostPattern = String.format("(^[^:]*?:\\/\\/[^\\?\\/]*?(%s))", this.hostPattern);
        } else {
            // Regexp Explanation:
            //   ^[^:]*?:\/\/ -- Only consider URLS with a protocol
            //   ([A-Za-z0-9\\.\\-]*\.)*? -- Accept any subdomains
            //   (%s) -- The search engine host pattern
            extendedHostPattern = String.format("^[^:]*?:\\/\\/([A-Za-z0-9\\.\\-]*\\.)*?(%s)", this.hostPattern);
        }
        //


        if (this.paramPattern.isEmpty()) {
            return extendedHostPattern;
        }

        return String.format("(%s.*?\\?.*?(%s))", extendedHostPattern, this.paramPattern);
    }

}
