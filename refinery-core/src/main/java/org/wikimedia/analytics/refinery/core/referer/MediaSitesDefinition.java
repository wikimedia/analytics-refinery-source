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
 * An enum for classifying external referer that are from
 * media sites and are non search engines.
 */
public enum MediaSitesDefinition {
    YOUTUBE("Youtube", "[^\\?\\/]*?((youtube|googlevideo|ytimg)\\.com|youtu\\.be)"),
    FACEBOOK("Facebook", "[^\\?\\/]*?((facebook\\.(com|co))|(.messenger\\.com))"),
    TWITTER("Twitter", "(t\\.co$)|(t\\.co[^a-zA-Z0-9\\.])|[^\\?\\/]*?((twitter|twimg|twttr)\\.(com|android))"),
    REDDIT("Reddit", "[^\\?\\/]*?((reddit\\.com)|(redd\\.it))"),
    TIKTOK("Tiktok", "([^\\?\\/]*?\\.tiktok\\.com)|(tiktok\\.com)"),
    QUORA("Quora", "[^\\?\\/]*?((quora\\.com)|(quoracdn\\.net))"),
    INSTAGRAM("Instagram", "(ig\\.me)|[^\\?\\/]*?((instagram|cdninstagram)\\.com)"),
    YCOMBINATOR("Ycombinator", "[^\\?\\/]*?((startupschool\\.org)|(ycombinator\\.com))"),
    LINKEDIN("LinkedIn", "[^\\?\\/]*?((linkedin\\.(com|android))|lnkd\\.in)"),
    GITHUB("Github", "[^\\?\\/]*?(github\\.com)"),
    PINTEREST("Pinterest", "[^\\?\\/]*?((pin\\.it)|(pinimg\\.com)|\\.pinterest)|pinterest"),
    COURSERA("Coursera", "[^\\?\\/]*?(coursera\\.(org|com))"),
    MEDIUM("Medium", "[^\\?\\/]*?(medium\\.com)"),
    STACKOVERFLOW("Stackoverflow", "[^\\?\\/]*?(((stackexchange|stackapps|askubuntu|blogoverflow|stackoverflow)\\.com)|mathoverflow\\.net)");

    private final String mediaSitesName;
    private final String hostPattern;

    MediaSitesDefinition(String name, String hostRegex) {
        this.mediaSitesName = name;
        this.hostPattern = hostRegex;
    }

    public String getMediaSitesName() {
        return this.mediaSitesName;
    }


    /**
     * Constructs regex pattern to apply to entire referer URI.
     *
     * @return String
     */
    public String getPattern() {

        return String.format("(^[^:]*?:\\/\\/(%s))", this.hostPattern);


    }

}
