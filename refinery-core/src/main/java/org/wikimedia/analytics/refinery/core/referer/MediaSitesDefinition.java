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
 * An enum for classifying external referer that are from media sites
 */
public enum MediaSitesDefinition {
    BLUESKY("Bluesky", "bsky\\.app"),
    COURSERA("Coursera", "coursera\\.(org|com)"),
    DISCORD("Discord", "discord\\.com"),
    FACEBOOK("Facebook", "(facebook\\.(com|co))|(messenger\\.com)"),
    GITHUB("Github", "github\\.com"),
    INSTAGRAM("Instagram", "(ig\\.me)|((instagram|cdninstagram)\\.com)"),
    LINKEDIN("LinkedIn", "(linkedin\\.(com|android))|lnkd\\.in"),
    MEDIUM("Medium", "medium\\.com"),
    PINTEREST("Pinterest", "((pin\\.it)|(pinimg\\.com)|\\.pinterest)|pinterest"),
    QUORA("Quora", "(quora\\.com)|(quoracdn\\.net)"),
    REDDIT("Reddit", "(reddit\\.com)|(redd\\.it)|(com\\.reddit)"),
    STACKOVERFLOW("Stackoverflow", "((stackexchange|stackapps|askubuntu|blogoverflow|stackoverflow)\\.com)|mathoverflow\\.net"),
    STEAM("Steam", "steam\\.com"),
    THREADS("Threads", "threads\\.com"),
    TIKTOK("Tiktok", "tiktok\\.com"),
    TWITCH("Twitch", "twitch\\.tv"),
    TWITTER("Twitter", "(x\\.com)|(t\\.co$)|(t\\.co[^a-zA-Z0-9\\.])|((twitter|twimg|twttr)\\.(com|android))"),
    YCOMBINATOR("Ycombinator", "(startupschool\\.org)|(ycombinator\\.com)"),
    YOUTUBE("Youtube", "((youtube|googlevideo|ytimg)\\.com)|(youtu\\.be)");

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

        // Regexp Explanation:
        //   ^[^:]*?:\/\/ -- Only consider URLS with a protocol
        //   ([A-Za-z0-9\\.\\-]*\.)*? -- Accept any subdomains
        //   (%s) -- The search engine host pattern
        return String.format("^[^:]*?:\\/\\/([A-Za-z0-9\\.\\-]*\\.)*?(%s)", this.hostPattern);

    }

}
