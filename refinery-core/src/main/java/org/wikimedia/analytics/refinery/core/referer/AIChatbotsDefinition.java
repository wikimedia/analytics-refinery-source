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
 * An enum for classifying external referer that are from AI Chatbots
 */
public enum AIChatbotsDefinition {
    CHATGPT("ChatGPT", "chatgpt\\.com"),
    CLAUDE("Claude", "claude\\.ai"),
    COPILOT("Copilot", "copilot\\.microsoft\\.com"),
    GEMINI("Gemini", "(gemini\\.google\\.com)|(deepmind\\.google)"),
    GROK("Grok", "grok\\.com"),
    PERPLEXITY("Perplexity", "perplexity\\.ai");
    private final String aiChatbotName;
    private final String hostPattern;

    AIChatbotsDefinition(String name, String hostRegex) {
        this.aiChatbotName = name;
        this.hostPattern = hostRegex;
    }

    public String getAIChatbotName() {
        return this.aiChatbotName;
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
