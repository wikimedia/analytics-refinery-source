/**
 * Copyright (C) 2020 Wikimedia Foundation
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


import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Map;

public class TestActorSignatureGenerator extends TestCase {

    private ActorSignatureGenerator actorSignatureGenerator = ActorSignatureGenerator.getInstance();

    public void testGetActorSignatureGeneratorNullIP() {
        String actorSignature = actorSignatureGenerator.execute(
                null,
                "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML like Gecko) Chrome/37.0.2062.120 Safari/537.36",
                "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
                "commons.wikimedia.org",
                "?modules=skins.minerva.mainMenu.icons&image=mapPin&format=original&skin=minerva&version=a4j1i",
                new HashMap<>());

        assertNull(actorSignature);
    }

    public void testGetActorSignatureGeneratorNullUserAgent() {
        String actorSignature = actorSignatureGenerator.execute(
                "192.168.12.2",
                null,
                "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
                "commons.wikimedia.org",
                "?modules=skins.minerva.mainMenu.icons&image=mapPin&format=original&skin=minerva&version=a4j1i",
                new HashMap<>());

        assertNull(actorSignature);
    }

    public void testGetActorSignatureGeneratorNullAcceptLanguage() {
        String actorSignature = actorSignatureGenerator.execute(
                "192.168.12.2",
                "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML like Gecko) Chrome/37.0.2062.120 Safari/537.36",
                null,
                "commons.wikimedia.org",
                "?modules=skins.minerva.mainMenu.icons&image=mapPin&format=original&skin=minerva&version=a4j1i",
                new HashMap<>());

        assertNull(actorSignature);
    }

    public void testGetActorSignatureGeneratorNullUriHost() {
        String actorSignature = actorSignatureGenerator.execute(
                "192.168.12.2",
                "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML like Gecko) Chrome/37.0.2062.120 Safari/537.36",
                "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
                null,
                "?modules=skins.minerva.mainMenu.icons&image=mapPin&format=original&skin=minerva&version=a4j1i",
                new HashMap<>());

        assertNull(actorSignature);
    }

    public void testGetActorSignatureGeneratorNullUriQuery() {
        String actorSignature = actorSignatureGenerator.execute(
                "192.168.12.2",
                "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML like Gecko) Chrome/37.0.2062.120 Safari/537.36",
                "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
                "commons.wikimedia.org",
                null,
                new HashMap<>());

        // Value generated using spark
        assertEquals("c9f49eaeaf474a818ac43604c39dd222", actorSignature);
    }

    public void testGetActorSignatureGeneratorNullXAnalyticsMap() {
        String actorSignature = actorSignatureGenerator.execute(
                "192.168.12.2",
                "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML like Gecko) Chrome/37.0.2062.120 Safari/537.36",
                "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
                "commons.wikimedia.org",
                "?modules=skins.minerva.mainMenu.icons&image=mapPin&format=original&skin=minerva&version=a4j1i",
                null);
        // Value generated using spark
        assertEquals("c9f49eaeaf474a818ac43604c39dd222", actorSignature);
    }

    public void testGetSimpleActorHash() {
        Map<String, String> xAnalytics = new HashMap<>();
        xAnalytics.put("page_id", "1284311");
        xAnalytics.put("WMF-Last-Access-Global", "28-Mar-2020");
        xAnalytics.put("ns", "0");
        xAnalytics.put("WMF-Last-Access", "28-Mar-2020");
        xAnalytics.put("https", "1");

        String actorSignature = actorSignatureGenerator.execute(
                "192.168.12.2",
                "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML like Gecko) Chrome/37.0.2062.120 Safari/537.36",
                "en",
                "commons.wikimedia.org",
                "?modules=skins.minerva.mainMenu.icons&image=mapPin&format=original&skin=minerva&version=a4j1i",
                xAnalytics);
        // Value generated using hive
        assertEquals("55c23befdb14a6e8f90d55973fce628c", actorSignature);
    }

    public void testGetSimpleActorHashWithWMFUUID() {
        Map<String, String> xAnalytics = new HashMap<>();
        xAnalytics.put("WMF-Last-Access-Global", "28-Mar-2020");
        xAnalytics.put("WMF-Last-Access", "28-Mar-2020");
        xAnalytics.put("wmfuuid", "c34ba606-30ec-4b40-bdbb-dcc5aeb5e3a3");
        xAnalytics.put("https", "1");

        String actorSignature = actorSignatureGenerator.execute(
                "192.168.12.2",
                "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML like Gecko) Chrome/37.0.2062.120 Safari/537.36",
                "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
                "commons.wikimedia.org",
                "?modules=skins.minerva.mainMenu.icons&image=mapPin&format=original&skin=minerva&version=a4j1i",
                xAnalytics);
        // Value generated using spark
        assertEquals("01acc2a702cef266cc99d752b44e6ffe", actorSignature);
    }

    public void testGetSimpleActorHashWithAppInstallId() {
        String actorSignature = actorSignatureGenerator.execute(
                "192.168.12.2",
                "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML like Gecko) Chrome/37.0.2062.120 Safari/537.36",
                "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
                "commons.wikimedia.org",
                "?action=mobileview&format=json&page=Wikipedia%3APortada&prop=text%7Csections%7Clastmodified%7Cnormalizedtitle%7Cdisplaytitle%7Cprotection%7Ceditable&onlyrequestedsections=1&sections=0&sectionprop=toclevel%7Cline%7Canchor&noheadings=true&appInstallID=7361b6e8-6095-4148-90e3-41b0ce0fd0ec",
                new HashMap<>());
        // Value generated using spark
        assertEquals("6e4acf39216cf7c662993f4ba7eadf33", actorSignature);
    }

    public void testGetSimpleActorHashWithAppInstallIdMiddle() {
        String actorSignature = actorSignatureGenerator.execute(
                "192.168.12.2",
                "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML like Gecko) Chrome/37.0.2062.120 Safari/537.36",
                "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
                "commons.wikimedia.org",
                "?action=mobileview&format=json&page=Wikipedia%3APortada&prop=text%7Csections%7Clastmodified%7Cnormalizedtitle%7Cdisplaytitle%7Cprotection%7Ceditable&onlyrequestedsections=1&sections=0&sectionprop=toclevel%7Cline%7Canchor&noheadings=true&appInstallID=7361b6e8-6095-4148-90e3-41b0ce0fd0ec&dummy=dummy",
                new HashMap<>());
        // Value generated using spark
        assertEquals("6e4acf39216cf7c662993f4ba7eadf33", actorSignature);
    }
}