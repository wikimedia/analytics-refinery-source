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
package org.wikimedia.analytics.refinery.hive;

import org.wikimedia.analytics.refinery.core.IpUtil;
import org.wikimedia.analytics.refinery.core.IpUtil.NetworkOrigin;

import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestNetworkOriginUDF {

    private static NetworkOriginUDF fixture;

    @BeforeClass
    public static void setUp() throws RuntimeException {
        fixture = new NetworkOriginUDF();
    }

    @Test
    public void testEvaluateWithLabsIpv4() {
        assertEquals(NetworkOrigin.WIKIMEDIA_LABS.toString(), fixture.evaluate("10.68.16.44"));
    }

    @Test
    public void testEvaluateWithLabsIpv6() {
        assertEquals(NetworkOrigin.WIKIMEDIA_LABS.toString(), fixture.evaluate("2620:0:861:204::dead:beef"));
    }

    @Test
    public void testEvaluateWithWikimediaIpv4() {
        assertEquals(NetworkOrigin.WIKIMEDIA.toString(), fixture.evaluate("10.64.0.162"));
    }

    @Test
    public void testEvaluateWithWikimediaIpv6() {
        assertEquals(NetworkOrigin.WIKIMEDIA.toString(), fixture.evaluate("2620:0:861:101:46a8:42ff:fe11:686b"));
    }

    @Test
    public void testEvaluateWithInternetIpv4() {
        assertEquals(NetworkOrigin.INTERNET.toString(), fixture.evaluate("159.118.124.57"));
    }

    @Test
    public void testEvaluateWithInternetIpv6() {
        assertEquals(NetworkOrigin.INTERNET.toString(), fixture.evaluate("2001:470:b:530:a17c:bb90:9583:7620"));
    }

    @Test
    public void testEvaluateWithInvalidIp() {
        assertEquals(NetworkOrigin.INTERNET.toString(), fixture.evaluate("xyzzy"));
    }
}
