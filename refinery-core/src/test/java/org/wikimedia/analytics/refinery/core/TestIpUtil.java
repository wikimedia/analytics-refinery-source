/**
 * Copyright (C) 2015 Wikimedia Foundation
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
import org.junit.BeforeClass;

public class TestIpUtil extends TestCase {

    private IpUtil ipUtil;

    @BeforeClass
    public void setUp() throws RuntimeException {
        ipUtil = new IpUtil();
    }

    public void testGetClientIpWithClientIpInXFF() {
        String clientIp = ipUtil.getClientIp(
                "10.0.0.0",
                "37.228.105.17,198.35.26.0"   // trusted proxy as IP, but valid IP in XFF
        );

        assertEquals("Invalid client IP address", "37.228.105.17", clientIp);
    }

    public void testGetClientIpWithTrustedProxiesInXFF() {
        String clientIp = ipUtil.getClientIp(
                "37.228.105.17",
                "10.0.0.1,198.35.26.0"   // all entries in XFF matching trusted proxies
        );

        assertEquals("Invalid client IP address", "37.228.105.17", clientIp);
    }

    public void testGetClientIpWithIpv6Address() {
        String clientIp = ipUtil.getClientIp(
                "2001:db8:0:0:0:0:0:0",
                "2a02:ec80:0000:0000:0000:0000:0000:0000,2a02:ec80:ffff:ffff:ffff:ffff:ffff:ffff"
        );

        assertEquals("Invalid client IP address", "2001:db8:0:0:0:0:0:0", clientIp);
    }

    public void testGetClientIpWithInvalidIp() {
        String clientIp = ipUtil.getClientIp(
                "invalid_ip",
                "37.228.105.17,198.35.26.0"
        );

        assertNull("Client IP address should be null", clientIp);
    }

    public void testGetClientIpWithInvalidXFFIpv4() {
        String clientIp = ipUtil.getClientIp(
                "37.228.105.17",
                "%20101.209.27.230,500.600.70.80,5.6.7,5.6.7.8.9,invalid_ip"
        );

        assertEquals("Invalid client IP address", "37.228.105.17", clientIp);
    }

    public void testGetClientIpWithInvalidXFFIpv6() {
        String clientIp = ipUtil.getClientIp(
                "37.228.105.17",
                "%20101.209.27.230,::ffff:800.200.600.500,invalid_ip"
        );

        assertEquals("Invalid client IP address", "37.228.105.17", clientIp);
    }
}