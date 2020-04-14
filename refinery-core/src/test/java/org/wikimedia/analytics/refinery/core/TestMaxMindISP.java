// Copyright 2014 Wikimedia Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wikimedia.analytics.refinery.core;

import junit.framework.TestCase;
import org.junit.BeforeClass;
import org.wikimedia.analytics.refinery.core.maxmind.ISPDatabaseReader;

import java.io.IOException;
import java.util.Map;

public class TestMaxMindISP extends TestCase {

    private ISPDatabaseReader maxMindISP ;

    @BeforeClass
    public void setUp() throws IOException {
        maxMindISP = new ISPDatabaseReader();
    }

    public void testDoISPDataLookupIPv4() {
        //IPv4 addresses taken from MaxMind's test suite
        String ip = "82.99.17.96";

        Map<String, String> ispData = maxMindISP.getResponse(ip).getMap();
        assertNotNull("ISP data cannot be null", ispData);
        assertEquals("IP-Only Telecommunication Networks AB", ispData.get("isp"));
        assertEquals("Effectiv Solutions", ispData.get("organization"));
        assertEquals("IP-Only", ispData.get("autonomous_system_organization"));
        assertEquals("12552", ispData.get("autonomous_system_number"));
    }

    public void testDoISPLookupIpv6() {
        //IPv6 representation of an IPv4 address taken from MaxMind's test suite
        String ip = "::ffff:82.99.17.96";

        Map<String, String> ispData = maxMindISP.getResponse(ip).getMap();
        assertNotNull("ISP data cannot be null", ispData);
        assertEquals("IP-Only Telecommunication Networks AB", ispData.get("isp"));
        assertEquals("Effectiv Solutions", ispData.get("organization"));
        assertEquals("IP-Only", ispData.get("autonomous_system_organization"));
        assertEquals("12552", ispData.get("autonomous_system_number"));
    }

    public void testDoGeoLookupIpUnknown() {
        // Invalid or unknown IP address
        String ip = "-";

        Map<String, String> ispData = maxMindISP.getResponse(ip).getMap();
        assertNotNull("ISP data cannot be null", ispData);
        assertEquals("Unknown", ispData.get("isp"));
        assertEquals("Unknown", ispData.get("organization"));
        assertEquals("Unknown", ispData.get("autonomous_system_organization"));
        assertEquals("-1", ispData.get("autonomous_system_number"));
    }

    public void testDoGeoLookupWithNull() {
        // Invalid IP address
        String ip = null;

        Map<String, String> ispData = maxMindISP.getResponse(ip).getMap();
        assertNotNull("ISP data cannot be null", ispData);
        assertEquals("Unknown", ispData.get("isp"));
        assertEquals("Unknown", ispData.get("organization"));
        assertEquals("Unknown", ispData.get("autonomous_system_organization"));
        assertEquals("-1", ispData.get("autonomous_system_number"));
    }

}
