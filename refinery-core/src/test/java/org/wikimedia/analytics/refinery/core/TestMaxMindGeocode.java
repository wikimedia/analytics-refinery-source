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
import org.wikimedia.analytics.refinery.core.maxmind.GeocodeDatabaseReader;
import org.wikimedia.analytics.refinery.core.maxmind.MaxmindDatabaseReaderFactory;

import java.io.IOException;
import java.util.Map;

public class TestMaxMindGeocode extends TestCase {

    private GeocodeDatabaseReader maxMindGeocode;

    @BeforeClass
    public void setUp() throws IOException {
        maxMindGeocode =  MaxmindDatabaseReaderFactory.getInstance().getGeocodeDatabaseReader();
    }



    public void testGeoDataLookupIPv4() {
        //IPv4 addresses taken from MaxMind's test suite
        String ip = "81.2.69.160";

        Map<String, String> geoData = maxMindGeocode.getResponse(ip).getMap();

        assertNotNull("Geo data cannot be null", geoData);
        assertEquals("Europe", geoData.get("continent"));
        assertEquals("GB", geoData.get("country_code"));
        assertEquals("United Kingdom", geoData.get("country"));
        assertEquals("England", geoData.get("subdivision"));
        assertEquals("London", geoData.get("city"));
        assertEquals("Unknown", geoData.get("postal_code"));
        assertEquals("51.5142", geoData.get("latitude"));
        assertEquals("-0.0931", geoData.get("longitude"));
        assertEquals("Europe/London", geoData.get("timezone"));
    }

    public void testDoGeoLookupIpv6() {
        //IPv6 representation of an IPv4 address taken from MaxMind's test suite
        String ip = "::ffff:81.2.69.160";

        Map<String, String> geoData = maxMindGeocode.getResponse(ip).getMap();
        assertNotNull("Geo data cannot be null", geoData);
        assertEquals("Europe", geoData.get("continent"));
        assertEquals("GB", geoData.get("country_code"));
        assertEquals("United Kingdom", geoData.get("country"));
        assertEquals("England", geoData.get("subdivision"));
        assertEquals("London", geoData.get("city"));
        assertEquals("Unknown", geoData.get("postal_code"));
        assertEquals("51.5142", geoData.get("latitude"));
        assertEquals("-0.0931", geoData.get("longitude"));
        assertEquals("Europe/London", geoData.get("timezone"));
    }

    public void testDoGeoLookupIpUnknown() {
        // Invalid or unknown IP address
        String ip = "-";

        Map<String, String> geoData = maxMindGeocode.getResponse(ip).getMap();
        assertNotNull("Geo data cannot be null", geoData);
        assertEquals("--", geoData.get("country_code"));
        assertEquals("Unknown", geoData.get("continent"));
        assertEquals("--", geoData.get("country_code"));
        assertEquals("Unknown", geoData.get("country"));
        assertEquals("Unknown", geoData.get("subdivision"));
        assertEquals("Unknown", geoData.get("city"));
        assertEquals("Unknown", geoData.get("postal_code"));
        assertEquals("-1.0", geoData.get("latitude"));
        assertEquals("-1.0", geoData.get("longitude"));
        assertEquals("Unknown", geoData.get("timezone"));
    }

    public void testDoGeoLookupWithNull() {
        // Invalid IP address
        String ip = null;

        Map<String, String> geoData = maxMindGeocode.getResponse(ip).getMap();
        assertNotNull("Geo data cannot be null", geoData);
        assertEquals("Unknown", geoData.get("continent"));
        assertEquals("--", geoData.get("country_code"));
        assertEquals("Unknown", geoData.get("country"));
        assertEquals("Unknown", geoData.get("subdivision"));
        assertEquals("Unknown", geoData.get("city"));
        assertEquals("Unknown", geoData.get("postal_code"));
        assertEquals("-1.0", geoData.get("latitude"));
        assertEquals("-1.0", geoData.get("longitude"));
        assertEquals("Unknown", geoData.get("timezone"));
    }

}
