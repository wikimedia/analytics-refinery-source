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
import org.wikimedia.analytics.refinery.core.maxmind.CountryDatabaseReader;
import org.wikimedia.analytics.refinery.core.maxmind.RefineryCountryDatabaseResponse;

import java.io.IOException;

public class TestMaxMindCountryCode extends TestCase {

    private CountryDatabaseReader maxMindCountryDBReader;

    @BeforeClass
    public void setUp() throws IOException {
        maxMindCountryDBReader = new CountryDatabaseReader();
    }

    public void testGeoCountryLookup() {
        //IPv4 addresses taken from MaxMind's test suite
        String ip = "81.2.69.160";
        RefineryCountryDatabaseResponse response = maxMindCountryDBReader.getResponse(ip);
        assertEquals("GB", maxMindCountryDBReader.getResponse(ip).getCountryCode());
        assertEquals("--", maxMindCountryDBReader.getResponse("-").getCountryCode());
        assertEquals("--", maxMindCountryDBReader.getResponse(null).getCountryCode());
    }

}
