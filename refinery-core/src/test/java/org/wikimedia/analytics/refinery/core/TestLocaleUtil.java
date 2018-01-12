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

import java.io.IOException;

public class TestLocaleUtil extends TestCase {


    public void testGetKnownCountryName() {
        assertEquals("Ireland", LocaleUtil.getCountryName("IE"));
        assertEquals("Ireland", LocaleUtil.getCountryName("ie"));
    }

    public void testGetUnknownCountryName() {
        assertEquals("Unknown", LocaleUtil.getCountryName("-"));
        assertEquals("Unknown", LocaleUtil.getCountryName("--"));
        assertEquals("Unknown", LocaleUtil.getCountryName("XX"));
        assertEquals("Unknown", LocaleUtil.getCountryName("XXX"));
        assertEquals("Unknown", LocaleUtil.getCountryName("ct"));
        assertEquals("Unknown", LocaleUtil.getCountryName(null));
    }
}
