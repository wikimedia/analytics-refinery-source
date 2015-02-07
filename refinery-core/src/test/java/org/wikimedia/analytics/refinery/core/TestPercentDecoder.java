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

public class TestPercentDecoder extends TestCase {
    public void assertDecoded(String encoded, String expected) {
        String decoded = PercentDecoder.decode(encoded);
        assertEquals(expected, decoded);
    }

    public void testNull() {
        assertDecoded(null, null);
    }

    public void testEmpty() {
        assertDecoded("", "");
    }

    public void testPlainString() {
        assertDecoded("foo", "foo");
    }

    public void testEncodedPercent() {
        assertDecoded("foo%25bar", "foo%bar");
    }

    public void testEncodedSlashUppercase() {
        assertDecoded("foo%2Fbar", "foo/bar");
    }

    public void testEncodedSlashLowercase() {
        assertDecoded("foo%2fbar", "foo/bar");
    }

    public void testEncodedSpace() {
        assertDecoded("foo%20bar", "foo bar");
    }

    public void testUnencodedPercentFirstCharacterNonHex() {
        assertDecoded("100% sure", "100% sure");
    }

    public void testUnencodedPercentFirstCharacterHex() {
        assertDecoded("100%foo", "100%foo");
    }

    public void testTrailingEncoding1() {
        assertDecoded("foo%2f", "foo/");
    }

    public void testTrailingEncoding2() {
        assertDecoded("foo%2", "foo%2");
    }

    public void testTrailingEncoding3() {
        assertDecoded("foo%", "foo%");
    }
    public void testMultpileEncoded() {
        assertDecoded("%2F%2525/%2f", "/%25//");
    }
}
