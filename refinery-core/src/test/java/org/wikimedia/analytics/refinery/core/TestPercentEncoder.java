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

public class TestPercentEncoder extends TestCase {
    public void assertEncoded(String unencoded, String expected) {
        String encoded = PercentEncoder.encode(unencoded);
        assertEquals(expected, encoded);
    }

    public void testNull() {
        assertEncoded(null, null);
    }

    public void testEmpty() {
        assertEncoded("", "");
    }

    public void testPlainString() {
        assertEncoded("foo", "foo");
    }

    public void testEncodedTab() {
        assertEncoded("foo	bar", "foo%09bar");
    }

    public void testEncodedCarriageReturn() {
        assertEncoded("foo\rbar", "foo%0Dbar");
    }

    public void testEncodedNewline() {
        assertEncoded("foo\nbar", "foo%0Abar");
    }

    public void testEncodedSpace() {
        assertEncoded("foo bar", "foo%20bar");
    }

    public void testLeaingSlashAlone() {
        assertEncoded("foo/bar", "foo/bar");
    }

    public void testEncodedUmlaut() {
        assertEncoded("fooäbar", "foo%C3%A4bar");
    }

    public void testEmDash() {
        assertEncoded("foo—bar", "foo%E2%80%94bar");
    }

    public void testEncodingEncoded() {
        assertEncoded("foo%25bar", "foo%25bar");
    }
}
