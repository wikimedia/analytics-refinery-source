/*
 * Copyright (C) 2026  Wikimedia Foundation
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;

import org.junit.Test;

/**
 * Assert values provided by running it through
 * <a href="https://github.com/haproxy/haproxy/blob/c4deec58198b2327187f115deb0ea52543d602a4/src/hash.c#L78">
 *     haproxy's implementation
 * </a>.
 */
public class TestSdbm {

    @Test
    public void testEmptyStringHashesToZero() {
        assertEquals(0L, Sdbm.hash(""));
    }

    @Test
    public void testSingleByteEqualsItsCodePoint() {
        // For a one-character input the hash is just the byte value.
        assertEquals(97L, Sdbm.hash("a"));
    }

    @Test
    public void testMatchesCReference() {
        assertEquals(6363201L, Sdbm.hash("ab"));
        assertEquals(807794786L, Sdbm.hash("abc"));
        assertEquals(2154307799L, Sdbm.hash("Wikipedia"));
        assertEquals(430867652L, Sdbm.hash("hello world"));
        assertEquals(2359783795L,
                Sdbm.hash("The quick brown fox jumps over the lazy dog"));
    }

    @Test
    public void testResultIsAlwaysUnsigned32Bit() {
        // Even inputs whose 32-bit hash has the high bit set stay non-negative
        // and within the unsigned 32-bit range (C returns 2154307799 here).
        long h = Sdbm.hash("Wikipedia");
        assertTrue(h >= 0L);
        assertTrue(h <= 0xFFFFFFFFL);
    }

    @Test
    public void testUsesUtf8Bytes() {
        // Non-ASCII input is hashed over its UTF-8 byte representation,
        // matching the C reference fed the same UTF-8 bytes.
        assertEquals(410727374L, Sdbm.hash("café"));
        assertEquals(3886153882L, Sdbm.hash("日本語"));
        // The String and byte[] overloads agree for the same UTF-8 encoding.
        assertEquals(Sdbm.hash("café"),
                Sdbm.hash("café".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testIsDeterministic() {
        assertEquals(Sdbm.hash("repeatable"), Sdbm.hash("repeatable"));
    }
}
