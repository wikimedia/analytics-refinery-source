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
package org.wikimedia.analytics.refinery.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

/**
 * Assert values provided by running it through
 * <a href="https://github.com/haproxy/haproxy/blob/c4deec58198b2327187f115deb0ea52543d602a4/src/hash.c#L78">
 *     haproxy's implementation
 * </a>.
 */
public class TestSdbmHashUDF {

    private final SdbmHashUDF udf = new SdbmHashUDF();

    @Test
    public void testNullReturnsNull() {
        assertNull(udf.evaluate(null));
    }

    @Test
    public void testEmptyString() {
        assertEquals(Long.valueOf(0L), udf.evaluate(""));
    }

    @Test
    public void testMatchesCReference() {
        assertEquals(Long.valueOf(807794786L), udf.evaluate("abc"));
        assertEquals(Long.valueOf(2154307799L), udf.evaluate("Wikipedia"));
    }
}
