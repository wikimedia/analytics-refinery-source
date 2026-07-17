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

import java.nio.charset.StandardCharsets;

/**
 * Reimplementation of
 * <a href="https://github.com/haproxy/haproxy/blob/c4deec58198b2327187f115deb0ea52543d602a4/src/hash.c#L78">
 *     haproxy's version
 * </a>
 * of the SDBM hashing algorithm in Java. It hashes over the UTF-8 representation,
 * and uses long instead of unsigned int (Java does not have unsigned types)
 */
public final class Sdbm {

    private Sdbm() {
    }

    /**
     * Computes the SDBM hash of the UTF-8 bytes of the given string.
     *
     * @param str the string to hash
     * @return the unsigned int (represented as a long) SDBM hash
     *         (empty string hashes to 0)
     */
    public static long hash(String str) {
        return hash(str.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Computes the SDBM hash of the given bytes.
     *
     * @param bytes the bytes to hash
     * @return the unsigned int (represented as a long) SDBM hash
     *         (empty input hashes to 0)
     */
    public static long hash(byte[] bytes) {
        int hash = 0;
        for (byte b : bytes) {
            int c = Byte.toUnsignedInt(b);
            hash = c + (hash << 6) + (hash << 16) - hash;
        }
        // Java doesn't have unsigned int so pretend it's unsigned by converting it to a long that is always positive
        return Integer.toUnsignedLong(hash);
    }
}
