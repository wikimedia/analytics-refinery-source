/**
 * Copyright (C) 2015  Wikimedia Foundation
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

import static org.junit.Assert.*;
import org.junit.Test;

public class TestGetHexDecodedIpUDF {

    @Test
    public void testEvaluateIpv6() {
        GetHexDecodedIpUDF getHexDecodedIpUDF = new GetHexDecodedIpUDF();
        String ipHex  = "v6-2A0023C8F30E65012C6AA9F9A7E89223";
        assertEquals("2A00:23C8:F30E:6501:2C6A:A9F9:A7E8:9223", getHexDecodedIpUDF.evaluate(ipHex));
    }

    @Test
    public void testEvaluateIpv4() {
        GetHexDecodedIpUDF getHexDecodedIpUDF = new GetHexDecodedIpUDF();
        String ipHex  = "560A2310";
        assertEquals("86.10.35.16", getHexDecodedIpUDF.evaluate(ipHex));
    }

    @Test
    public void testEvaluateWithInvalidIp() {
        GetHexDecodedIpUDF getHexDecodedIpUDF = new GetHexDecodedIpUDF();
        String ipHex  = "";
        assertEquals("0.0.0.0", getHexDecodedIpUDF.evaluate(ipHex));
    }
}
