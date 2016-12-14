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

import org.junit.Test;
import static org.junit.Assert.*;

public class TestGetClientIpUDF {

    @Test
    public void testEvaluate() {
        GetClientIpUDF getClientIpUDF = new GetClientIpUDF();
        String ip  = "208.80.154.133"; //Trusted proxy
        String xff = "127.0.0.1,96.56.123.2";
        assertEquals("96.56.123.2", getClientIpUDF.evaluate(ip, xff));
    }

    @Test
    public void testEvaluateWithInvalidXFF() {
        GetClientIpUDF getClientIpUDF = new GetClientIpUDF();
        String ip  = "208.80.154.133"; //Trusted proxy
        String xff = "127.0.0.1,%20101.209.27.230";
        assertEquals("208.80.154.133", getClientIpUDF.evaluate(ip, xff));
    }
}
