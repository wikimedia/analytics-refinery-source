/**
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

import org.junit.Assert;
import org.junit.Test;

public class TestSanitizeXAnalyticsWprovUDF {

    @Test
    public void evaluateDelegatesToCoreSanitizer() {
        SanitizeXAnalyticsWprovUDF udf = new SanitizeXAnalyticsWprovUDF();
        Assert.assertNull(udf.evaluate(null));
        Assert.assertEquals(
                "https=1;wprov=sfla1;mf-m=b",
                udf.evaluate("https=1;wprov=sfla1https://en.wikipedia.org/wiki/Foo;mf-m=b"));
        Assert.assertEquals(
                "wprov=srpw1",
                udf.evaluate("wprov=srpw1_0"));
    }
}
