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

package org.wikimedia.analytics.refinery.hive;

import java.io.IOException;

import org.apache.hadoop.hive.ql.metadata.HiveException;

import junit.framework.TestCase;

import org.wikimedia.analytics.refinery.core.Referer;

@Deprecated
public class TestGetRefererTypeUDF extends TestCase {

    public void testEvaluateUnknown() throws HiveException, IOException {
        GetRefererTypeUDF udf = new GetRefererTypeUDF();
        assertEquals("Unknown referer", udf.evaluate("foo"), Referer.UNKNOWN.getRefLabel());
    }

    public void testEvaluateInternal() throws HiveException, IOException {
        GetRefererTypeUDF udf = new GetRefererTypeUDF();
        assertEquals("Unknown referer", udf.evaluate("http://en.wikipedia.org/foo"), Referer.INTERNAL.getRefLabel());
    }

    public void testEvaluateExternal() throws HiveException, IOException {
        GetRefererTypeUDF udf = new GetRefererTypeUDF();
        assertEquals("Unknown referer", udf.evaluate("http://www.google.com/"), Referer.SEARCH_ENGINE.getRefLabel());
    }
}