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
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.wikimedia.analytics.refinery.core.Webrequest.RefererClassification;

import junit.framework.TestCase;

public class TestRefererClassifierUDF extends TestCase {
    ObjectInspector StringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector LongOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector;

    private Object callUDF(String url) throws HiveException, IOException {
        DeferredObject urlDO = new DeferredJavaObject(url);
        DeferredObject[] arguments = new DeferredObject[] {urlDO};
        Object res = null;

        RefererClassifierUDF udf = new RefererClassifierUDF();
        try {
            udf.initialize(new ObjectInspector[]{StringOI});
            res = udf.evaluate(arguments);
        } finally {
            udf.close();
        }
        return res;
    }

    private void assertKind(String url, RefererClassification kind)
            throws HiveException, IOException {
        Object[] res = (Object[]) callUDF(url);

        assertEquals("Result array has wrong length", 3, res.length);

        assertEquals("is_unknown does not match", kind == RefererClassification.UNKNOWN, res[0]);
        assertEquals("is_internal does not match", kind == RefererClassification.INTERNAL, res[1]);
        assertEquals("is_external does not match", kind == RefererClassification.EXTERNAL, res[2]);
    }

    public void testInitialize() throws HiveException, IOException {
        RefererClassifierUDF udf = new RefererClassifierUDF();
        try {
            udf.initialize(new ObjectInspector[]{StringOI});
        } finally {
            udf.close();
        }
    }

    public void testInitializeEmpty() throws HiveException, IOException {
        RefererClassifierUDF udf = new RefererClassifierUDF();
        try {
            udf.initialize(new ObjectInspector[]{});
            fail("Initialize did not throw HiveException");
        } catch (HiveException e) {
        } finally {
            udf.close();
        }
    }

    public void testInitializeWrongType() throws HiveException, IOException {
        RefererClassifierUDF udf = new RefererClassifierUDF();
        try {
            udf.initialize(new ObjectInspector[]{LongOI});
            fail("Initialize did not throw HiveException");
        } catch (HiveException e) {
        } finally {
            udf.close();
        }
    }

    public void testEvaluateUnknown() throws HiveException, IOException {
        assertKind("foo", RefererClassification.UNKNOWN);
    }

    public void testEvaluateInternal() throws HiveException, IOException {
        assertKind("http://en.wikipedia.org/foo", RefererClassification.INTERNAL);
    }

    public void testEvaluateExternal() throws HiveException, IOException {
        assertKind("http://www.google.com/", RefererClassification.EXTERNAL);
    }
}