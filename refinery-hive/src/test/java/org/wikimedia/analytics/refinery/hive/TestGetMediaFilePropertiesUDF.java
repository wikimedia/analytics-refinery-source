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
import org.wikimedia.analytics.refinery.core.MediaFileUrlInfo.Classification;

import junit.framework.TestCase;

public class TestGetMediaFilePropertiesUDF extends TestCase {
    ObjectInspector StringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector LongOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector;

    private Object callUDF(String url) throws HiveException, IOException {
        DeferredObject urlDO = new DeferredJavaObject(url);
        DeferredObject[] arguments = new DeferredObject[] {urlDO};
        Object res = null;

        GetMediaFilePropertiesUDF udf = new GetMediaFilePropertiesUDF();
        try {
            udf.initialize(new ObjectInspector[]{StringOI});
            res = udf.evaluate(arguments);
        } finally {
            udf.close();
        }
        return res;
    }

    private void assertOutput(String url, String baseName,
            Classification classification, Integer width, Integer height)
            throws HiveException, IOException {
        Object[] res = (Object[]) callUDF(url);

        assertEquals("Result array has wrong length", 7, res.length);

        assertEquals("baseName does not match", baseName, res[0]);

        assertEquals("is_original does not match", classification == Classification.ORIGINAL, res[1]);
        assertEquals("is_high_quality does not match", classification == Classification.TRANSCODED_TO_AUDIO, res[2]);
        assertEquals("is_low_quality does not match", classification == Classification.TRANSCODED_TO_IMAGE, res[3]);
        assertEquals("is_low_quality does not match", classification == Classification.TRANSCODED_TO_MOVIE, res[4]);

        if (width == null) {
            assertNull("width is not null", res[5]);
        } else {
            assertEquals("width does not match", width, res[5]);
        }

        if (height == null) {
            assertNull("height is not null", res[6]);
        } else {
            assertEquals("height does not match", height, res[6]);
        }
    }

    public void testInitialize() throws HiveException, IOException {
        GetMediaFilePropertiesUDF udf = new GetMediaFilePropertiesUDF();
        try {
            udf.initialize(new ObjectInspector[]{StringOI});
        } finally {
            udf.close();
        }
    }

    public void testInitializeEmpty() throws HiveException, IOException {
        GetMediaFilePropertiesUDF udf = new GetMediaFilePropertiesUDF();
        try {
            udf.initialize(new ObjectInspector[]{});
            fail("Initialize did not throw HiveException");
        } catch (HiveException e) {
        } finally {
            udf.close();
        }
    }

    public void testInitializeWrongType() throws HiveException, IOException {
        GetMediaFilePropertiesUDF udf = new GetMediaFilePropertiesUDF();
        try {
            udf.initialize(new ObjectInspector[]{LongOI});
            fail("Initialize did not throw HiveException");
        } catch (HiveException e) {
        } finally {
            udf.close();
        }
    }

    public void testEvaluateUnknown() throws HiveException, IOException {
        assertOutput("foo", null, Classification.UNKNOWN, null, null);
    }

    public void testEvaluateOriginal() throws HiveException, IOException {
        assertOutput("/math/d/a/9/da9d325123d50dbc4e36363f2863ce3e.png",
                "/math/d/a/9/da9d325123d50dbc4e36363f2863ce3e.png",
                Classification.ORIGINAL, null, null);
    }

    public void testEvaluateAudio() throws HiveException, IOException {
        assertOutput(
                "/wikipedia/commons/transcoded/b/bd/Xylophone_jingle.wav/Xylophone_jingle.wav.ogg",
                "/wikipedia/commons/b/bd/Xylophone_jingle.wav",
                Classification.TRANSCODED_TO_AUDIO, null, null);
    }

    public void testEvaluateImageWithWidth() throws HiveException, IOException {
        assertOutput(
                "/wikipedia/commons/thumb/a/ae/Flag_of_the_United_Kingdom.svg/1024px-Flag_of_the_United_Kingdom.svg.png",
                "/wikipedia/commons/a/ae/Flag_of_the_United_Kingdom.svg",
                Classification.TRANSCODED_TO_IMAGE, 1024, null);
    }

    public void testEvaluateImageWithoutWidth() throws HiveException, IOException {
        assertOutput(
                "/wikipedia/commons/thumb/a/ae/Flag_of_the_United_Kingdom.svg/mid-Flag_of_the_United_Kingdom.svg.png",
                "/wikipedia/commons/a/ae/Flag_of_the_United_Kingdom.svg",
                Classification.TRANSCODED_TO_IMAGE, null, null);
    }

    public void testEvaluateMovie() throws HiveException, IOException {
        assertOutput(
                "/wikipedia/commons/transcoded/3/31/Lheure_du_foo.ogv/Lheure_du_foo.ogv.360p.webm",
                "/wikipedia/commons/3/31/Lheure_du_foo.ogv",
                Classification.TRANSCODED_TO_MOVIE, null, 360);
    }

    public void testEncodedInput() throws HiveException, IOException {
        assertOutput(
                "/wikipedia/commons/a/ae/F%6F%6f.svg",
                "/wikipedia/commons/a/ae/Foo.svg",
                Classification.ORIGINAL, null, null);
    }

    public void testEncodedOutput() throws HiveException, IOException {
        assertOutput(
                "/wikipedia/commons/a/ae/F\no—o.svg",
                "/wikipedia/commons/a/ae/F%0Ao%E2%80%94o.svg",
                Classification.ORIGINAL, null, null);
    }
}