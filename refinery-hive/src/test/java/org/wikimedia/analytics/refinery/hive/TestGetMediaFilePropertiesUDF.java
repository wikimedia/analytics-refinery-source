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

import junit.framework.TestCase;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.wikimedia.analytics.refinery.core.media.MediaFileUrlInfo;
import org.wikimedia.analytics.refinery.core.media.MediaFileUrlInfo.TranscodingClassification;

import java.io.IOException;

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
                              TranscodingClassification transcodingClassification, Integer width, Integer height)
            throws HiveException, IOException {
        Object[] res = (Object[]) callUDF(url);

        assertEquals("Result array has wrong length", 9, res.length);

        assertEquals("baseName does not match", baseName, res[0]);

        assertEquals("is_original does not match", transcodingClassification == TranscodingClassification.ORIGINAL, res[3]);
        assertEquals("is_high_quality does not match", transcodingClassification == TranscodingClassification.TRANSCODED_TO_AUDIO, res[4]);
        assertEquals("is_low_quality does not match", transcodingClassification == TranscodingClassification.TRANSCODED_TO_IMAGE, res[5]);
        assertEquals("is_low_quality does not match", transcodingClassification == TranscodingClassification.TRANSCODED_TO_MOVIE, res[6]);

        if (width == null) {
            assertNull("width is not null", res[7]);
        } else {
            assertEquals("width does not match", width, res[7]);
        }

        if (height == null) {
            assertNull("height is not null", res[8]);
        } else {
            assertEquals("height does not match", height, res[8]);
        }
    }

    public void assertMediaClassification(String baseName, String mediaParentType, String extension)
            throws HiveException, IOException {
        Object[] res = (Object[]) callUDF(baseName);
        String receivedMediaParentTypeClassification = (String) res[1];
        String receivedExtension = (String) res[2];
        assertEquals(mediaParentType, receivedMediaParentTypeClassification);
        assertEquals(extension, receivedExtension);
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
        assertOutput("foo", null, TranscodingClassification.UNKNOWN, null, null);
    }

    public void testEvaluateOriginal() throws HiveException, IOException {
        String baseName = "/math/d/a/9/da9d325123d50dbc4e36363f2863ce3e.png";
        assertOutput(baseName, baseName,
                TranscodingClassification.ORIGINAL, null, null);
        assertMediaClassification(baseName, "image", "png");
    }

    public void testEvaluateAudio() throws HiveException, IOException {
        String baseName = "/wikipedia/commons/b/bd/Xylophone_jingle.wav";
        assertOutput(
                "/wikipedia/commons/transcoded/b/bd/Xylophone_jingle.wav/Xylophone_jingle.wav.ogg",
                baseName,
                TranscodingClassification.TRANSCODED_TO_AUDIO, null, null);
        assertMediaClassification(baseName, "audio", "wav");
    }

    public void testEvaluateImageWithWidth() throws HiveException, IOException {
        String baseName = "/wikipedia/commons/a/ae/Flag_of_the_United_Kingdom.svg";
        assertOutput(
                "/wikipedia/commons/thumb/a/ae/Flag_of_the_United_Kingdom.svg/1024px-Flag_of_the_United_Kingdom.svg.png",
                baseName,
                TranscodingClassification.TRANSCODED_TO_IMAGE, 1024, null);
        assertMediaClassification(baseName, "image", "svg");
    }

    public void testEvaluateImageWithoutWidth() throws HiveException, IOException {
        String baseName = "/wikipedia/commons/a/ae/Flag_of_the_United_Kingdom.svg";
        assertOutput(
                "/wikipedia/commons/thumb/a/ae/Flag_of_the_United_Kingdom.svg/mid-Flag_of_the_United_Kingdom.svg.png",
                baseName,
                MediaFileUrlInfo.TranscodingClassification.TRANSCODED_TO_IMAGE, null, null);
        assertMediaClassification(baseName, "image", "svg");
    }

    public void testEvaluateMovie() throws HiveException, IOException {
        String baseName = "/wikipedia/commons/3/31/Lheure_du_foo.ogv";
        assertOutput(
                "/wikipedia/commons/transcoded/3/31/Lheure_du_foo.ogv/Lheure_du_foo.ogv.360p.webm",
                baseName,
                TranscodingClassification.TRANSCODED_TO_MOVIE, null, 360);
        assertMediaClassification(baseName, "video", "ogv");
    }

    public void testEncodedInput() throws HiveException, IOException {
        String baseName = "/wikipedia/commons/a/ae/Foo.svg";
        assertOutput(
                "/wikipedia/commons/a/ae/F%6F%6f.svg",
                baseName,
                TranscodingClassification.ORIGINAL, null, null);
        assertMediaClassification(baseName, "image", "svg");
    }

    public void testEncodedOutput() throws HiveException, IOException {
        String baseName = "/wikipedia/commons/a/ae/F%0Ao%E2%80%94o.svg";
        assertOutput(
                "/wikipedia/commons/a/ae/F\no—o.svg",
                baseName,
                TranscodingClassification.ORIGINAL, null, null);
        assertMediaClassification(baseName, "image", "svg");
    }

    public void testUnknownMedia() throws HiveException, IOException {
        String baseName = "/wikipedia/commons/a/ae/Foo.wololo";
        assertOutput(
                "/wikipedia/commons/a/ae/F%6F%6f.wololo",
                baseName,
                TranscodingClassification.ORIGINAL, null, null);
        assertMediaClassification(baseName, "other", "wololo");
    }

    public void testExtensionsConsolidatedToLongformName() throws HiveException, IOException {
        // JPG file type should be filed as JPEG, TIF as TIFF, MID as MIDI...
        String baseName = "/wikipedia/commons/a/ae/Foo.jpg";
        assertOutput(
            "/wikipedia/commons/a/ae/Foo.jpg",
            baseName,
            TranscodingClassification.ORIGINAL, null, null);
        assertMediaClassification(baseName, "image", "jpeg");
    }
}
