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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.wikimedia.analytics.refinery.core.media.MediaType;
import org.wikimedia.analytics.refinery.core.media.MediaFileUrlParser;
import org.wikimedia.analytics.refinery.core.media.MediaFileUrlInfo;
import org.wikimedia.analytics.refinery.core.media.MediaFileUrlInfo.TranscodingClassification;
import org.wikimedia.analytics.refinery.core.PercentEncoder;

import java.util.LinkedList;
import java.util.List;

/**
 * Hive UDF to extract information out of upload.wikimedia.org urls
 * <p>
 * The UDF will return a map with the following keys:
 * <ul>
 * <li>{@code base_name} String. base_name of the file. (Without thumbs, transcodings, etc.)</li>
 * <li>{@code media_classification} String. general type of media (image, audio, video...)</li>
 * <li>{@code file_type} String. extension of original file before transcoding, if applicable</li>
 * <li>{@code is_original} bool true iff the url is for the raw, original uploaded file</li>
 * <li>{@code is_transcoded_to_audio} bool true iff the url is for a transcoding to audio</li>
 * <li>{@code is_transcoded_to_image} bool true iff the url is for a transcoding to an image</li>
 * <li>{@code is_transcoded_to_movie} bool true iff the url is for a transcoding to a movie</li>
 * <li>{@code width} Integer Width of transcoded images (may be null)</li>
 * <li>{@code height} Integer Height of transcoded movies (may be null)</li>
 * </ul>
 */
// "deterministic" is the default anyways, but we want to make it visible,
// hence we explicitly set it.:
@UDFType(deterministic = true)
@Description(name = "get_media_file_properties",
        value = "_FUNC_(url) - Returns a map of details to a media file url",
        extended = "argument 0 is the url to analyze")
public class GetMediaFilePropertiesUDF extends GenericUDF {
    private Object[] result;

    private StringObjectInspector inputOI;

    private int IDX_BASE_NAME;
    private int IDX_IS_ORIGINAL;
    private int IDX_IS_TRANSCODED_AUDIO;
    private int IDX_IS_TRANSCODED_IMAGE;
    private int IDX_IS_TRANSCODED_MOVIE;
    private int IDX_WIDTH;
    private int IDX_HEIGHT;
    private int IDX_MEDIA_CLASSIFICATION;
    private int IDX_FILE_EXTENSION;
    private int IDX_TRANSCODING;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {
        checkArgsSize(arguments, 1, 1);

        // ... and the parameter has to be a string
        if (!(arguments[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(0, "The parameter to "
                    + "GetMediaFileProperties has to be a string");
        }

        inputOI = (StringObjectInspector) arguments[0];

        List<String> fieldNames = new LinkedList<String>();
        List<ObjectInspector> fieldOIs= new LinkedList<ObjectInspector>();
        int idx = 0;

        fieldNames.add("base_name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        IDX_BASE_NAME=idx++;

        fieldNames.add("media_classification");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        IDX_MEDIA_CLASSIFICATION =idx++;

        fieldNames.add("file_type");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        IDX_FILE_EXTENSION =idx++;

        fieldNames.add("is_original");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
        IDX_IS_ORIGINAL=idx++;

        fieldNames.add("is_transcoded_to_audio");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
        IDX_IS_TRANSCODED_AUDIO=idx++;

        fieldNames.add("is_transcoded_to_image");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
        IDX_IS_TRANSCODED_IMAGE=idx++;

        fieldNames.add("is_transcoded_to_movie");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
        IDX_IS_TRANSCODED_MOVIE=idx++;

        fieldNames.add("width");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        IDX_WIDTH=idx++;

        fieldNames.add("height");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        IDX_HEIGHT=idx++;

        fieldNames.add("transcoding");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        IDX_TRANSCODING =idx++;

        result = new Object[idx];

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert arguments != null : "Method 'evaluate' of GetMediaFileProperties "
                + "called with null arguments array";
        assert arguments.length == 1 : "Method 'evaluate' of "
                + "GetMediaFileProperties called arguments of length "
                + arguments.length + " (instead of 1)";
        // arguments is an array with exactly 1 entry.

        assert result != null : "Result object has not yet been initialized, "
                + "but evaluate called";
        // result object has been initialized. So it's an array of objects of
        // the right length.

        String url = inputOI.getPrimitiveJavaObject(arguments[0].get());

        MediaFileUrlInfo info = MediaFileUrlParser.parse(url);

        if (info == null) {
            result[IDX_BASE_NAME] = null;

            result[IDX_FILE_EXTENSION] = null;
            result[IDX_MEDIA_CLASSIFICATION] = null;

            result[IDX_IS_ORIGINAL] = false;
            result[IDX_IS_TRANSCODED_AUDIO] = false;
            result[IDX_IS_TRANSCODED_IMAGE] = false;
            result[IDX_IS_TRANSCODED_MOVIE] = false;

            result[IDX_WIDTH] = null;
            result[IDX_HEIGHT] = null;

            result[IDX_TRANSCODING] = null;
        } else {
            result[IDX_BASE_NAME] = PercentEncoder.encode(info.getBaseName());

            MediaType mediaType = info.getMediaType();
            result[IDX_FILE_EXTENSION] = mediaType.getFileExtension();
            result[IDX_MEDIA_CLASSIFICATION] = mediaType.getParentType();

            TranscodingClassification transcodingClassification = info.getTranscodingClassification();
            result[IDX_IS_ORIGINAL] = (transcodingClassification == MediaFileUrlInfo.TranscodingClassification.ORIGINAL);
            result[IDX_IS_TRANSCODED_AUDIO] = (transcodingClassification == MediaFileUrlInfo.TranscodingClassification.TRANSCODED_TO_AUDIO);
            result[IDX_IS_TRANSCODED_IMAGE] = (transcodingClassification == MediaFileUrlInfo.TranscodingClassification.TRANSCODED_TO_IMAGE);
            result[IDX_IS_TRANSCODED_MOVIE] = (transcodingClassification == MediaFileUrlInfo.TranscodingClassification.TRANSCODED_TO_MOVIE);

            result[IDX_WIDTH] = info.getWidth();
            result[IDX_HEIGHT] = info.getHeight();

            result[IDX_TRANSCODING] = info.getTranscoding();
        }

        return result;
    }

    @Override
    public String getDisplayString(String[] arguments) {
        String argument;
        if (arguments == null) {
            argument = "<arguments == null>";
        } else if (arguments.length == 1) {
            argument = arguments[0];
        } else {
            argument = "<arguments of length " + arguments.length + ">";
        }
        return "get_media_file_properties(" + argument +")";
    }
}
