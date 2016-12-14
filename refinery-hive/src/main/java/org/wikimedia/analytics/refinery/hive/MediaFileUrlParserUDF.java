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
import org.apache.hadoop.hive.ql.udf.UDFType;

/**
 * Deprecated - Use GetMediaFilePropertiesUDF
 * Hive UDF to extract information out of upload.wikimedia.org urls
 * <p>
 * The UDF will return a map with the following keys:
 * <ul>
 * <li>{@code base_name} String. base_name of the file. (Without thumbs, transcodings, etc.)</li>
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
@Deprecated
@UDFType(deterministic = true)
@Description(name = "parse_media_file_url",
    value = "_FUNC_(url) - Returns a map of details to a media file url",
    extended = "argument 0 is the url to analyze")
public class MediaFileUrlParserUDF extends GetMediaFilePropertiesUDF {
}
