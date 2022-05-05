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

package org.wikimedia.analytics.refinery.core.media;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.wikimedia.analytics.refinery.core.PercentDecoder;

/**
 * Parse an url for MediaFileUrlInfo
 */
public class MediaFileUrlParser {
    /**
     * If a URL is longer than this, the parser will return null
     */
    private static final int MAX_URL_LENGTH = 1024;

    /**
     * Pattern to match wikis within other patterns
     */
    private static Pattern wikiPattern = Pattern.compile("[a-z_-]{2,}[0-9]*");

    /**
     * Pattern to match math urls
     */
    private static Pattern mathPattern = Pattern.compile(
            "/math"
            + "/([0-9-a-f])"
            + "/([0-9-a-f])"
            + "/([0-9-a-f])"
            + "/\\1\\2\\3[0-9-a-f]{29}\\.png");

    private static Pattern mathPerWikiPattern = Pattern.compile(
            "/[^/]*/" + wikiPattern.pattern() + mathPattern.pattern());

    /**
     * Pattern to match score urls
     */
    private static Pattern scorePattern = Pattern.compile(
            "(/score"
            + "/([0-9a-z])"
            + "/([0-9a-z])"
            + "/(\\2\\3[0-9a-z]{6})[0-9a-z]{23}/\\4\\.)((png)|(ogg|midi))");

    /**
     * Pattern to match timeline urls
     */
    private static Pattern timelinePattern = Pattern.compile(
            "/[^/]*/" + wikiPattern.pattern() + "/timeline/[0-9-a-f]{32}\\.png");

    /**
     * Pattern to match urls for plain uploaded media files
     */
    private static Pattern uploadedPattern = Pattern.compile(
            "(/[^/]*/" + wikiPattern.pattern() + ")"  // group 1: project
            + "(?:/(thumb|transcoded))?"        // group 2: Markers for transcodings
            + "(/archive|/temp)?"            // group 3: Needed to construct basename
            + "(/([0-9a-f])/\\5[0-9a-f])"    // groups 4+5: Hash. Needed for backref, and to construct basename
            + "/(?:([12][0-9]{3}[01][0-9][0-3][0-9][0-2][0-9][0-5][0-9][0-6][0-9])(?:!|%21))?" // group 6: timestamp
            + "(([^/]*?)(?:\\.[^./]*)?)"     // group 7: the main file name
                                             // group 8: them main file name without suffix (such as ".png")
            + "(/"                           // group 9: the whole transcoding spec
                + "(?:lossy-)?"              // If transcoding is marked lossy (Like a single page of a tiff -> jpeg)
                + "(?:lossless-)?"           // If transcoding is marked lossless (Like a single page of a tiff -> png)
                + "(?:page[0-9]+-)?"         // For single page transcodings of a multi-page original (Like tiff -> png, pdf-> png)
                + "(?:lang[a-z-]*-)?"        // Rendering only a single language of a multi-language original (Like svg -> png)
                + "(?:(?:qlow|mid)-)?"       // Quality markers with undefined width/height
                + "(?:0*([1-9]+[0-9]*)px-)?" // group 10: Thumbnail pixel width (like 120px)
                + "(?:seek(?:=|%3D)[0-9]+(?:\\.[0-9]*)?)?" // Seeking to a timestamp in a video (When transcoding movies to images)
                + "-?"                       // This is the "-" that separates the prepended options from the thumbnail name.
                + "(?:\\7|\\8|thumbnail(?:\\.(?:djvu|ogv|pdf|svg|tiff?))?)" // main thumbnail name
                + "(?:\\.0*([1-9][0-9]*)p)?" // group 11: Transcoding height
                + "(?:\\.(?:"                // Ending of transcoded output format for:
                    + "(ogg)"                //   group 12: audio files
                    + "|(gif|jpe?g|png)"     //   group 13: images
                    + "|(webm|ogv)"          //   group 14: movies
                + "))?"
            + ")?");

    /**
     * Parses a string of digits to a bounded Integer, possibly null
     * <p/>
     * If the string of digits it too to fit in an Integer, the maximum possible
     * Integer is returned.
     *
     * @param digits The string of digits to parse integer
     * @return Integer null, if str is null. Otherwise a Integer in
     *   [0, Integer.MAX_VALUE]
     */
    private static Integer parseDigitString(String digits) {
        Integer ret = null;
        if (digits != null) {
            try {
                ret = Integer.parseInt(digits);
            } catch (NumberFormatException e) {
                // Since digits is required to be a string of digits, the only way a NumberFormatException can be thrown is that the number is too big. Hence, we bound the maximum possible integer.
                ret = Integer.MAX_VALUE;
            }
        }
        return ret;
    }

    /**
     * Parses information out of a url for media files in the upload domain
     *
     * @param url The url to parse
     * @return IdentifyMediaFileUrlInfo holding the parsed data.
     *   null if parsing failed.
     */
    public static MediaFileUrlInfo parse(String url) {
        final MediaFileUrlInfo ret;

        if (url == null || url.length() > MAX_URL_LENGTH) {
            return null;
        }

        String uriPath;

        if (url.startsWith("http://upload.wikimedia.org/")) {
            uriPath = url.substring(27);
        } else if (url.startsWith("https://upload.wikimedia.org/")) {
            uriPath = url.substring(28);
        } else if (url.startsWith("/")) {
            uriPath = url;
        } else {
            return null;
        }

        // url was either protocol- and domain-less, or it is valid for upload.
        assert uriPath.startsWith("/") : "uriPath does not start in \"/\", but is " + uriPath;

        uriPath = PercentDecoder.decode(uriPath);
        uriPath = uriPath.replaceAll("//+", "/");
        uriPath = uriPath.trim();

        String[] uriPathParts = StringUtils.split(uriPath, '/');

        assert uriPathParts != null : "Split gave null array";

        if (uriPathParts.length < 1) {
            return null;
        }

        switch (uriPathParts[0]) {
        case "math":
            if (mathPattern.matcher(uriPath).matches()){
                ret = MediaFileUrlInfo.createOriginal(uriPath);
            } else {
                return null;
            };
            break;
        case "score":
            Matcher matcher = scorePattern.matcher(uriPath);
            if (matcher.matches()) {
                String baseName = matcher.group(1) + "png";
                if (matcher.group(6) != null) {
                    ret = MediaFileUrlInfo.createTranscodedToImage(
                            baseName, null);
                } else if (matcher.group(7) != null) {
                    ret = MediaFileUrlInfo.createTranscodedToAudio(baseName);
                } else {
                    throw new AssertionError("Logic error due to score having "
                            + "both group 6 and 7 empty  ('" + uriPath + "')");
                }
            } else {
                return null;
            };
            break;
        case "wikibooks":
        case "wikinews":
        case "wikimedia":
        case "wikipedia":
        case "wikiquote":
        case "wikisource":
        case "wikiversity":
        case "wikivoyage":
        case "wiktionary":
            Matcher imageMatcher = uploadedPattern.matcher(uriPath);
            if (imageMatcher.matches()) {
                String project = imageMatcher.group(1);
                String transcoding = imageMatcher.group(2);
                String  timestampFlag = imageMatcher.group(3);
                String hash = imageMatcher.group(4);
                // No group 5, as that group holds the first hexadecimal digit.
                String timestamp = imageMatcher.group(6);
                String file = imageMatcher.group(7);
                // No group 8, as that group holds the file without suffix.
                String transcodingSpec = imageMatcher.group(9);
                String widthStr = imageMatcher.group(10);
                String heightStr = imageMatcher.group(11);
                String transcodedAudioSuffix = imageMatcher.group(12);
                String transcodedImageSuffix = imageMatcher.group(13);
                String transcodedMovieSuffix = imageMatcher.group(14);

                // Setting basename
                final String baseName;
                if (timestampFlag == null) {
                    baseName = project + hash + '/' + file;
                } else if ("/archive".equals(timestampFlag)) {
                    baseName = project + timestampFlag + hash + '/'
                            + timestamp + '!' + file;
                } else if ("/temp".equals(timestampFlag)) {
                    // Note that the timestamp is matched within the file, so
                    // no need to add the timestamp here.
                    baseName = project + timestampFlag + hash + '/' + file;
                } else {
                    throw new AssertionError("Logic error due to timestampFlag"
                            + " '" + timestampFlag + "' not being handled");
                }

                if (transcoding == null && transcodingSpec == null) {
                    ret = MediaFileUrlInfo.createOriginal(baseName);
                } else if (transcoding != null && transcodingSpec != null) {
                    if ("thumb".equals(transcoding)
                            || (transcodedImageSuffix != null)) {
                        Integer width = parseDigitString(widthStr);
                        ret = MediaFileUrlInfo.createTranscodedToImage(baseName, width);
                    } else if (transcodedAudioSuffix != null) {
                        ret = MediaFileUrlInfo.createTranscodedToAudio(baseName);
                    } else if (transcodedMovieSuffix != null) {
                        Integer height = parseDigitString(heightStr);
                        ret = MediaFileUrlInfo.createTranscodedToMovie(baseName, height);
                    } else {
                        // We sometimes see urls as
                        //   /wikipedia/commons/transcoded/b/bf/foo.ogv/foo.ogv
                        // which would match this branch. But such requests do
                        // not make much sense. Instead of failing hard for
                        // them, we just signal that we could not make sense
                        // of them.
                        return null;
                    }
                } else {
                    return null;
                }
            } else if (timelinePattern.matcher(uriPath).matches()) {
                ret = MediaFileUrlInfo.createOriginal(uriPath);
            } else if (mathPerWikiPattern.matcher(uriPath).matches()) {
                ret = MediaFileUrlInfo.createOriginal(uriPath);
            } else {
                return null;
            };
            break;
        case "favicon.ico":
            if (("/" + uriPathParts[0]).equals(uriPath)) {
                ret = MediaFileUrlInfo.createOriginal(uriPath);
            } else {
                return null;
            }
            break;
        default:
            return null;
        }

        assert (ret != null) : "Logic error, as info is still not set";

        return ret;
    }
}
