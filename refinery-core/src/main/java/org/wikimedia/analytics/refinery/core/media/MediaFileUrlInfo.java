package org.wikimedia.analytics.refinery.core.media;

public class MediaFileUrlInfo {

    public enum TranscodingClassification {
        UNKNOWN,
        ORIGINAL,
        TRANSCODED_TO_AUDIO,
        TRANSCODED_TO_IMAGE,
        TRANSCODED_TO_MOVIE,
    }

    private String baseName;
    private TranscodingClassification transcodingClassification;
    private Integer width;
    private Integer height;
    private MediaTypeClassifier mediaTypeClassifier = new MediaTypeClassifier();

    private MediaType mediaType;

    public static MediaFileUrlInfo createUnknown() {
        return new MediaFileUrlInfo(null,
                TranscodingClassification.UNKNOWN,
                null,
                null);
    }

    public static MediaFileUrlInfo createOriginal(final String baseName) {
        return new MediaFileUrlInfo(baseName, TranscodingClassification.ORIGINAL, null, null);
    }

    public static MediaFileUrlInfo createTranscodedToImage(
            final String baseName, final Integer width) {
        return new MediaFileUrlInfo(baseName,
                TranscodingClassification.TRANSCODED_TO_IMAGE,
                width,
                null);
    }

    public static MediaFileUrlInfo createTranscodedToMovie(
            final String baseName, final int height) {
        return new MediaFileUrlInfo(baseName,
                TranscodingClassification.TRANSCODED_TO_MOVIE,
                null,
                height);
    }

    public static MediaFileUrlInfo createTranscodedToAudio(
            final String baseName) {
        return new MediaFileUrlInfo(baseName,
                TranscodingClassification.TRANSCODED_TO_AUDIO,
                null,
                null);
    }

    private MediaFileUrlInfo(final String baseName,
                             final TranscodingClassification quality,
                             final Integer width,
                             final Integer height) {
        this.baseName = baseName;
        this.transcodingClassification = quality;
        this.mediaType = mediaTypeClassifier.classify(baseName);
        this.width = width;
        this.height = height;
    }

    public String getBaseName() {
        return baseName;
    }

    public TranscodingClassification getTranscodingClassification() {
        return transcodingClassification;
    }

    public MediaType getMediaType() {
        return mediaType;
    }

    public Integer getWidth() {
        return width;
    }

    public Integer getHeight() {
        return height;
    }

    public String getTranscoding() {
        if (transcodingClassification == TranscodingClassification.TRANSCODED_TO_IMAGE) {
            if (width == null) return "image";
            else if (width <= 199) return "image_0_199";
            else if (width >= 200 && width <= 399 ) return "image_200_399";
            else if (width >= 400 && width <= 599 ) return "image_400_599";
            else if (width >= 600 && width <= 799 ) return "image_600_799";
            else if (width >= 800 && width <= 999 ) return "image_800_999";
            else if (width >= 1000) return "image_1000";
        } else if (transcodingClassification == TranscodingClassification.TRANSCODED_TO_MOVIE) {
            if (height == null) return "movie";
            else if (height <= 239) return "movie_0_239";
            else if (height >= 240 || height <= 479 ) return "movie_240_479";
            else if (height >= 480 ) return "movie_480";
        } else if (transcodingClassification == TranscodingClassification.ORIGINAL) {
            return "original";
        } else if (transcodingClassification == TranscodingClassification.TRANSCODED_TO_AUDIO) {
            return "audio";
        }
        return null;
    }

    @Override
    public boolean equals(final Object obj) {
        boolean ret = false;

        if (obj instanceof MediaFileUrlInfo) {
            MediaFileUrlInfo other =
                    (MediaFileUrlInfo) obj;

            ret = true;

            ret &= transcodingClassification == other.transcodingClassification;

            if (baseName == null) {
                ret &= other.baseName == null;
            } else {
                ret &= baseName.equals(other.baseName);
            }

            if (width == null) {
                ret &= other.width == null;
            } else {
                ret &= width.equals(other.width);
            }

            if (height == null) {
                ret &= other.height == null;
            } else {
                ret &= height.equals(other.height);
            }
        }

        return ret;
    }

    @Override
    public String toString() {
        String ret = "MediaFileUrlInfo[";
        switch (transcodingClassification) {
        case UNKNOWN:
            ret += "unknown";
            break;
        case ORIGINAL:
            ret += baseName;
            ret += ", original";
            break;
        case TRANSCODED_TO_AUDIO:
            ret += baseName;
            ret += ", transcoded to audio";
            break;
        case TRANSCODED_TO_IMAGE:
            ret += baseName;
            ret += ", transcoded to image, width: " + width;
            break;
        case TRANSCODED_TO_MOVIE:
            ret += baseName;
            ret += ", transcoded to movie, height: " + height;
            break;
        }
        ret += "]";
        return ret;
    }
}
