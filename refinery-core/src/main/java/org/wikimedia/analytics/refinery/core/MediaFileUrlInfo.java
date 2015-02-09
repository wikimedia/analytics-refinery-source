package org.wikimedia.analytics.refinery.core;

public class MediaFileUrlInfo {

    public enum Classification {
        UNKNOWN,
        ORIGINAL,
        TRANSCODED_TO_AUDIO,
        TRANSCODED_TO_IMAGE,
        TRANSCODED_TO_MOVIE,
    }

    private String baseName;
    private Classification classification;
    private Integer width;
    private Integer height;

    public static MediaFileUrlInfo createUnknown() {
        return new MediaFileUrlInfo(null, Classification.UNKNOWN,
                null, null);
    }

    public static MediaFileUrlInfo createOriginal(final String baseName) {
        return new MediaFileUrlInfo(baseName, Classification.ORIGINAL,
                null, null);
    }

    public static MediaFileUrlInfo createTranscodedToImage(
            final String baseName, final Integer width) {
        return new MediaFileUrlInfo(baseName,
                Classification.TRANSCODED_TO_IMAGE, width, null);
    }

    public static MediaFileUrlInfo createTranscodedToMovie(
            final String baseName, final int height) {
        return new MediaFileUrlInfo(baseName,
                Classification.TRANSCODED_TO_MOVIE, null, height);
    }

    public static MediaFileUrlInfo createTranscodedToAudio(
            final String baseName) {
        return new MediaFileUrlInfo(baseName,
                Classification.TRANSCODED_TO_AUDIO, null, null);
    }

    private MediaFileUrlInfo(final String baseName,
            final Classification quality, final Integer width,
            final Integer height) {
        this.baseName = baseName;
        this.classification = quality;
        this.width = width;
        this.height = height;
    }

    public String getBaseName() {
        return baseName;
    }

    public Classification getClassification() {
        return classification;
    }

    public Integer getWidth() {
        return width;
    }

    public Integer getHeight() {
        return height;
    }

    @Override
    public boolean equals(final Object obj) {
        boolean ret = false;

        if (obj instanceof MediaFileUrlInfo) {
            MediaFileUrlInfo other =
                    (MediaFileUrlInfo) obj;

            ret = true;

            ret &= classification == other.classification;

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
        switch (classification) {
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
