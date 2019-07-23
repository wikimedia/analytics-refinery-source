package org.wikimedia.analytics.refinery.core.media;

/**
 * Given a file extension what is the media "parent type" and its "canonical"
 * extension according to wikimedia's tradditional media type classification
 */
public enum MediaTypeClassification {


    SVG("svg", "image"),
    PNG("png", "image"),
    TIF("tiff", "image"), TIFF("tiff", "image"),
    JPEG("jpeg", "image"), JPG("jpeg", "image"),
    GIF("gif", "image"),
    XCF("xcf", "image"),
    WEBP("webp", "image"),
    BMP("bmp", "image"),

    MP3("mp3", "audio"),
    OGG("ogg", "audio"),
    OGA("oga", "audio"),
    FLAC("flac", "audio"),
    WAV("wav", "audio"),
    MID("midi", "audio"), MIDI("midi", "audio"),

    WEBM("webm", "video"),
    OGV("ogv", "video"),

    PDF("pdf", "document"),
    DJVU("djvu", "document"),
    SRT("srt", "document"),
    TXT("txt", "document"),
    JSON("json", "data"),
    OTHER(null, "other");

    private String parentType;

    // This field captures the fact
    // that JPG file type should be filed as JPEG, TIF as TIFF, MID as MIDI...
    private String canonicalExtensionForType;

    MediaTypeClassification(String canonicalExtensionForType, String parentType){
        this.canonicalExtensionForType = canonicalExtensionForType;
        this.parentType = parentType;
    }

    public String getParentType(){
        return this.parentType;
    }


    public String getCanonicalExtension(){
        return this.canonicalExtensionForType;
    }

}

