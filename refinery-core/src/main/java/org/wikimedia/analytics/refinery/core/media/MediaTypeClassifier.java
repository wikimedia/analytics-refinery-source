package org.wikimedia.analytics.refinery.core.media;

/**
 * This class returns a MediaType
 * given a filename
 */
public class MediaTypeClassifier {

    public MediaTypeClassifier(){

    }

    public MediaType classify(String fileName) {
        if (fileName == null || fileName.trim().isEmpty()) {
            return null;
        }

        String extension = this.extractExtension(fileName);
        return new MediaType(extractExtension(fileName));
    }


    private String extractExtension (String fileName) {
        int extensionDotIndex = fileName.lastIndexOf('.');
        return fileName.substring(extensionDotIndex + 1);
    }

}
