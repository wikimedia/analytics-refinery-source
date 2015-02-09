package org.wikimedia.analytics.refinery.core;

import org.wikimedia.analytics.refinery.core.MediaFileUrlInfo.Classification;

import junit.framework.TestCase;

public class TestMediaFileUrlInfo extends TestCase {

    public void assertEquals(String message, int expected, Integer actual) {
        assertEquals(message, new Integer(expected), actual);
    }

    public void assertContainsIgnoreCase(String hayStack, String needle) {
        assertTrue("'" + hayStack + "' does not contain '" + needle + "'",
                hayStack.toLowerCase().contains(needle.toLowerCase()));
    }

    // Factory methods --------------------------------------------------------

    public void testCreateUnknown() {
        MediaFileUrlInfo info = MediaFileUrlInfo.createUnknown();

        assertNull("Base name not null", info.getBaseName());
        assertEquals("Classification does not match",
                Classification.UNKNOWN, info.getClassification());
        assertNull("Width not null", info.getWidth());
        assertNull("Height not null", info.getHeight());
    }

    public void testCreateOriginal() {
        MediaFileUrlInfo info = MediaFileUrlInfo.createOriginal("foo");

        assertEquals("Base name does not match", "foo", info.getBaseName());
        assertEquals("Classification does not match",
                Classification.ORIGINAL, info.getClassification());
        assertNull("Width not null for unknown", info.getWidth());
        assertNull("Height not null for unknown", info.getHeight());
    }

    public void testCreateTranscodedToAudio() {
        MediaFileUrlInfo info = MediaFileUrlInfo.createTranscodedToAudio("foo");

        assertEquals("Base name does not match", "foo", info.getBaseName());
        assertEquals("Classification does not match",
                Classification.TRANSCODED_TO_AUDIO, info.getClassification());
        assertNull("Width not null for unknown", info.getWidth());
        assertNull("Height not null for unknown", info.getHeight());
    }

    public void testCreateTranscodedToImage() {
        MediaFileUrlInfo info = MediaFileUrlInfo.createTranscodedToImage("foo", 42);

        assertEquals("Base name does not match", "foo", info.getBaseName());
        assertEquals("Classification does not match",
                Classification.TRANSCODED_TO_IMAGE, info.getClassification());
        assertEquals("Width does not match", 42, info.getWidth());
        assertNull("Height not null", info.getHeight());
    }

    public void testCreateTranscodedToMovie() {
        MediaFileUrlInfo info = MediaFileUrlInfo.createTranscodedToMovie("foo", 42);

        assertEquals("Base name does not match", "foo", info.getBaseName());
        assertEquals("Classification does not match",
                Classification.TRANSCODED_TO_MOVIE, info.getClassification());
        assertNull("Width not null", info.getWidth());
        assertEquals("Height does not match", 42, info.getHeight());
    }

    // Instance methods -------------------------------------------------------

    // Equals .................................................................

    public void testUnknowEqualsNull() {
        MediaFileUrlInfo info = MediaFileUrlInfo.createUnknown();

        boolean actual = info.equals(null);

        assertFalse("Proper instance equals null", actual);
    }

    public void testUnknownEqualsSame() {
        MediaFileUrlInfo info = MediaFileUrlInfo.createUnknown();

        boolean actual = info.equals(info);

        assertTrue("Proper instance does not equal itself", actual);
    }

    public void testUnknownEqualsUnknown() {
        MediaFileUrlInfo infoA = MediaFileUrlInfo.createUnknown();
        MediaFileUrlInfo infoB = MediaFileUrlInfo.createUnknown();

        boolean actual = infoA.equals(infoB);

        assertTrue("Two unknown not equal", actual);
    }

    public void testOriginalEqualsUnknown() {
        MediaFileUrlInfo infoA = MediaFileUrlInfo.createOriginal("foo");
        MediaFileUrlInfo infoB = MediaFileUrlInfo.createUnknown();

        boolean actual = infoA.equals(infoB);

        assertFalse("Original and unknown equal", actual);
    }

    public void testOriginalEqualsOriginal() {
        MediaFileUrlInfo infoA = MediaFileUrlInfo.createOriginal("foo");

        MediaFileUrlInfo infoB = MediaFileUrlInfo.createOriginal("foo");

        boolean actual = infoA.equals(infoB);

        assertTrue("Originals not equal", actual);
    }

    public void testOriginalEqualsOriginalDifferentBaseName() {
        MediaFileUrlInfo infoA = MediaFileUrlInfo.createOriginal("foo");

        MediaFileUrlInfo infoB = MediaFileUrlInfo.createOriginal("bar");

        boolean actual = infoA.equals(infoB);

        assertFalse("Base name does not disambiguate", actual);
    }

    public void testOriginalEqualsOriginalBaseNameOtherNull() {
        MediaFileUrlInfo infoA = MediaFileUrlInfo.createOriginal("foo");

        MediaFileUrlInfo infoB = MediaFileUrlInfo.createOriginal(null);

        boolean actual = infoA.equals(infoB);

        assertFalse("BaseName does not disambiguate", actual);
    }

    public void testOriginalEqualsOriginalBaseNameThisNull() {
        MediaFileUrlInfo infoA = MediaFileUrlInfo.createOriginal(null);

        MediaFileUrlInfo infoB = MediaFileUrlInfo.createOriginal("foo");

        boolean actual = infoA.equals(infoB);

        assertFalse("BaseName does not disambiguate", actual);
    }

    public void testAudioEqualsAudio() {
        MediaFileUrlInfo infoA =
                MediaFileUrlInfo.createTranscodedToAudio("foo");
        MediaFileUrlInfo infoB =
                MediaFileUrlInfo.createTranscodedToAudio("foo");

        boolean actual = infoA.equals(infoB);

        assertTrue("Audio files to not equal", actual);
    }

    public void testAudioEqualsAudioDifferentBaseName() {
        MediaFileUrlInfo infoA =
                MediaFileUrlInfo.createTranscodedToAudio("foo");
        MediaFileUrlInfo infoB =
                MediaFileUrlInfo.createTranscodedToAudio("bar");

        boolean actual = infoA.equals(infoB);

        assertFalse("Base name does not disambiguate", actual);
    }

    public void testAudioEqualsOriginal() {
        MediaFileUrlInfo infoA =
                MediaFileUrlInfo.createTranscodedToAudio("foo");
        MediaFileUrlInfo infoB = MediaFileUrlInfo.createOriginal("foo");

        boolean actual = infoA.equals(infoB);

        assertFalse("Audio and original equal", actual);
    }

    public void testImageEqualsImage() {
        MediaFileUrlInfo infoA =
                MediaFileUrlInfo.createTranscodedToImage("foo", 42);
        MediaFileUrlInfo infoB =
                MediaFileUrlInfo.createTranscodedToImage("foo", 42);

        boolean actual = infoA.equals(infoB);

        assertTrue("Images not equal", actual);
    }

    public void testImageEqualsAudio() {
        MediaFileUrlInfo infoA =
                MediaFileUrlInfo.createTranscodedToImage("foo", 42);
        MediaFileUrlInfo infoB =
                MediaFileUrlInfo.createTranscodedToAudio("foo");

        boolean actual = infoA.equals(infoB);

        assertFalse("Image and audio equal", actual);
    }

    public void testImageEqualsImageDifferentBaseName() {
        MediaFileUrlInfo infoA =
                MediaFileUrlInfo.createTranscodedToImage("foo", 42);
        MediaFileUrlInfo infoB =
                MediaFileUrlInfo.createTranscodedToImage("bar", 42);

        boolean actual = infoA.equals(infoB);

        assertFalse("Base name does not disambiguate", actual);
    }

    public void testImageEqualsImageDifferentWidth() {
        MediaFileUrlInfo infoA =
                MediaFileUrlInfo.createTranscodedToImage("foo", 42);
        MediaFileUrlInfo infoB =
                MediaFileUrlInfo.createTranscodedToImage("foo", 43);

        boolean actual = infoA.equals(infoB);

        assertFalse("Width does not disambiguate", actual);
    }

    public void testMovieEqualsImage() {
        MediaFileUrlInfo infoA =
                MediaFileUrlInfo.createTranscodedToMovie("foo", 42);
        MediaFileUrlInfo infoB =
                MediaFileUrlInfo.createTranscodedToImage("foo", 42);

        boolean actual = infoA.equals(infoB);

        assertFalse("Movie and image equal", actual);
    }

    public void testMovieEqualsMovie() {
        MediaFileUrlInfo infoA =
                MediaFileUrlInfo.createTranscodedToMovie("foo", 42);
        MediaFileUrlInfo infoB =
                MediaFileUrlInfo.createTranscodedToMovie("foo", 42);

        boolean actual = infoA.equals(infoB);

        assertTrue("Movies not equal", actual);
    }

    public void testMovieEqualsMovieDifferentBaseName() {
        MediaFileUrlInfo infoA =
                MediaFileUrlInfo.createTranscodedToMovie("foo", 42);
        MediaFileUrlInfo infoB =
                MediaFileUrlInfo.createTranscodedToMovie("bar", 42);

        boolean actual = infoA.equals(infoB);

        assertFalse("Base name does not disambiguate", actual);
    }

    public void testMovieEqualsMovieDifferentHeight() {
        MediaFileUrlInfo infoA =
                MediaFileUrlInfo.createTranscodedToMovie("foo", 42);
        MediaFileUrlInfo infoB =
                MediaFileUrlInfo.createTranscodedToMovie("foo", 43);

        boolean actual = infoA.equals(infoB);

        assertFalse("Height does not disambiguate", actual);
    }

    public void testUnknownEqualsMovie() {
        MediaFileUrlInfo infoA = MediaFileUrlInfo.createUnknown();
        MediaFileUrlInfo infoB =
                MediaFileUrlInfo.createTranscodedToMovie("foo", 42);

        boolean actual = infoA.equals(infoB);

        assertFalse("Unknown and movie equal", actual);
    }

    // toString ...............................................................

    public void testUnknown() {
        String stringRep = MediaFileUrlInfo.createUnknown().toString();

        assertContainsIgnoreCase(stringRep, "unknown");
    }

    public void testOriginal() {
        String stringRep = MediaFileUrlInfo.createOriginal("foo").toString();

        assertContainsIgnoreCase(stringRep, "original");
        assertContainsIgnoreCase(stringRep, "foo");
    }

    public void testAudio() {
        String stringRep = MediaFileUrlInfo.createTranscodedToAudio("foo").toString();

        assertContainsIgnoreCase(stringRep, "audio");
        assertContainsIgnoreCase(stringRep, "foo");
    }

    public void testImageWidthNonNull() {
        String stringRep = MediaFileUrlInfo.createTranscodedToImage("foo", 42).toString();

        assertContainsIgnoreCase(stringRep, "image");
        assertContainsIgnoreCase(stringRep, "foo");
        assertContainsIgnoreCase(stringRep, "42");
    }

    public void testImageWidthNull() {
        String stringRep = MediaFileUrlInfo.createTranscodedToImage("foo", null).toString();

        assertContainsIgnoreCase(stringRep, "image");
        assertContainsIgnoreCase(stringRep, "foo");
        assertContainsIgnoreCase(stringRep, "null");
    }

    public void testMovie() {
        String stringRep = MediaFileUrlInfo.createTranscodedToMovie("foo", 42).toString();

        assertContainsIgnoreCase(stringRep, "movie");
        assertContainsIgnoreCase(stringRep, "foo");
        assertContainsIgnoreCase(stringRep, "42");
    }
}
