package org.wikimedia.analytics.refinery.core;

import java.util.Arrays;

/**
 * Decoder for percent encoded strings
 * <p/>
 * In contrast to URLDecoder, this decoder keeps percent signs that are not
 * followed by hexadecimal digits, and does not convert plus-signs to spaces.
 */
public final class PercentDecoder {

    private PercentDecoder() {
        // utility class should never be instantiated
    }

    private static int getHexValue(byte b) {
        if ('0' <= b && b <= '9') {
            return b - 0x30;
        } else if ('A' <= b && b <= 'F') {
            return b - 0x37;
        } else if ('a' <= b && b <= 'f') {
            return b - 0x57;
        }
        return -1;
    }
    /**
     * Decodes percent encoded strings.
     *
     * @param encoded the percent encoded string
     * @return the decoded string
     */
    @SuppressWarnings({"checkstyle:InnerAssignment", "checkstyle:ModifiedControlVariable"})
    public static String decode(String encoded) {
        if (encoded == null) {
            return null;
        }

        byte[] encodedChars = encoded.getBytes();
        int encodedLength = encodedChars.length;
        byte[] decodedChars = new byte[encodedLength];

        int decodedIdx = 0;
        for (int encodedIdx = 0; encodedIdx < encodedLength;
                encodedIdx++, decodedIdx++) {
            if ((decodedChars[decodedIdx] = encodedChars[encodedIdx]) == '%') {
                // Current character is percent
                if (encodedIdx + 2 < encodedLength) {
                    int value1;
                    int value2;
                    if (
                            (value1 = getHexValue(encodedChars[encodedIdx + 1])) >= 0
                            && (value2 = getHexValue(encodedChars[encodedIdx + 2])) >= 0
                    ) {
                        decodedChars[decodedIdx] = (byte) ((value1 << 4) + value2);
                        encodedIdx += 2;
                    }
                }
            }
        }
        return new String(Arrays.copyOf(decodedChars, decodedIdx));
    }
}
