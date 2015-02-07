package org.wikimedia.analytics.refinery.core;

import java.util.Arrays;

/**
 * Encoder for percent encoding
 * <p/>
 * In contrast to URLEncodeDecoder, this encoder does not convert space into a
 * plus-sign.
 */
public class PercentEncoder {

    /**
     * Converts least-significant nibble to character
     * @param i the value to extractthe nibble from
     * @return the ASCII value for the hexadecimal character representing the
     *     nibble's value
     */
    private static int nibbleToHex(int i) {
        i &= 0x0f;
        if (i < 10) {
            return '0' + i;
        }
        return 'A' + i - 10;
    }
    /**
     * Encodes to percent encoded string
     *
     * @param str the string to encode
     * @return the encoded string
     */
    public static String encode(String str) {
        if (str == null) {
            return null;
        }

        byte[] chars = str.getBytes();
        int length = chars.length;
        byte[] encodedChars = new byte[3*length];

        int encodedIdx = 0;
        for (int idx = 0; idx < length; idx++) {

            if (0x21 <= chars[idx] && chars[idx] <= 0x7e) {
                // Plain nice character. No need to encode
                encodedChars[encodedIdx++] = chars[idx];
            } else {
                // Encode
                encodedChars[encodedIdx++] = '%';
                encodedChars[encodedIdx++] = (byte) nibbleToHex(chars[idx] >> 4);
                encodedChars[encodedIdx++] = (byte) nibbleToHex(chars[idx]);
            }
        }
        return new String(Arrays.copyOf(encodedChars, encodedIdx));
    }
}
