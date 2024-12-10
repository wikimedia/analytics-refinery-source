package org.wikimedia.analytics.refinery.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.zip.CRC32;

import org.junit.Test;

public class ChunkedByteArrayOutputStreamTest {

    @Test
    public void testWriteByteByByte() throws IOException {
        ChunkedByteArrayOutputStream stream = new ChunkedByteArrayOutputStream(5);
        stream.write('H');
        stream.write('e');
        stream.write('l');
        stream.write('l');
        stream.write('o');
        stream.write('!'); // This should trigger a new chunk
        stream.close();

        List<byte[]> chunks = stream.getChunks();
        assertThat(chunks).hasSize(2).containsExactly("Hello".getBytes(), "!".getBytes());
    }

    @Test
    public void testWriteArray() throws IOException {
        ChunkedByteArrayOutputStream stream = new ChunkedByteArrayOutputStream(5);
        stream.write("Hello, World!".getBytes());
        stream.close();

        List<byte[]> chunks = stream.getChunks();
        assertThat(chunks)
                .hasSize(3)
                .containsExactly("Hello".getBytes(), ", Wor".getBytes(), "ld!".getBytes());
    }

    @Test
    public void testFlushBehavior() throws IOException {
        ChunkedByteArrayOutputStream stream = new ChunkedByteArrayOutputStream(5);
        stream.write("Hello".getBytes());
        stream.write(','); // At this point, "Hello" should trigger a flush
        stream.flush();
        stream.write(" World".getBytes());
        stream.close();

        List<byte[]> chunks = stream.getChunks();
        assertThat(chunks)
                .hasSize(4)
                .containsExactly("Hello".getBytes(), ",".getBytes(), " Worl".getBytes(), "d".getBytes());
    }

    @Test
    public void testCloseBehavior() throws IOException {
        ChunkedByteArrayOutputStream stream = new ChunkedByteArrayOutputStream(5);
        stream.write("Hi".getBytes());
        stream.close();

        List<byte[]> chunks = stream.getChunks();
        assertThat(chunks).hasSize(1).containsExactly("Hi".getBytes());
    }

    @Test
    public void testEmptyStream() throws IOException {
        ChunkedByteArrayOutputStream stream = new ChunkedByteArrayOutputStream(5);
        stream.close();

        List<byte[]> chunks = stream.getChunks();
        assertThat(chunks).isEmpty();
    }

    @Test
    public void testChecksum() throws IOException {
        ChunkedByteArrayOutputStream stream = new ChunkedByteArrayOutputStream(5);
        String data = "Hello, World!";
        stream.write(data.getBytes());
        stream.close();

        long originalChecksum = stream.getChecksum();

        // Reconstruct the data from chunks
        List<byte[]> chunks = stream.getChunks();
        CRC32 reconstructedChecksum = new CRC32();
        for (byte[] chunk : chunks) {
            reconstructedChecksum.update(chunk);
        }

        // Verify checksum
        assertThat(originalChecksum).isEqualTo(reconstructedChecksum.getValue());
    }
}
