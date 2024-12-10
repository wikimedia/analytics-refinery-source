package org.wikimedia.analytics.refinery.core;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

import lombok.Getter;

/**
 * An output stream that creates a sequence of byte array chunks.
 *
 * <p>In contrast to {@link java.io.ByteArrayOutputStream}, this implementation does not need to copy
 * existing data, resulting in lower memory usage.
 */
public class ChunkedByteArrayOutputStream extends OutputStream {
    private final int chunkSize;
    @Getter private final List<byte[]> chunks = new ArrayList<>();
    private final CRC32 checksum = new CRC32();
    private byte[] currentChunk;
    private int currentIndex = 0;

    public ChunkedByteArrayOutputStream(int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("Chunk size must be greater than 0");
        }
        this.chunkSize = chunkSize;
        this.currentChunk = new byte[chunkSize];
    }

    @Override
    public void write(int b) {
        if (currentIndex >= chunkSize) {
            flushCurrentChunk();
        }
        currentChunk[currentIndex++] = (byte) b;
        checksum.update(b);
    }

    @Override
    public void write(byte[] b, int off, int len) {
        checksum.update(b, off, len);
        int remaining = len;
        int offset = off;
        while (remaining > 0) {
            if (currentIndex >= chunkSize) {
                flushCurrentChunk();
            }
            int bytesToWrite = Math.min(remaining, chunkSize - currentIndex);
            System.arraycopy(b, offset, currentChunk, currentIndex, bytesToWrite);
            currentIndex += bytesToWrite;
            offset += bytesToWrite;
            remaining -= bytesToWrite;
        }
    }

    @Override
    public void flush() {
        flushCurrentChunk();
    }

    @Override
    public void close() {
        flush();
    }

    private void flushCurrentChunk() {
        if (currentIndex > 0) {
            byte[] chunk = new byte[currentIndex];
            System.arraycopy(currentChunk, 0, chunk, 0, currentIndex);
            chunks.add(chunk);
            currentChunk = new byte[chunkSize];
            currentIndex = 0;
        }
    }

    public long getChecksum() {
        return checksum.getValue();
    }
}
