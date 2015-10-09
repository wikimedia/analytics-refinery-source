package org.wikimedia.analytics.refinery.camus.coders;

import com.linkedin.camus.coders.Message;

import java.io.IOException;

/**
 * Test class implementing Message with payload only
 */
public class TestMessage implements Message {

    private byte[] payload;

    public TestMessage(byte[] payload) { this.payload = payload; }
    public TestMessage(String payloadString) { this(payloadString.getBytes()); }

    @Override
    public byte[] getPayload() { return payload; }
    @Override
    public byte[] getKey() { return new byte[0]; }
    @Override
    public String getTopic() { return null; }
    @Override
    public long getOffset() { return 0; }
    @Override
    public int getPartition() { return 0; }
    @Override
    public long getChecksum() { return 0; }
    @Override
    public void validate() throws IOException { }

}
