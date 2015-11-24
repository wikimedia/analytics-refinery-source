package org.wikimedia.analytics.refinery.camus.coders;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.type.TypeReference;

import com.google.common.base.Charsets;
import com.linkedin.camus.coders.Message;

/**
 * Test class implementing Message with payload only
 */
public class TestMessage implements Message {
    private final static int AVRO_JSON = 1;
    private final static int AVRO_BIN = 2;
    private byte[] payload;
    private final int type;
    private long revId;
    private final boolean includeSchemaInfo;

    public TestMessage(byte[] payload) {
        this(payload, 0);
    }

    public TestMessage(String payloadString) {
        this(payloadString, 0);
    }
    public TestMessage(byte[] payload, long revId) {
        this(payload, revId, true);
    }
    public TestMessage(String payloadString, long revId) {
        this(payloadString, revId, true);
    }
    public TestMessage(byte[] payload, long revId, boolean includeSchemaInfo) {
        this.payload = payload;
        this.revId = revId;
        type = AVRO_BIN;
        this.includeSchemaInfo = includeSchemaInfo;
    }

    public TestMessage(String payloadString, long revId, boolean includeSchemaInfo) {
        this.payload = payloadString.getBytes(Charsets.UTF_8);
        this.revId = revId;
        type = AVRO_JSON;
        this.includeSchemaInfo = includeSchemaInfo;
    }

    @Override
    public byte[] getPayload() {
        if(!includeSchemaInfo) {
            return payload;
        }
        if(type == AVRO_BIN) {
            return getAvroBin();
        } else {
            return getJsonBin();
        }
    }

    public byte[] getJsonBin() {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> node;
        try {
            node = mapper.readValue(payload, new TypeReference<HashMap<String, Object>>() {});
            node.put(AvroJsonMessageDecoder.DEFAULT_SCHEMA_ID_FIELD, revId);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectWriter writer = mapper.writer();
            writer.writeValue(baos, node);
            baos.close();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] getAvroBin() {
        try {
            byte[] data = new byte[payload.length+AvroBinaryMessageDecoder.MessageDecoderHelper.SCHEMA_SIZE];
            ByteArrayOutputStream baos = new ByteArrayOutputStream(AvroBinaryMessageDecoder.MessageDecoderHelper.SCHEMA_SIZE);
            DataOutputStream output = new DataOutputStream(baos);
            output.writeByte(AvroBinaryMessageDecoder.MessageDecoderHelper.MAGIC);
            output.writeLong(revId);
            output.close();
            System.arraycopy(baos.toByteArray(), 0, data, 0, AvroBinaryMessageDecoder.MessageDecoderHelper.SCHEMA_SIZE);
            System.arraycopy(payload, 0, data, AvroBinaryMessageDecoder.MessageDecoderHelper.SCHEMA_SIZE, payload.length);
            return data;
        } catch(IOException ioe) {
            throw new RuntimeException();
        }
    }
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
