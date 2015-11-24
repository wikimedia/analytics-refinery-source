package org.wikimedia.analytics.refinery.camus.coders;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.wikimedia.analytics.refinery.camus.schemaregistry.KafkaTopicSchemaRegistry;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.linkedin.camus.coders.Message;
import com.linkedin.camus.coders.MessageDecoder;

/**
 * Test schema evolution.
 * With TestSchema:
 * v0: id, name, someInfo
 * v1: added newField and newUnion
 * v2: removed newField
 */
@RunWith(Parameterized.class)
public class TestAvroSchemaEvolution {
    private final static int JSON = 1;
    private final static int BINARY = 2;

    private final int type;
    private final boolean includeSchema;
    private MessageDecoder<Message, GenericData.Record> decoder;

    @Parameters
    public static Collection<Object[]> params() throws IOException {
        return Arrays.asList(
                new Object[]{JSON, true}, // Json with writer schema in message
                new Object[]{JSON, false}, // Json with writer schema as a property
                new Object[]{BINARY, true}, // Binary with writer schema in message
                new Object[]{BINARY, false} // Binary with writer schema as a property
        );
    }

    public TestAvroSchemaEvolution(int type, boolean includeSchema) {
        this.type = type;
        this.includeSchema = includeSchema;
    }

    public Record receiveMessage(int fromVersion, int toVersion, boolean useWriterSchemaOnly) {
        switch (type) {
        case JSON:
            decoder = new AvroJsonMessageDecoder();
            break;

        default:
            decoder = new AvroBinaryMessageDecoder();
            break;
        }
        Properties testProperties = new Properties();
        testProperties.setProperty("camus.message.timestamp.format", "unix_milliseconds");
        testProperties.setProperty(KafkaTopicSchemaRegistry.SCHEMA_REGISTRY_CLASS,
                "org.wikimedia.analytics.refinery.camus.schemaregistry.KafkaTopicSchemaRegistry");

        if(fromVersion != toVersion || !useWriterSchemaOnly) {
            testProperties.setProperty("org.wikimedia.analytics.schemas.TestSchema.latestRev", String.valueOf(toVersion));
        }
        if(!includeSchema) {
            testProperties.setProperty("camus.message.schema.default", String.valueOf(fromVersion));
        }
        decoder.init(testProperties, "testprefix_TestSchema");
        return decoder.decode(getMessage(fromVersion)).getRecord();
    }

    public Record receiveMessage(int fromVersion, int toVersion) {
        return receiveMessage(fromVersion, toVersion, false);
    }

    @Test
    public void testV0_V0() throws IOException {
        Record record = receiveMessage(0,0);
        assertV0(record);
    }

    @Test
    public void testV0_V1() throws IOException {
        Record record = receiveMessage(0,1);
        assertV0ToV1(record);
    }

    @Test
    public void testV0_V2() throws IOException {
        Record record = receiveMessage(0,2);
        assertV0ToV2(record);
    }

    @Test
    public void testV1_V1() throws IOException {
        Record record = receiveMessage(1,1);
        assertV1(record);
    }

    @Test
    public void testV1_V2() throws IOException {
        Record record = receiveMessage(1,2);
        assertV1ToV2(record);
    }

    @Test
    public void testV2_V2() throws IOException {
        Record record = receiveMessage(2,2);
        assertV2(record);
    }

    @Test
    public void testWriterSchemaOnlyV0() throws IOException {
        Record record = receiveMessage(0,0,true);
        assertV0(record);
    }

    @Test
    public void testWriterSchemaOnlyV1() throws IOException {
        Record record = receiveMessage(1,1,true);
        assertV1(record);
    }

    @Test
    public void testWriterSchemaOnlyV2() throws IOException {
        Record record = receiveMessage(2,2,true);
        assertV2(record);
    }

    @Test(expected=RuntimeException.class)
    public void testV1_V0() throws IOException {
        Record record = receiveMessage(1,0);
        assertV1(record);
    }

    public void assertV0(Record record) {
        assertEquals(1L, (long)record.get("id"));
        assertEquals("test name", record.get("name").toString());
        Map<Utf8, Utf8> subres = (Map<Utf8, Utf8>) record.get("someInfo");
        assertEquals("val 1", subres.get(new Utf8("sub1")).toString());
        assertEquals("val 2", subres.get(new Utf8("sub2")).toString());
    }

    public void assertV0ToV1(Record record) {
        assertV0(record);
        Assert.assertNotNull(record.get("newField"));
        assertEquals("defaultValue", record.get("newField").toString());
        Assert.assertNull(null, record.get("newUnion"));
    }

    public void assertV0ToV2(Record record) {
        assertV0(record);
        Assert.assertNull(record.get("newField"));
        Assert.assertNull(record.get("newUnion"));
    }


    public void assertV1ToV2(Record record) {
        assertEquals(1L, (long)record.get("id"));
        assertEquals("test name", record.get("name").toString());
        Map<Utf8, Utf8> subres = (Map<Utf8, Utf8>) record.get("someInfo");
        assertEquals("val 1", subres.get(new Utf8("sub1")).toString());
        assertEquals("val 2", subres.get(new Utf8("sub2")).toString());
        assertEquals("my new union", record.get("newUnion").toString());
        // This one is removed from v2
        Assert.assertNull(null, record.get("newField"));
    }

    public void assertV1(Record record) {
        assertEquals(1L, (long)record.get("id"));
        assertEquals("test name", record.get("name").toString());
        Map<Utf8, Utf8> subres = (Map<Utf8, Utf8>) record.get("someInfo");
        assertEquals("val 1", subres.get(new Utf8("sub1")).toString());
        assertEquals("val 2", subres.get(new Utf8("sub2")).toString());
        assertEquals("my new field", record.get("newField").toString());
        assertEquals("my new union", record.get("newUnion").toString());
    }

    public void assertV2(Record record) {
        assertEquals(1L, (long)record.get("id"));
        assertEquals("test name", record.get("name").toString());
        Map<Utf8, Utf8> subres = (Map<Utf8, Utf8>) record.get("someInfo");
        assertEquals("val 1", subres.get(new Utf8("sub1")).toString());
        assertEquals("val 2", subres.get(new Utf8("sub2")).toString());
        assertEquals("my new union", record.get("newUnion").toString());
    }

    private TestMessage getMessage(int version) {
        try {
            switch(type) {
            case JSON: {
                switch (version) {
                case 0:
                    return getAvroJsonPayloadV0();
                case 1:
                    return getAvroJsonPayloadV1();
                case 2:
                    return getAvroJsonPayloadV2();
                default:
                    return null;
                }
            }
            case BINARY: {
                switch (version) {
                case 0:
                    return getAvroBinaryPayloadV0();
                case 1:
                    return getAvroBinaryPayloadV1();
                case 2:
                    return getAvroBinaryPayloadV2();
                default:
                    return null;
                }
            }
            default: return null;
            }
        } catch(IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
    private TestMessage getAvroBinaryPayloadV0() throws IOException {
        Schema s = new Schema.Parser().parse(TestAvroSchemaEvolution.class.getResourceAsStream("/avro_schema_repo/TestSchema/0.avsc"));
        GenericRecordBuilder builder = new GenericRecordBuilder(s);
        builder.set("id", 1l);
        builder.set("name", "test name");
        HashMap<String, String> map = new HashMap<String, String>();
        map.put("sub1", "val 1");
        map.put("sub2", "val 2");
        builder.set("someInfo", map);

        Record record = builder.build();
        DatumWriter<Record> writer = new GenericDatumWriter<Record>(s);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Encoder enc = EncoderFactory.get().binaryEncoder(baos, null);
        writer.write(record, enc);
        enc.flush();
        baos.close();
        return new TestMessage(baos.toByteArray(), 0, includeSchema);
    }

    private TestMessage getAvroBinaryPayloadV1() throws IOException {
        Schema s = new Schema.Parser().parse(TestAvroSchemaEvolution.class.getResourceAsStream("/avro_schema_repo/TestSchema/1.avsc"));
        GenericRecordBuilder builder = new GenericRecordBuilder(s);
        builder.set("id", 1l);
        builder.set("name", "test name");
        HashMap<String, String> map = new HashMap<String, String>();
        map.put("sub1", "val 1");
        map.put("sub2", "val 2");
        builder.set("someInfo", map);
        builder.set("newField", "my new field");
        builder.set("newUnion", "my new union");

        Record record = builder.build();
        DatumWriter<Record> writer = new GenericDatumWriter<Record>(s);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Encoder enc = EncoderFactory.get().binaryEncoder(baos, null);
        writer.write(record, enc);
        enc.flush();
        baos.close();
        return new TestMessage(baos.toByteArray(), 1, includeSchema);
    }

    private TestMessage getAvroBinaryPayloadV2() throws IOException {
        Schema s = new Schema.Parser().parse(TestAvroSchemaEvolution.class.getResourceAsStream("/avro_schema_repo/TestSchema/2.avsc"));
        GenericRecordBuilder builder = new GenericRecordBuilder(s);
        builder.set("id", 1l);
        builder.set("name", "test name");
        HashMap<String, String> map = new HashMap<String, String>();
        map.put("sub1", "val 1");
        map.put("sub2", "val 2");
        builder.set("someInfo", map);
        builder.set("newUnion", "my new union");

        Record record = builder.build();
        DatumWriter<Record> writer = new GenericDatumWriter<Record>(s);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Encoder enc = EncoderFactory.get().binaryEncoder(baos, null);
        writer.write(record, enc);
        enc.flush();
        baos.close();
        return new TestMessage(baos.toByteArray(), 2, includeSchema);
    }


    private TestMessage getAvroJsonPayloadV0() {
        JsonObject avroMessage = new JsonObject();
        avroMessage.addProperty("id", 1l);
        avroMessage.addProperty("name", "test name");
        JsonObject someInfo = new JsonObject();
        someInfo.addProperty("sub1", "val 1");
        someInfo.addProperty("sub2", "val 2");
        avroMessage.add("someInfo", someInfo);
        return new TestMessage(new GsonBuilder().create().toJson(avroMessage), 0, includeSchema);
    }

    private TestMessage getAvroJsonPayloadV1() {
        JsonObject avroMessage = new JsonObject();
        avroMessage.addProperty("id", 1l);
        avroMessage.addProperty("name", "test name");
        JsonObject someInfo = new JsonObject();
        someInfo.addProperty("sub1", "val 1");
        someInfo.addProperty("sub2", "val 2");
        avroMessage.add("someInfo", someInfo);

        avroMessage.addProperty("newField", "my new field");
        JsonObject union = new JsonObject();
        union.addProperty("string", "my new union");
        avroMessage.add("newUnion", union);
        return new TestMessage(new GsonBuilder().create().toJson(avroMessage), 1, includeSchema);
    }

    private TestMessage getAvroJsonPayloadV2() {
        JsonObject avroMessage = new JsonObject();
        avroMessage.addProperty("id", 1l);
        avroMessage.addProperty("name", "test name");
        JsonObject someInfo = new JsonObject();
        someInfo.addProperty("sub1", "val 1");
        someInfo.addProperty("sub2", "val 2");
        avroMessage.add("someInfo", someInfo);

        JsonObject union = new JsonObject();
        union.addProperty("string", "my new union");
        avroMessage.add("newUnion", union);
        return new TestMessage(new GsonBuilder().create().toJson(avroMessage), 2, includeSchema);
    }
}
