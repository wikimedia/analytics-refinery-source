package org.wikimedia.analytics.refinery.camus.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.Message;
import com.linkedin.camus.coders.MessageDecoder;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.wikimedia.analytics.refinery.camus.schemaregistry.KafkaTopicSchemaRegistry;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.imageio.stream.FileImageInputStream;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for avro json/binary decoders
 */
@RunWith(Parameterized.class)
public class TestAvroMessageDecoder {
    @Parameters
    public static Collection<Object[]> decoders() throws IOException {
        return Arrays.asList(
                new Object[]{AvroJsonMessageDecoder.class, new TestMessage(getAvroJsonPayload())},
                new Object[]{AvroBinaryMessageDecoder.class, new TestMessage(getAvroBinaryPayload())}
        );
    }

    private static byte[] getAvroBinaryPayload() throws IOException {
        Schema s = new Schema.Parser().parse(TestAvroSchemaEvolution.class.getResourceAsStream("/schema_repo/avro/mediawiki/TestSchema/0.avsc"));
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
        return baos.toByteArray();
    }

    private static String getAvroJsonPayload() {
        return "{\"id\":1,\"name\":\"test name\", \"someInfo\":{\"sub1\":\"val 1\",\"sub2\":\"val 2\"}}";
    }

    private Class<MessageDecoder<Message, GenericData.Record>> decoderClass;
    private MessageDecoder<Message, GenericData.Record> decoder;
    private TestMessage testMessage;

    public TestAvroMessageDecoder(Class<MessageDecoder<Message, GenericData.Record>> decoderClass, TestMessage testMessage) {
        this.decoderClass = decoderClass;
        this.testMessage = testMessage;
    }

    @Before
    public void initClass() throws InstantiationException, IllegalAccessException {
        this.decoder = decoderClass.newInstance();
    }

    @Test
    public void testInit() {
        Properties testProperties = new Properties();
        testProperties.setProperty("camus.message.timestamp.format", "unix_milliseconds");
        testProperties.setProperty(KafkaTopicSchemaRegistry.SCHEMA_REGISTRY_CLASS,
                "org.wikimedia.analytics.refinery.camus.schemaregistry.KafkaTopicSchemaRegistry");
        testProperties.setProperty(KafkaTopicSchemaRegistry.SCHEMA_NAMESPACE+".TestSchema.latestRev", "0");

        decoder.init(testProperties, "testprefix_TestSchema");
    }

    @Test(expected = RuntimeException.class)
    public void testInitFail() {
        Properties testProperties = new Properties();
        testProperties.setProperty("camus.message.timestamp.format", "unix_milliseconds");
        testProperties.setProperty(KafkaTopicSchemaRegistry.SCHEMA_REGISTRY_CLASS,
                "org.wikimedia.analytics.refinery.camus.schemaregistry.KafkaTopicSchemaRegistry");

        decoder.init(testProperties, "testprefix_wrongschemaname");
    }
    
    @Test(expected = RuntimeException.class)
    public void testInitFailNoLatestRev() {
        Properties testProperties = new Properties();
        testProperties.setProperty("camus.message.timestamp.format", "unix_milliseconds");
        testProperties.setProperty(KafkaTopicSchemaRegistry.SCHEMA_REGISTRY_CLASS,
                "org.wikimedia.analytics.refinery.camus.schemaregistry.KafkaTopicSchemaRegistry");

        decoder.init(testProperties, "testprefix_TestSchema");
    }


    @Test
    public void testDecode() {
        Properties testProperties = new Properties();
        testProperties.setProperty("camus.message.timestamp.format", "unix_milliseconds");
        testProperties.setProperty(KafkaTopicSchemaRegistry.SCHEMA_REGISTRY_CLASS,
                "org.wikimedia.analytics.refinery.camus.schemaregistry.KafkaTopicSchemaRegistry");
        testProperties.setProperty(KafkaTopicSchemaRegistry.SCHEMA_NAMESPACE+".TestSchema.latestRev", "0");

        decoder.init(testProperties, "testprefix_TestSchema");

        CamusWrapper<GenericData.Record> res = decoder.decode(testMessage);

        assertEquals(1L, (long)res.getRecord().get("id"));
        assertEquals("test name", res.getRecord().get("name").toString());
        Map<Utf8, Utf8> subres = (Map<Utf8, Utf8>) res.getRecord().get("someInfo");
        assertEquals("val 1", subres.get(new Utf8("sub1")).toString());
        assertEquals("val 2", subres.get(new Utf8("sub2")).toString());
    }

    @Test(expected = RuntimeException.class)
    public void testFailDecode() {
        Properties testProperties = new Properties();
        testProperties.setProperty("camus.message.timestamp.format", "unix_milliseconds");
        testProperties.setProperty(KafkaTopicSchemaRegistry.SCHEMA_REGISTRY_CLASS,
                "org.wikimedia.analytics.refinery.camus.schemaregistry.KafkaTopicSchemaRegistry");
        testProperties.setProperty(KafkaTopicSchemaRegistry.SCHEMA_NAMESPACE+".TestSchema.latestRev", "0");

        decoder.init(testProperties, "testprefix_TestSchema");

        decoder.decode(new TestMessage("{\"wrong\":\"field\"}"));

    }
}
