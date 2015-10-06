package org.wikimedia.analytics.refinery.camus.coders;

import com.linkedin.camus.coders.CamusWrapper;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.junit.Test;
import org.wikimedia.analytics.refinery.camus.schemaregistry.KafkaTopicSchemaRegistry;

import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for avro json decoder
 */
public class TestAvroJsonMessageDecoder {

    @Test
    public void testInit() {

        AvroJsonMessageDecoder decoder = new AvroJsonMessageDecoder();

        Properties testProperties = new Properties();
        testProperties.setProperty("camus.message.timestamp.format", "unix_milliseconds");
        testProperties.setProperty(KafkaTopicSchemaRegistry.SCHEMA_REGISTRY_CLASS,
                "org.wikimedia.analytics.refinery.camus.schemaregistry.KafkaTopicSchemaRegistry");

        decoder.init(testProperties, "testprefix_TestSchema");
    }

    @Test(expected = RuntimeException.class)
    public void testInitFail() {

        AvroJsonMessageDecoder decoder = new AvroJsonMessageDecoder();

        Properties testProperties = new Properties();
        testProperties.setProperty("camus.message.timestamp.format", "unix_milliseconds");
        testProperties.setProperty(KafkaTopicSchemaRegistry.SCHEMA_REGISTRY_CLASS,
                "org.wikimedia.analytics.refinery.camus.schemaregistry.KafkaTopicSchemaRegistry");

        decoder.init(testProperties, "testprefix_wrongschemaname");
    }

    @Test
    public void testDecode() {

        AvroJsonMessageDecoder decoder = new AvroJsonMessageDecoder();

        Properties testProperties = new Properties();
        testProperties.setProperty("camus.message.timestamp.format", "unix_milliseconds");
        testProperties.setProperty(KafkaTopicSchemaRegistry.SCHEMA_REGISTRY_CLASS,
                "org.wikimedia.analytics.refinery.camus.schemaregistry.KafkaTopicSchemaRegistry");

        decoder.init(testProperties, "testprefix_TestSchema");

        CamusWrapper<GenericData.Record> res = decoder.decode(new TestMessage("{\"id\":1,\"name\":\"test name\"," +
                "\"someInfo\":{\"sub1\":\"val 1\",\"sub2\":\"val 2\"}}"));

        assertEquals(1L, (long)res.getRecord().get("id"));
        assertEquals("test name", res.getRecord().get("name").toString());
        Map<Utf8, Utf8> subres = (Map<Utf8, Utf8>) res.getRecord().get("someInfo");
        assertEquals("val 1", subres.get(new Utf8("sub1")).toString());
        assertEquals("val 2", subres.get(new Utf8("sub2")).toString());
    }

    @Test(expected = RuntimeException.class)
    public void testFailDecode() {

        AvroJsonMessageDecoder decoder = new AvroJsonMessageDecoder();

        Properties testProperties = new Properties();
        testProperties.setProperty("camus.message.timestamp.format", "unix_milliseconds");
        testProperties.setProperty(KafkaTopicSchemaRegistry.SCHEMA_REGISTRY_CLASS,
                "org.wikimedia.analytics.refinery.camus.schemaregistry.KafkaTopicSchemaRegistry");

        decoder.init(testProperties, "testprefix_TestSchema");

        decoder.decode(new TestMessage("{\"wrong\":\"field\"}"));

    }

}
