package org.wikimedia.analytics.refinery.camus.coders;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.wikimedia.analytics.refinery.camus.schemaregistry.KafkaTopicSchemaRegistry;
import org.wikimedia.analytics.schemas.TestTimestampSchema;

import scala.util.Random;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.Message;
import com.linkedin.camus.coders.MessageDecoder;

/**
 * Test timestamps behavior when setting :
 * camus.message.timestamp.field
 * camus.message.timestamp.format
 */
@RunWith(Parameterized.class)
public class TestAvroMessageTimestamps {
    private static Random RAND = new Random();
    private static Date NOW = new Date();

    // Timestamps in messages are at least 10 days older than now but not older than 100 days.
    // We use different timestamps for each field so the test can assert that we do not
    // fetch a timestamp from another field. This allows to use the same schema for each test.
    private static Date TIMESTAMP_MILLIS = new Date(NOW.getTime() - TimeUnit.DAYS.toMillis(10 + RAND.nextInt(100)));
    private static Date TIMESTAMP_SECS = new Date(NOW.getTime() - TimeUnit.DAYS.toMillis(10 + RAND.nextInt(100)));
    private static Date TIMESTAMP_SECS_INT = new Date(NOW.getTime() - TimeUnit.DAYS.toMillis(10 + RAND.nextInt(100)));
    private static Date TIMESTAMP_ISO = new Date(NOW.getTime() - TimeUnit.DAYS.toMillis(10 + RAND.nextInt(100)));
    private static Date TIMESTAMP_SDF = new Date(NOW.getTime() - TimeUnit.DAYS.toMillis(10 + RAND.nextInt(100)));

    private static String CUSTOM_SDF_FORMAT = "dd/MM/yyyy HH:mm:ss";
    private static SimpleDateFormat customFormat = new SimpleDateFormat(CUSTOM_SDF_FORMAT);

    private Class<MessageDecoder<Message, GenericData.Record>> decoderClass;
    private MessageDecoder<Message, GenericData.Record> decoder;
    private TestMessage testMessage;
    private Properties properties = new Properties();
    private AssertTimestamp assertion;
    private String testName;

    @Parameters
    public static Collection<Object[]> decoders() throws IOException {
        List<Object[]> tests = new ArrayList<Object[]>();

        // Test defaults, we should use timestamp as millis
        tests.add(new Object[]{null, null, AssertTimestamp.EXACT(TIMESTAMP_MILLIS.getTime())});

        // Test with explicit defaults
        tests.add(new Object[]{"timestamp", null, AssertTimestamp.EXACT(TIMESTAMP_MILLIS.getTime())});
        tests.add(new Object[]{null, "unix_milliseconds", AssertTimestamp.EXACT(TIMESTAMP_MILLIS.getTime())});
        tests.add(new Object[]{"timestamp", "unix_milliseconds", AssertTimestamp.EXACT(TIMESTAMP_MILLIS.getTime())});

        // Test fallback to NOW if the field is missing or not a number
        tests.add(new Object[]{"missing", null, AssertTimestamp.DEFAULT});
        tests.add(new Object[]{"timestampISO", null, AssertTimestamp.DEFAULT});

        // Test timestamps in seconds
        tests.add(new Object[]{"timestampInSeconds", "unix_seconds", AssertTimestamp.SECONDS(TIMESTAMP_SECS.getTime())});
        tests.add(new Object[]{"timestampInSecondsAsInt", "unix_seconds", AssertTimestamp.SECONDS(TIMESTAMP_SECS_INT.getTime())});

        // Test fallback to NOW if the field is missing or not a number
        tests.add(new Object[]{"missing", "unix_seconds", AssertTimestamp.DEFAULT});
        tests.add(new Object[]{"timestampISO", "unix_seconds", AssertTimestamp.DEFAULT});

        // Test ISO
        tests.add(new Object[]{"timestampISO", "ISO-8601", AssertTimestamp.EXACT(TIMESTAMP_ISO.getTime())});
        // Test fallback to NOW if the field is missing or not a valid format
        tests.add(new Object[]{"missing", "ISO-8601", AssertTimestamp.DEFAULT});
        tests.add(new Object[]{"timestampBadISO", "ISO-8601", AssertTimestamp.DEFAULT});
        tests.add(new Object[]{"timestamp", "ISO-8601", AssertTimestamp.DEFAULT});

        // Test with a custom format (we assert that seconds are equal because our format does not support millis)
        tests.add(new Object[]{"timestampCustom", CUSTOM_SDF_FORMAT, AssertTimestamp.SECONDS(TIMESTAMP_SDF.getTime())});
        // Test fallback to NOW if the field is missing or not a valid format
        tests.add(new Object[]{"missing", CUSTOM_SDF_FORMAT, AssertTimestamp.DEFAULT});
        tests.add(new Object[]{"timestampBadISO", CUSTOM_SDF_FORMAT, AssertTimestamp.DEFAULT});
        tests.add(new Object[]{"timestamp", CUSTOM_SDF_FORMAT, AssertTimestamp.DEFAULT});

        // Test garbage
        tests.add(new Object[]{"timestampCustom", "Invalid Format", AssertTimestamp.DEFAULT});
        tests.add(new Object[]{"missing", "Invalid Format", AssertTimestamp.DEFAULT});
        tests.add(new Object[]{"timestampBadISO", "Invalid Format", AssertTimestamp.DEFAULT});
        tests.add(new Object[]{"", "", AssertTimestamp.DEFAULT});
        tests.add(new Object[]{" ", " ", AssertTimestamp.DEFAULT});

        // Rebuild the tests so we can test both Json and Binary decoders
        List<Object[]> actualTests = new ArrayList<Object[]>(tests.size()*2);
        for(Object[] params : tests) {
            List<Object> nParams = new ArrayList<Object>();
            nParams.add(AvroJsonMessageDecoder.class);
            nParams.add(new TestMessage(getAvroJsonPayload()));
            nParams.addAll(Arrays.asList(params));
            actualTests.add(nParams.toArray());

            nParams = new ArrayList<Object>();
            nParams.add(AvroBinaryMessageDecoder.class);
            nParams.add(new TestMessage(getAvroBinaryPayload()));
            nParams.addAll(Arrays.asList(params));
            actualTests.add(nParams.toArray());
        }
        return actualTests;
    }

    private static byte[] getAvroBinaryPayload() throws IOException {
        TestTimestampSchema avroMessage = TestTimestampSchema.newBuilder()
                .setName("name")
                .setTimestamp(TIMESTAMP_MILLIS.getTime())
                .setTimestampInSeconds(TIMESTAMP_SECS.getTime() / 1000)
                .setTimestampInSecondsAsInt((int) (TIMESTAMP_SECS_INT.getTime() / 1000))
                .setTimestampCustom(customFormat.format(TIMESTAMP_SDF))
                .setTimestampBadCustom("garbage")
                .setTimestampISO(new DateTime(TIMESTAMP_ISO.getTime()).toString())
                .setTimestampBadISO("garbage").build();
        DatumWriter<TestTimestampSchema> writer = new SpecificDatumWriter<TestTimestampSchema>(TestTimestampSchema.class);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Encoder enc = EncoderFactory.get().binaryEncoder(baos, null);
        writer.write(avroMessage, enc);
        enc.flush();
        baos.close();
        return baos.toByteArray();
    }

    private static String getAvroJsonPayload() throws IOException {
        JsonObject avroMessage = new JsonObject();
        avroMessage.add("name", new JsonPrimitive("name"));
        avroMessage.add("timestamp", new JsonPrimitive(TIMESTAMP_MILLIS.getTime()));
        avroMessage.add("timestampInSeconds", new JsonPrimitive(TIMESTAMP_SECS.getTime() / 1000));
        avroMessage.add("timestampInSecondsAsInt", new JsonPrimitive((int) (TIMESTAMP_SECS_INT.getTime() / 1000)));
        avroMessage.add("timestampCustom", new JsonPrimitive(customFormat.format(TIMESTAMP_SDF)));
        avroMessage.add("timestampBadCustom", new JsonPrimitive("garbage"));
        avroMessage.add("timestampISO", new JsonPrimitive(new DateTime(TIMESTAMP_ISO.getTime()).toString()));
        avroMessage.add("timestampBadISO", new JsonPrimitive("garbage"));
        return new GsonBuilder().create().toJson(avroMessage);
    }

    public TestAvroMessageTimestamps(Class<MessageDecoder<Message, GenericData.Record>> clazz,
            TestMessage testMessage, String field, String format, AssertTimestamp assertion) {
        this.decoderClass = clazz;
        this.testMessage = testMessage;
        if(field != null) {
            properties.setProperty(CamusAvroWrapper.CAMUS_MESSAGE_TIMESTAMP_FIELD, field);
        }
        if(format != null) {
            properties.setProperty(CamusAvroWrapper.CAMUS_MESSAGE_TIMESTAMP_FORMAT, format);
        }
        properties.setProperty(KafkaTopicSchemaRegistry.SCHEMA_REGISTRY_CLASS,
                KafkaTopicSchemaRegistry.class.getName());
        this.assertion = assertion;
        this.testName = "Test (" + assertion.getClass().getSimpleName() + ") "
                + "decoder: " + decoderClass.getSimpleName()
                + ", field: " + field + ", format: " + format;
    }

    @Before
    public void init() throws InstantiationException, IllegalAccessException {
        this.decoder = decoderClass.newInstance();
        this.decoder.init(properties, "testprefix_TestTimestampSchema");
    }

    @Test
    public void test() {
        CamusWrapper<Record> wrapper = this.decoder.decode(testMessage);
        assertion.assertTimestamp(testName, wrapper.getTimestamp());
    }

    public static abstract class AssertTimestamp {
        public static AssertTimestamp DEFAULT = new AssertNow();
        public static AssertTimestamp EXACT(long expected) {
            return new AssertExact(expected);
        }

        public static AssertTimestamp SECONDS(long expected) {
            return new AssertSecondsExact(expected);
        }

        abstract void assertTimestamp(String message, long timeStamp);
        public static class AssertNow extends AssertTimestamp {
            @Override
            public void assertTimestamp(String message, long timeStamp) {
                Assert.assertTrue(message + "( timeStamp:" + new Date(timeStamp) + " >= NOW )" , timeStamp >= NOW.getTime());
            }
        }

        public static class AssertExact extends AssertTimestamp {
            private long expected;
            public AssertExact(long expected) {
                this.expected = expected;
            }
            @Override
            public void assertTimestamp(String message, long timeStamp) {
                Assert.assertEquals(message + "( timeStamp:" + new Date(timeStamp)
                        + " == " + new Date(expected)+" )",
                    expected, timeStamp);
            }
        }

        public static class AssertSecondsExact extends AssertExact {
            public AssertSecondsExact(long expected) {
                super((expected/1000) * 1000);
            }
        }
    }
}
