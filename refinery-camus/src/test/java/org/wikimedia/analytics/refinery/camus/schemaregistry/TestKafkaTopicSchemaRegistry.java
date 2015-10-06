package org.wikimedia.analytics.refinery.camus.schemaregistry;

import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Created by mviswanathan on 10/7/15.
 *
 * Unit tests for the KafkaTopicSchemaRegistry
 */
public class TestKafkaTopicSchemaRegistry {

    KafkaTopicSchemaRegistry registry = null;
    String schemaFullName = "org.wikimedia.analytics.schemas.TestSchema";

    @Before
    public void setUp() {
        registry = new KafkaTopicSchemaRegistry();
    }

    @Test(expected = java.lang.RuntimeException.class)
    public void testMalformedTopicString () {
        // The expected convention for a topic name is prefix_schema,
        // The schema is extracted from this. Malformed topic names
        // should not resolve to a schema, and throw and exception.
        assertNull(registry.getSchemaNameFromTopic("badTopicName"));
        registry.getLatestSchemaByTopic("badTopicName");

    }

    @Test
    public void testGetCanonicalSchemaName () {
        String schemaName = "TestSchema";
        assertEquals(registry.getSchemaCanonicalName(schemaName), schemaFullName);
    }

    @Test
    public void testGetLatestSchemaByTopic () {
        Schema schema = registry.getLatestSchemaByTopic("test_TestSchema").getSchema();
        assertEquals(schema.getFullName(), schemaFullName);
    }

    @Test(expected = java.lang.RuntimeException.class)
    public void testNonExistentSchemaShouldThrowException () {
        registry.getLatestSchemaByTopic("test_DummySchema").getSchema();
    }
}
