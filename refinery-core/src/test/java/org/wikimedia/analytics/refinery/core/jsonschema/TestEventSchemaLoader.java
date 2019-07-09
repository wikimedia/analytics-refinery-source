package org.wikimedia.analytics.refinery.core.jsonschema;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import junit.framework.TestCase;
import org.junit.BeforeClass;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;


public class TestEventSchemaLoader extends TestCase {
    private EventSchemaLoader schemaLoader;

    private static String resourcesPath = new File("src/test/resources").getAbsolutePath();

    private static final JsonNodeFactory jf = JsonNodeFactory.instance;

    private static final ObjectNode expectedTestSchema = jf.objectNode();
    private static final ObjectNode testEvent = jf.objectNode();

    static {
        // Build the expected test event schema
        ObjectNode dt = jf.objectNode();
        dt.put("type", "string");
        dt.put("format", "date-time");
        dt.put("maxLength", 26);
        dt.put("description", "the time stamp of the event, in ISO8601 format");
        ObjectNode stream = jf.objectNode();
        stream.put("type", "string");
        stream.put("minLength", 1);
        stream.put("description", "The name of the stream/queue that this event belongs in.");
        ObjectNode metaProperties = jf.objectNode();
        metaProperties.set("dt", dt);
        metaProperties.set("stream", stream);
        ArrayNode metaRequired = jf.arrayNode();
        metaRequired.add(new TextNode("dt"));
        metaRequired.add(new TextNode("stream"));
        ObjectNode meta = jf.objectNode();
        meta.put("type", "object");
        meta.set("properties", metaProperties);
        meta.set("required", metaRequired);
        ObjectNode _schema = jf.objectNode();
        _schema.put("type", "string");
        _schema.put("description", "The URI identifying the jsonschema for this event.");
        ObjectNode testField = jf.objectNode();
        testField.put("type", "string");
        testField.put("default", "default test value");
        ObjectNode expectedTestSchemaProperties = jf.objectNode();
        expectedTestSchemaProperties.set("$schema", _schema);
        expectedTestSchemaProperties.set("meta", meta);
        expectedTestSchemaProperties.set("test", testField);
        expectedTestSchema.put("title", "test_event");
        expectedTestSchema.put("$id", "/test_event.schema");
        expectedTestSchema.put("$schema", "http://json-schema.org/draft-07/schema#");
        expectedTestSchema.put("type", "object");
        expectedTestSchema.set("properties", expectedTestSchemaProperties);


        ObjectNode eventMeta = jf.objectNode();
        eventMeta.put("dt", "2019-01-01T00:00:00Z");
        eventMeta.put("stream", "test.event");
        // Build the expected test event with $schema set to test event schema URI
        testEvent.put("$schema", "/test_event.schema.yaml");
        testEvent.set("meta", eventMeta);
        // Include unicode characters in the test event that are not allowed in yaml.
        // An event should be parsed using JsonParser instead of YAMLParser.
        // https://phabricator.wikimedia.org/T227484
        testEvent.put("test", "yoohoo \uD862\uDF4E");
    }


    @BeforeClass
    public void setUp() {
        schemaLoader = new EventSchemaLoader("file://" + resourcesPath);
    }

    public void testLoad() throws URISyntaxException {
        URI testSchemaUri = new URI("file://" + resourcesPath + "/test_event.schema.yaml");
        JsonNode testSchema = schemaLoader.load(testSchemaUri);
        assertEquals(
            "test event schema should load from yaml at " + testSchemaUri,
            expectedTestSchema,
            testSchema
        );
    }


    public void testGetEventSchemaURI() throws URISyntaxException {
        URI expectedSchemaUri = new URI("file://" + resourcesPath + "/test_event.schema.yaml");
        URI testSchemaUri = schemaLoader.getEventSchemaUri(testEvent);
        assertEquals(
            "Should load schema URI from event $schema field",
            expectedSchemaUri,
            testSchemaUri
        );
    }


    public void testGetEventSchema() {
        JsonNode testSchema = schemaLoader.getEventSchema(testEvent);
        assertEquals(
            "Should load schema from event $schema field",
            expectedTestSchema,
            testSchema
        );
    }

    public void testGetEventSchemaFromJsonString() {

        JsonNode testSchema = schemaLoader.getEventSchema(testEvent.toString());
        assertEquals(
            "Should load schema from JSON string event $schema field",
            expectedTestSchema,
            testSchema
        );
    }




}
