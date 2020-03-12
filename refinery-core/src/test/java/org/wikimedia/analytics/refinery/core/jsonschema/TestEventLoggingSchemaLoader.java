package org.wikimedia.analytics.refinery.core.jsonschema;


import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.JsonNode;

import junit.framework.TestCase;
import org.junit.BeforeClass;

import java.io.File;
import java.net.URI;


public class TestEventLoggingSchemaLoader extends TestCase {

    private EventLoggingSchemaLoader schemaLoader;

    private static String schemaBaseUri = new File("src/test/resources/event-schemas/repo1").getAbsolutePath();

    private static final JsonNodeFactory jf = JsonNodeFactory.instance;

    private static final ObjectNode expectedEchoSchema = jf.objectNode();
    private static final ObjectNode expectedEncapsulatedEchoSchema;
    private static final ObjectNode expectedEventLoggingTestSchema = jf.objectNode();
    private static final ObjectNode expectedEncapsulatedEventLoggingTestSchema;

    private static final ObjectNode testEchoEvent = jf.objectNode();

    // testEvent is not used unless you uncomment the remote schema tests.
    private static final ObjectNode testEvent = jf.objectNode();

    static {
        // Build the expected encapsulated EventLogging event schema
        ObjectNode version = jf.objectNode();
        version.put("type", "string");
        version.put("required", true);
        version.put("description", "References the full specifications of the current version of Echo, example: 1.1");
        ObjectNode revisionId = jf.objectNode();
        revisionId.put("type", "integer");
        revisionId.put("description", "Revision ID of the edit that the event is for");
        ObjectNode expectedEchoEventProperties = jf.objectNode();
        expectedEchoEventProperties.set("version", version);
        expectedEchoEventProperties.set("revisionId", revisionId);

        expectedEchoSchema.put("title", "Echo");
        expectedEchoSchema.put("description", "Logs events related to the generation of notifications via the Echo extension");
        expectedEchoSchema.put("type", "object");
        expectedEchoSchema.set("properties", expectedEchoEventProperties);

        expectedEncapsulatedEchoSchema = (ObjectNode)EventLoggingSchemaLoader.buildEventLoggingCapsule();
        ((ObjectNode)expectedEncapsulatedEchoSchema.get("properties")).set("event", expectedEchoSchema);

        // Build the test Echo event
        ObjectNode testEchoEventField = jf.objectNode();
        testEchoEventField.put("version", "1.1");
        testEchoEventField.put("revisionId", 123);
        testEchoEvent.put("schema", "Echo");
        testEchoEvent.put("version", 7731316);
        testEchoEvent.set("event", testEchoEventField);

        // Build the expected encapsulated REMOTE EventLogging schema.
        // NOTE: The test that uses this is commented out.
        // That test requests the schema over HTTP at https://meta.wikimedia.org/w/api.php.
        // Uncomment if you want to test locally.
        ObjectNode otherMessage = jf.objectNode();
        otherMessage.put("type", "string");
        otherMessage.put("description", "Free-form text");
        otherMessage.put("required", true);
        ObjectNode eventLoggingTestSchemaProperties = jf.objectNode();
        eventLoggingTestSchemaProperties.set("OtherMessage", otherMessage);
        expectedEventLoggingTestSchema.put("type", "object");
        expectedEventLoggingTestSchema.put("title", "Test");
        expectedEventLoggingTestSchema.put("description", "Test schema for checking that EventLogging works");
        expectedEventLoggingTestSchema.set("properties", eventLoggingTestSchemaProperties);

        expectedEncapsulatedEventLoggingTestSchema = (ObjectNode)EventLoggingSchemaLoader.buildEventLoggingCapsule();
        ObjectNode capsuleProperties = (ObjectNode)expectedEncapsulatedEventLoggingTestSchema.get("properties");
        capsuleProperties.set("event", (JsonNode)expectedEventLoggingTestSchema);

        // build the test event with a remote schema.
        // This is only used if you uncomment remote tests.
        ObjectNode testEventField = jf.objectNode();
        testEventField.put("OtherMessage", "yoohoo");
        testEvent.put("schema", "Test");
        testEvent.set("event", testEventField);
    }


    @BeforeClass
    public void setUp() throws RuntimeException {
        schemaLoader = new EventLoggingSchemaLoader("file://" + schemaBaseUri + "/");
    }

    public void testLoad() throws Exception {
        URI echoSchemaUri = new URI("file://" + schemaBaseUri + "/Echo_7731316.schema.json");
        JsonNode echoSchema = schemaLoader.load(echoSchemaUri);
        assertEquals(
            "test event schema should load from json at " + echoSchemaUri,
            expectedEncapsulatedEchoSchema,
            echoSchema
        );
    }

    public void testGetEventLoggingSchemaUri() {
        URI uri = schemaLoader.eventLoggingSchemaUriFor(
            "Echo",
            7731316
        );

        assertEquals(
            "Should return an EventLogging schema URI with revid",
            "file://" + schemaBaseUri + "/?action=jsonschema&formatversion=2&format=json&title=Echo&revid=7731316",
            uri.toString()
        );
    }

    public void testGetEventLoggingLatestSchemaUri() {
        URI uri = schemaLoader.eventLoggingSchemaUriFor(
            "Echo"
        );

        assertEquals(
            "Should return an EventLogging schema URI without revid",
            "file://" + schemaBaseUri + "/?action=jsonschema&formatversion=2&format=json&title=Echo",
            uri.toString()
        );
    }

    public void testGetSchemaURIFromEvent() throws Exception {
        URI expectedSchemaUri = new URI("file://" + schemaBaseUri + "/?action=jsonschema&formatversion=2&format=json&title=Echo");
        URI uri = schemaLoader.getEventSchemaUri(testEchoEvent);
        assertEquals(
            "Should load schema URI from event schema field",
            expectedSchemaUri,
            uri
        );
    }

    // /**
    //  * NOTE: This test will actually perform a remote HTTP lookup to the Test EventLogging schema
    //  * hosted at https://meta.wikimedia.org/w/api.php.
    //  * @throws Exception
    //  */
    // public void testGetRemoteSchema() throws Exception {
    //     EventLoggingSchemaLoader remoteSchemaLoader = new EventLoggingSchemaLoader();
    //     JsonNode eventLoggingTestSchema = remoteSchemaLoader.getEventLoggingSchema("Test", 15047841);
    //     assertEquals(expectedEncapsulatedEventLoggingTestSchema, eventLoggingTestSchema);
    // }

    // /**
    //  * NOTE: This test will actually perform a remote HTTP lookup to the Test EventLogging schema
    //  * hosted at https://meta.wikimedia.org/w/api.php.
    //  * @throws Exception
    //  */
    // public void testGetRemoteSchemaFromEvent() throws Exception {
    //     EventLoggingSchemaLoader remoteSchemaLoader = new EventLoggingSchemaLoader();

    //     JsonNode schema = remoteSchemaLoader.getEventSchema(testEvent);
    //     assertEquals(
    //         "Should load schema from event schema field",
    //         expectedEncapsulatedEventLoggingTestSchema,
    //         schema
    //     );
    // }

}
