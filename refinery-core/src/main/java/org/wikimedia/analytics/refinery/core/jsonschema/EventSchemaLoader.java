package org.wikimedia.analytics.refinery.core.jsonschema;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.log4j.Logger;

import java.net.URI;

/**
 * Class to load and cache JSONSchema JsonNodes from URIs and event data.
 *
 * Usage:
 *
 * EventSchemaLoader schemaLoader = new EventSchemaLoader("file:///path/to/schemas");
 *
 * // Load the JSONSchema at file:///path/to/schemas/test/event/0.0.2
 * schemaLoader.getEventSchema("/test/event/0.0.2");
 *
 * // Load the JsonNode or JSON String event's JSONSchema at /$schema
 * schemaLoader.getEventSchema(event);
 */
public class EventSchemaLoader {

    protected static final String SCHEMA_FIELD_DEFAULT = "/$schema";

    protected final String baseURI;
    protected final JsonPointer schemaFieldPointer;
    protected final JsonSchemaLoader schemaLoader = JsonSchemaLoader.getInstance();

    private static final Logger log = Logger.getLogger(EventSchemaLoader.class.getName());


    /**
     * Constructs a EventSchemaLoader with no baseURI prefix and uses /$schema to extract
     * schema URIs from events.
     */
    public EventSchemaLoader() {
        this("");
    }

    /**
     * Constructs a EventSchemaLoader with a baseURI and uses /$schema to extract
     * schema URIs from events.
     * @param baseUri
     */
    public EventSchemaLoader(String baseUri) {
        this(baseUri, SCHEMA_FIELD_DEFAULT);
    }

    /**
     * Constructs a EventSchemaLoader that prefixes URIs with baseURI and
     * extracts schema URIs from the schemaField in events.
     * @param baseURI
     * @param schemaField
     */
    public EventSchemaLoader(String baseURI, String schemaField) {
        this.baseURI = baseURI;
        this.schemaFieldPointer = JsonPointer.compile(schemaField);
    }

    /**
     * Returns the content at schemaURI as a JsonNode JSONSchema.
     *
     * @param schemaUri
     * @return the jsonschema at schemaURI.
     */
    public JsonNode load(URI schemaUri) {
        log.debug("Loading event schema at " + schemaUri);
        return schemaLoader.load(schemaUri);
    }

    /**
     * Given an 'event' with it's JSONSchema URI at schemaField,
     * returns the schema URI prefix with baseURI.
     * @param event should have field at schemaFieldPointer pointing at its URI.
     * @return
     */
    public URI getEventSchemaUri(JsonNode event) {
        try {
            return new URI(this.baseURI + event.at(this.schemaFieldPointer).textValue());
        }
        catch (java.net.URISyntaxException e) {
            throw new RuntimeException(
                "Could not extract JSONSchema URI in field " + this.schemaFieldPointer +
                " json value with baseURI " + this.baseURI, e
            );
        }
    }

    /**
     * Given an event object, this extracts its schema URI at schemaField
     * (prefixed with baseURI) and returns the schema there.
     * @param event
     * @return
     */
    public JsonNode getEventSchema(JsonNode event) {
        URI schemaUri = this.getEventSchemaUri(event);
        return this.load(schemaUri);
    }

    /**
     * Given a JSON event string get its the schema URI, and load and return schema for the event.
     * @param eventString
     * @return
     */
    public JsonNode getEventSchema(String eventString) {
        JsonNode event = this.schemaLoader.parse(eventString);
        return this.getEventSchema(event);
    }

    public String toString() {
        return "EventSchemaLoader(" + baseURI + ", " + schemaFieldPointer + ")";
    }
}
