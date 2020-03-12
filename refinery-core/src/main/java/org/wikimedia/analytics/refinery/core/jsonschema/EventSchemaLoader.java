package org.wikimedia.analytics.refinery.core.jsonschema;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.log4j.Logger;

import java.net.URI;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Class to load and cache JSONSchema JsonNodes from URIs and event data.
 *
 * Usage:
 *
 * EventSchemaLoader schemaLoader = new EventSchemaLoader("file:///path/to/schemas");
 * // OR use multiple base URIs:
 * EventSchemaLoader schemaLoader = new EventSchemaLoader(
 *     new ArrayList<>(Arrays.asList(
 *         "file:///path/to/schemas1",
 *         "http://schema.repo.org/path/to/schemas"
 *     ))
 * );
 *
 * // Load the JSONSchema at file:///path/to/schemas/test/event/0.0.2
 * schemaLoader.getEventSchema("/test/event/0.0.2");
 *
 * // Load the JsonNode or JSON String event's JSONSchema at /$schema
 * schemaLoader.getEventSchema(event);
 */
public class EventSchemaLoader {

    protected static final String SCHEMA_FIELD_DEFAULT = "/$schema";
    protected static final String LATEST_FILE_NAME = "latest";

    protected final List<String> baseURIs;
    protected final JsonPointer schemaFieldPointer;
    protected final JsonSchemaLoader schemaLoader = JsonSchemaLoader.getInstance();

    private static final Logger log = Logger.getLogger(EventSchemaLoader.class.getName());

    /**
     * Constructs a EventSchemaLoader with no baseURI prefixes and uses /$schema to extract
     * schema URIs from events.
     */
    public EventSchemaLoader() {
        this("");
    }

    /**
     * Constructs a EventSchemaLoaer with a baseURI and uses /$schema to extract
     * schema URIs from events.
     * @param baseURI
     */
    public EventSchemaLoader(String baseURI) {
        this(Collections.singletonList(baseURI));
    }

    /**
     * Constructs a EventSchemaLoaer with possible baseURIs and uses /$schema to extract
     * schema URIs from events.
     * @param baseURIs
     */
    public EventSchemaLoader(List<String> baseURIs) {
        this(baseURIs, SCHEMA_FIELD_DEFAULT);
    }

    /**
     * Constructs a EventSchemaLoader that prefixes URIs with baseURI and
     * extracts schema URIs from the schemaField in events.
     * @param baseURIs
     * @param schemaField
     */
    public EventSchemaLoader(List<String> baseURIs, String schemaField) {
        this.baseURIs = baseURIs;
        this.schemaFieldPointer = JsonPointer.compile(schemaField);
    }

    /**
     * Returns the content at schemaURI as a JsonNode JSONSchema.
     *
     * @param schemaUri
     * @return the jsonschema at schemaURI.
     */
    public JsonNode load(URI schemaUri) throws JsonSchemaLoadingException {
        log.debug("Loading event schema at " + schemaUri);
        return schemaLoader.load(schemaUri);
    }

    /**
     * Given a list of schemaURIs, this returns the first successfully loaded schema.
     * If no schema is found, an exception will be thrown.
     * @param schemaURIs
     * @return
     */
    public JsonNode loadFirst(List<URI> schemaURIs) throws JsonSchemaLoadingException {
        JsonNode schema = null;
        List<JsonSchemaLoadingException> loaderExceptions = new ArrayList<>();

        for (URI schemaURI: schemaURIs) {
            try {
                schema = this.load(schemaURI);
                break;
            } catch (JsonSchemaLoadingException e) {
                loaderExceptions.add(e);
            }
        }

        if (schema != null) {
            return schema;
        } else {
            // If we failed loading a schema but we encountered any JsonSchemaLoaderExceptions
            // while trying, log them all but only throw the first one.
            if (!loaderExceptions.isEmpty()) {
                for (JsonSchemaLoadingException e: loaderExceptions) {
                    log.error("Got JsonSchemaLoaderException when trying to load event schema", e);
                }
                throw loaderExceptions.get(0);
            } else {
                throw new RuntimeException(this + " failed loading event schema");
            }
        }
    }

    /**
     * Extracts the event's schema URI and prepends each of the baseURIs to it
     * @param event
     * @return List of schema URIs where this event's schema might be.
     */
    public List<URI> getPossibleEventSchemaUris(JsonNode event) {
        List<URI> eventSchemaURIs = new ArrayList<>();
        for (String baseURI: baseURIs) {
            try {
                eventSchemaURIs.add(
                    new URI(baseURI + event.at(this.schemaFieldPointer).textValue())
                );
            }
            catch (java.net.URISyntaxException e) {
                throw new RuntimeException(
                    "Could not extract JSONSchema URI in field " + this.schemaFieldPointer +
                    " json value with baseURI " + baseURI, e
                );
            }
        }
        return eventSchemaURIs;
    }

    /**
     * Gets the possible schema URIs for this event, but replaces the filename
     * in the URI to LATEST_FILE_NAME.
     *
     * E.g. if possible schema URIs for this event are:
     * - https://schema.wikimedia.org/repositories/primary/jsonschema/test/event/1.0.0
     * - https://schema.wikimedia.org/repositories/secondary/jsonschema/test/event/1.0.0
     *
     * This will convert them to:
     * - https://schema.wikimedia.org/repositories/primary/jsonschema/test/event/latest
     * - https://schema.wikimedia.org/repositories/secondary/jsonschema/test/event/latest
     *
     * @param event
     * @return List of schema URIs where this event's latest schema might be.
     */
    public List<URI> getPossibleLatestEventSchemaUris(JsonNode event) {
        List<URI> eventSchemaURIs = getPossibleEventSchemaUris(event);
        List<URI> latestEventSchemaURIs = new ArrayList<>();

        for (URI schemaURI: eventSchemaURIs) {
            latestEventSchemaURIs.add(schemaURI.resolve(LATEST_FILE_NAME));
        }

        return latestEventSchemaURIs;
    }

    /**
     * Given an event object, this extracts its schema URI at schemaField
     * (prefixed with baseURI) and returns the schema there.
     * @param event
     * @return
     */
    public JsonNode getEventSchema(JsonNode event) throws JsonSchemaLoadingException {
        return this.loadFirst(this.getPossibleEventSchemaUris(event));
    }

    /**
     * Given a JSON event string, get its  schema URI,
     * and load and return schema for the event.
     * @param eventString
     * @return
     */
    public JsonNode getEventSchema(String eventString) throws JsonSchemaLoadingException {
        JsonNode event = this.schemaLoader.parse(eventString);
        return this.getEventSchema(event);
    }


    /**
     * Given an event object, this extracts its schema URI at schemaField
     * (prefixed with baseURI) and resolves it to the latest schema URI and returns
     * the schema there.
     * @param event
     * @return
     */
    public JsonNode getLatestEventSchema(JsonNode event) throws JsonSchemaLoadingException {
        return this.loadFirst(this.getPossibleLatestEventSchemaUris(event));
    }

    /**
     * Given a JSON event string, get its schema URI,
     * and load and return latest schema for the event.
     * @param eventString
     * @return
     */
    public JsonNode getLatestEventSchema(String eventString) throws JsonSchemaLoadingException {
        JsonNode event = this.schemaLoader.parse(eventString);
        return getLatestEventSchema(event);
    }

    public String toString() {
        return "EventSchemaLoader([" + String.join(", ", baseURIs) + "], " +
            schemaFieldPointer + ")";
    }
}
