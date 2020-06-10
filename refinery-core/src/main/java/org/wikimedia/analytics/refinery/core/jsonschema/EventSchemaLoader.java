package org.wikimedia.analytics.refinery.core.jsonschema;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.log4j.Logger;

import java.net.URI;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Collectors;

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

    protected final List<String> baseUris;
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
     * Constructs a EventSchemaLoader with a baseUri and uses /$schema to extract
     * schema URIs from events.
     * @param baseUri
     */
    public EventSchemaLoader(String baseUri) {
        this(Collections.singletonList(baseUri));
    }

    /**
     * Constructs a EventSchemaLoaer with possible baseURIs and uses /$schema to extract
     * schema URIs from events.
     * @param baseUris
     */
    public EventSchemaLoader(List<String> baseUris) {
        this(baseUris, SCHEMA_FIELD_DEFAULT);
    }

    /**
     * Constructs a EventSchemaLoader that prefixes URIs with baseURI and
     * extracts schema URIs from the schemaField in events.
     * @param baseUris
     * @param schemaField
     */
    public EventSchemaLoader(List<String> baseUris, String schemaField) {
        this.baseUris = baseUris;
        this.schemaFieldPointer = JsonPointer.compile(schemaField);
    }

    /**
     * Returns the content at schemaURI as a JsonNode JSONSchema.
     *
     * @param schemaUri
     * @return the jsonschema at schemaURI.
     */
    public JsonNode load(URI schemaUri) throws JsonLoadingException {
        log.debug("Loading event schema at " + schemaUri);
        return schemaLoader.load(schemaUri);
    }

    /**
     * Given a list of schemaURIs, this returns the first successfully loaded schema.
     * If no schema is found, an exception will be thrown.
     * @param schemaURIs
     * @return
     */
    public JsonNode loadFirst(List<URI> schemaURIs) throws JsonLoadingException {
        JsonNode schema = null;
        List<JsonLoadingException> loaderExceptions = new ArrayList<>();

        for (URI schemaURI: schemaURIs) {
            try {
                schema = this.load(schemaURI);
                break;
            } catch (JsonLoadingException e) {
                loaderExceptions.add(e);
            }
        }

        if (schema != null) {
            return schema;
        } else {
            // If we failed loading a schema but we encountered any JsonSchemaLoaderExceptions
            // while trying, log them all but only throw the first one.
            if (!loaderExceptions.isEmpty()) {
                for (JsonLoadingException e: loaderExceptions) {
                    log.error("Got JsonSchemaLoaderException when trying to load event schema", e);
                }
                throw loaderExceptions.get(0);
            } else {
                throw new RuntimeException(this + " failed loading event schema");
            }
        }
    }

    /**
     * Extracts the value at schemaFieldPointer from the event as a URI
     * @param event
     * @return schema URI
     */
    public URI extractSchemaUri(JsonNode event) {
        String uriString = event.at(this.schemaFieldPointer).textValue();
        try {
            return new URI(uriString);
        }
        catch (java.net.URISyntaxException e) {
            throw new RuntimeException(
                    "Failed building new URI from " + uriString + ". " + e.getMessage()
            );
        }
    }

    /**
     * Prepends the schemaUri with each of the baseUris.
     * (Note, the Urls returned here are fully qualified).
     * @param schemaUri
     * @return List of urls prepended with this EventSchemaLoader's base URIs.
     */
    public List<URI> getPossibleSchemaUrls(URI schemaUri) {
        return this.baseUris.stream().map(baseUri -> {
             try {
                 return new URI(baseUri + schemaUri);
             }
             catch (java.net.URISyntaxException e) {
                 throw new RuntimeException(
                     "Failed not build new URI with "  + baseUri + " and " + schemaUri + ". " +
                     e.getMessage()
                 );
             }
        }).collect(Collectors.toList());
    }

    /**
     * Extracts the event's schema URI and prepends it with each of the baseURIs
     * (Note, the Urls returned here are fully qualified).
     * @param event
     * @return List of schema URIs where this event's schema might be.
     */
    public List<URI> getPossibleSchemaUrls(JsonNode event) {
        return getPossibleSchemaUrls(extractSchemaUri(event));
    }


    /**
     * Converts the given schemaUri to a 'latest' schema URI.  E.g.
     * /my/schema/1.0.0 -> /my/schema/latest
     *
     * @param schemaUri
     * @return
     */
    public URI getLatestSchemaUri(URI schemaUri) {
        return schemaUri.resolve(LATEST_FILE_NAME);
    }

    /**
     * Extracts the event's schema URI and converts it to a latest schema URI.
     * @param event
     * @return 'latest' version of this event's schema URI
     */
    public URI getLatestSchemaUri(JsonNode event) {
        return getLatestSchemaUri(extractSchemaUri(event));
    }


    /**
     * @param schemaUri
     * @return a List of possible 'latest' schmea uris
     */
    public List<URI> getPossibleLatestSchemaUrls(URI schemaUri) {
        return getPossibleSchemaUrls(getLatestSchemaUri(schemaUri));
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
    public List<URI> getPossibleLatestSchemaUrls(JsonNode event) {
        return getPossibleLatestSchemaUrls(getLatestSchemaUri(event));
    }


    /**
     * Get a JsonSchema at schemaUri looking in all baseUris.
     * @param schemaUri
     * @return
     * @throws JsonLoadingException
     */
    public JsonNode getSchema(URI schemaUri) throws JsonLoadingException {
        return this.loadFirst(this.getPossibleSchemaUrls(schemaUri));
    }

    /**
     * Get a 'latest' JsonSchema given a schema URI looking in all baseUris.
     * @param schemaUri
     * @return
     * @throws JsonLoadingException
     */
    public JsonNode getLatestSchema(URI schemaUri) throws JsonLoadingException {
        return this.loadFirst(this.getPossibleLatestSchemaUrls(schemaUri));
    }


//    GET SCHEMA FROM EVENT or EVENT STRING

    /**
     * Given an event object, this extracts its schema URI at schemaField
     * (prefixed with baseURI) and returns the schema there.
     * @param event
     * @return
     */
    public JsonNode getEventSchema(JsonNode event) throws JsonLoadingException {
        return this.loadFirst(this.getPossibleSchemaUrls(event));
    }

    /**
     * Given a JSON event string, get its  schema URI,
     * and load and return schema for the event.
     * @param eventString
     * @return
     */
    public JsonNode getEventSchema(String eventString) throws JsonLoadingException {
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
    public JsonNode getLatestEventSchema(JsonNode event) throws JsonLoadingException {
        return this.loadFirst(this.getPossibleLatestSchemaUrls(event));
    }

    /**
     * Given a JSON event string, get its schema URI,
     * and load and return latest schema for the event.
     * @param eventString
     * @return
     */
    public JsonNode getLatestEventSchema(String eventString) throws JsonLoadingException {
        JsonNode event = this.schemaLoader.parse(eventString);
        return getLatestEventSchema(event);
    }

    public String toString() {
        return "EventSchemaLoader([" + String.join(", ", baseUris) + "], " +
            schemaFieldPointer + ")";
    }

    public String getLatestVersionFileName() {
        return LATEST_FILE_NAME;
    }
}
