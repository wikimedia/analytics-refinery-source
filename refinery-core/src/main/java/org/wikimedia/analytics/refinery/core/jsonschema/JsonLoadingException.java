package org.wikimedia.analytics.refinery.core.jsonschema;

/**
 * Thrown when an error is encountered by JsonSchemaLoader
 * while attempting to load a JSONSchema from a URI.
 */
public class JsonLoadingException extends Exception {
    public JsonLoadingException(String message) {
        super(message);
    }

    public JsonLoadingException(String message, Exception cause) {
        super(message, cause);
    }
}
