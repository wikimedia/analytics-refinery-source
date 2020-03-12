package org.wikimedia.analytics.refinery.core.jsonschema;

/**
 * Thrown when an error is encountered by JsonSchemaLoader
 * while attempting to load a JSONSchema from a URI.
 */
public class JsonSchemaLoadingException extends Exception {
    public JsonSchemaLoadingException(String message) {
        super(message);
    }

    public JsonSchemaLoadingException(String message, Exception cause) {
        super(message, cause);
    }
}
