package org.wikimedia.analytics.refinery.core.jsonschema;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.commons.io.IOUtils;
import java.io.IOException;
import java.net.URI;

public class JsonLoader {

    static final JsonLoader instance = new JsonLoader();

    final YAMLFactory  yamlFactory  = new YAMLFactory();
    final JsonFactory  jsonFactory  = new JsonFactory();

    // Make sure to reuse, expensive to create.
    // This should be thread safe.
    final ObjectMapper objectMapper = new ObjectMapper();

    public JsonLoader() { }

    public static JsonLoader getInstance() {
        return instance;
    }

    /**
     * Given a schemaURI, this will request the JSON or YAML content at that URI and
     * parse it into a JsonNode.  $refs will be resolved.
     * The compiled schema will be cached by schemaURI, and only looked up once per schemaURI.
     *
     * @param uri
     * @return the jsonschema at schemaURI.
     */
    public JsonNode load(URI uri) throws JsonLoadingException {

        JsonParser parser;
        try {
            parser = this.getParser(uri);
        }
        catch (IOException e) {
            throw new JsonLoadingException("Failed reading JSON/YAML data from " + uri, e);
        }

        try {
            return this.parse(parser);
        }
        catch (IOException e) {
            throw new JsonLoadingException("Failed loading JSON/YAML data from " + uri, e);
        }
    }


    /**
     * Parses the JSON or YAML string into a JsonNode.
     * @param data JSON or YAML string to parse into a JsonNode.
     * @return
     */
    public JsonNode parse(String data) throws JsonLoadingException {
        try {
            return this.parse(this.getParser(data));
        }
        catch (IOException e) {
            throw new JsonLoadingException(
                "Failed parsing JSON/YAML data from string '" + data + '"', e
            );
        }
    }

    private JsonNode parse(JsonParser parser) throws IOException {
        return this.objectMapper.readTree(parser);
    }

    /**
     * Gets either a YAMLParser or a JsonParser for String data
     * @param data
     * @return
     */
    private JsonParser getParser(String data) throws IOException {
        // If the first character is { or [, assume this is
        // JSON data and use a JsonParser.  Otherwise assume
        // YAML and use a YAMLParser.
        char firstChar = data.charAt(0);
        if (firstChar == '{' || firstChar == '[') {
            return this.jsonFactory.createParser(data);
        } else {
            return this.yamlFactory.createParser(data);
        }
    }

    /**
     * Gets either a YAMLParser or a JsonParser for the data at uri
     * @param uri
     * @return
     */
    private JsonParser getParser(URI uri) throws IOException {
        String content = IOUtils.toString(uri.toURL(), "UTF-8");
        return this.getParser(content);
    }

}
