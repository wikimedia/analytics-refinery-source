package org.wikimedia.analytics.refinery.core.jsonschema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import com.github.fge.jsonschema.core.load.SchemaLoader;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Singleton class to handle fetching JSON schemas from URIs,
 * parsing them into JsonNodes, and caching them.
 * URIs can be local file:// URIs or remote HTTP:// URIs, or anything that
 * jackson.dataformat.yaml.YAMLParser can load.
 *
 * Usage:
 *
 * JsonSchemaLoader schemaLoader = JsonSchemaLoader.getInstance();
 * JsonNode schema = schemaLoader.load("http://my.schemas.org/schemas/test/event/schema/0.0.2")
 */
public class JsonSchemaLoader {

    static final JsonSchemaLoader instance = new JsonSchemaLoader();

    final ConcurrentHashMap<URI, com.fasterxml.jackson.databind.JsonNode> cache = new ConcurrentHashMap<>();

    final YAMLFactory  yamlFactory  = new YAMLFactory();
    // make sure to reuse, expensive to create
    final ObjectMapper objectMapper = new ObjectMapper();
    final SchemaLoader schemaLoader = new SchemaLoader();

    public JsonSchemaLoader() { }

    public static JsonSchemaLoader getInstance() {
        return instance;
    }

    /**
     * Given a schemaURI, this will request the JSON or YAML content at that URI and
     * parse it into a JsonNode.  $refs will be resolved.
     * The compiled schema will be cached by schemaURI, and only looked up once per schemaURI.
     *
     * @param schemaUri
     * @return the jsonschema at schemaURI.
     */
    public JsonNode load(URI schemaUri) {
        if (this.cache.containsKey(schemaUri)) {
            return this.cache.get(schemaUri);
        }

        YAMLParser yamlParser;
        try {
            yamlParser = yamlFactory.createParser(schemaUri.toURL());
        }
        catch (IOException e) {
            throw new RuntimeException("Failed reading JSON/YAML data from " + schemaUri, e);
        }

        try {
            // TODO get fancy and use URITranslator to resolve relative $refs somehow?
            // Use SchemaLoader so we resolve any JsonRefs in the JSONSchema.
            JsonNode schema = this.schemaLoader.load(this.parse(yamlParser)).getBaseNode();
            this.cache.put(schemaUri, schema);
            return schema;
        }
        catch (IOException e) {
            throw new RuntimeException("Failed loading JSON/YAML returned from " + schemaUri, e);
        }
    }


    /**
     * Parses the JSON or YAML string into a JsonNode.  This data does not
     * need to be a JSONSchema, but can be any JSON or YAML object.
     * @param data JSON or YAML string to parse into a JsonNode.
     * @return
     */
    public JsonNode parse(String data) {
        YAMLParser yamlParser;
        try {
            return this.parse(yamlFactory.createParser(data));
        }
        catch (IOException e) {
            throw new RuntimeException("Failed reading JSON/YAML from string '" + data + '"', e);
        }
    }

    private JsonNode parse(YAMLParser yamlParser) throws IOException {
        return this.objectMapper.readTree(yamlParser);
    }

}
