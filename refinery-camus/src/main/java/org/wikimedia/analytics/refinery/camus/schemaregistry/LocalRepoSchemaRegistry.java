package org.wikimedia.analytics.refinery.camus.schemaregistry;

import com.linkedin.camus.schemaregistry.SchemaDetails;
import com.linkedin.camus.schemaregistry.SchemaRegistry;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


/*
 * Registry that fetches schemas from local git depot
 * At this time this registry occupies the 2nd step on the
 * schema resolution chain. Schemas are fetched from memory if present,
 * if not they are fetched from disk.

 * The convention it follows is: assuming the topic is of the form
 * PREFIX_SCHEMANAME, it strips out the schema name. The Schema namespace is
 * fixed, and it looks up the schema using {@link ClassLoader#getResourceAsStream(String)}.
 *
 *
 * As the chain of responsibility for fetching schemas is setup at this time this is the last link
 * so its successor is null, so it does not delegate fetching of schemas.
 */
public class LocalRepoSchemaRegistry implements SchemaRegistry<Schema>, Handler<SchemaRegistry> {

    /**
     * Default path in classpath to lookup schemas, e.g.:
     * /avro_schema_repo/${SchemaName}/${rev}.avsc
     */
    private static final String AVRO_SCHEMA_REPO_DEFAULT_PATH = "/avro_schema_repo";
    public static final String SCHEMA_NAMESPACE = "org.wikimedia.analytics.schemas";

    public static final String SCHEMA_REGISTRY_CLASS = "kafka.message.coder.schema.registry.class";

    private Properties props;

    private SchemaRegistry successor = null;

    public LocalRepoSchemaRegistry() {
        super();
    }


    public String getSchemaNameFromTopic(String topicName) {
        return SchemaRegistryHelper.getSchemaNameFromTopic(topicName);
    }

    @Override
    public SchemaDetails<Schema> getLatestSchemaByTopic(String topicName) {
        String schemaName = getSchemaNameFromTopic(topicName);

            String latestRev = props.getProperty(SCHEMA_NAMESPACE+"."+schemaName+".latestRev");
            if(latestRev == null) {
                // No latest rev provided
                return null;
            }
            MemorySchemaRegistryTuple tuple = loadSchema(schemaName, latestRev);




            if(tuple == null) {
                throw new RuntimeException(String.format("Lastest schema for %s not found (rev %s)",
                        schemaName, latestRev));
            }
            return new SchemaDetails<Schema>(topicName, String.valueOf(tuple.getId()), tuple.getSchema());

    }

    @Override
    public void init(Properties props) {
        this.props = props;
    }

    @Override
    public String register(String topic, Schema schema) {
        // schema are lazily fetched and not registered explicitely
        // camus schema registry is not really well suited for our use case.
        throw new UnsupportedOperationException("Unsupported operation");
    }

    private MemorySchemaRegistryTuple loadSchema(String name, String id) {
        InputStream is = this.getClass().getResourceAsStream(AVRO_SCHEMA_REPO_DEFAULT_PATH + "/" + name + "/" + id + ".avsc");
        if( is == null ) {
            throw new RuntimeException("Unknown schema:" + name + " with rev_id:" + id + " "); 
        }
        final Schema schema;
        try {
            Parser parser = new Parser();
            schema = parser.parse(is);
        } catch (IOException e) {
            throw new RuntimeException("Cannot parser schema:" + name + " with rev_id:" + id + " ", e); 
        }
        MemorySchemaRegistryTuple tuple = new MemorySchemaRegistryTuple(schema, Long.parseLong(id));
        return tuple;
    }

    @Override
    public Schema getSchemaByID(String topic, String id) {
        String name = getSchemaNameFromTopic(topic);

        MemorySchemaRegistryTuple tuple = loadSchema(name, id);

        return tuple.getSchema();
    }


    @Override
    public SchemaRegistry getSuccessor(){
        return this.successor;
    }

}
