package org.wikimedia.analytics.refinery.camus.schemaregistry;

import com.linkedin.camus.schemaregistry.SchemaDetails;
import com.linkedin.camus.schemaregistry.SchemaRegistry;
import org.apache.avro.Schema;
import org.apache.commons.math3.util.Pair;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Created by mviswanathan on 10/6/15.
 *
 * This is a registry that uses a memory-backed schema registry to store Avro
 * schemas, by mapping between the Kafka topic and the schema name.
 *
 * If schema is not present on cache for given revision it delegates
 * to its successor to find it.
 *
 * Sucessor is set at initialization time. At this time it fetches
 * schemas from a local git repo but successor
 * could fetch schemas from http to access a schema registry.
 *
 * Plugging in a different sucessor requires changing only the initialization code.
 *
 *
 */
public class KafkaTopicSchemaRegistry implements SchemaRegistry<Schema>, Handler<SchemaRegistry> {

    public static final String SCHEMA_NAMESPACE = "org.wikimedia.analytics.schemas";

    public static final String SCHEMA_REGISTRY_CLASS = "kafka.message.coder.schema.registry.class";

    // Map to retreive a schema based on schemaName+revId
    private final Map<Pair<String, String>, MemorySchemaRegistryTuple> cache = new ConcurrentHashMap<Pair<String, String>, MemorySchemaRegistryTuple>();
    private Properties props;

    private SchemaRegistry successor = null;

    public KafkaTopicSchemaRegistry() {
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
        Schema schema = this.getSchemaByID(topicName, latestRev);

        return new SchemaDetails<Schema>(topicName, latestRev, schema);

    }

    @Override
    public void init(Properties props) {
        this.props = props;
        LocalRepoSchemaRegistry   localRegistry = new LocalRepoSchemaRegistry() ;
        localRegistry.init(props);
        this.successor = localRegistry;
    }

    @Override
    public String register(String topic, Schema schema) {
        // schema are lazily fetched and not registered explicitely
        // camus schema registry is not really well suited for our use case.
        throw new UnsupportedOperationException("Unsupported operation");
    }


    @Override
    public Schema getSchemaByID(String topic, String id) {

        String name = getSchemaNameFromTopic(topic);
        MemorySchemaRegistryTuple tuple = cache.get(new Pair<String, String>(name, id));
        if(tuple == null) {
            Schema schema = (Schema) successor.getSchemaByID(topic, id);
            tuple = (new MemorySchemaRegistryTuple(schema, Long.parseLong(id)));
            cache.put(new Pair<String, String>(name, id), tuple);
        }

        return tuple.getSchema();
    }


    @Override
    public SchemaRegistry getSuccessor(){
        return null;
    }

}
