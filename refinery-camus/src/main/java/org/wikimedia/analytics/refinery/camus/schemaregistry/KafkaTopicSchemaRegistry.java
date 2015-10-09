package org.wikimedia.analytics.refinery.camus.schemaregistry;

import com.linkedin.camus.schemaregistry.MemorySchemaRegistry;
import com.linkedin.camus.schemaregistry.SchemaDetails;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * Created by mviswanathan on 10/6/15.
 *
 * This is a registry that uses a memory-backed schema
 * registry to store Avro schemas, by mapping between the Kafka
 * topic and the schema name. The convention it follows is:
 * assuming the topic is of the form PREFIX_SCHEMANAME, it strips out
 * the schema name. The Schema namespace is fixed, and it looks up
 * the schema class using Java reflection.
 *
 */
public class KafkaTopicSchemaRegistry extends MemorySchemaRegistry<Schema> {

    public static final String SCHEMA_NAMESPACE = "org.wikimedia.analytics.schemas";
    public static final String SCHEMA_REGISTRY_CLASS = "kafka.message.coder.schema.registry.class";

    public KafkaTopicSchemaRegistry() {
        super();
    }

    /**
     * Assuming the topic name is prefix_SchemaName, this method extracts the schemaName
     */
    public String getSchemaNameFromTopic(String topicName) {
        String schemaName = null;
        if (topicName.split("_").length >= 2) {
            Integer pos = topicName.indexOf('_');
            schemaName = topicName.substring(pos + 1);
        }
        return schemaName;
    }

    public String getSchemaCanonicalName(String schemaName) {
        return String.format("%s.%s", SCHEMA_NAMESPACE, schemaName);
    }

    @Override
    public SchemaDetails<Schema> getLatestSchemaByTopic(String topicName) {
        String schemaName = getSchemaNameFromTopic(topicName);
        if (schemaName == null) {
            throw new RuntimeException("Topic name doesn't conform to prefix_Schema convention");
        } else {
            String schemaFullName = getSchemaCanonicalName(schemaName);
            Schema schema;
            try {
                SpecificRecordBase record = (SpecificRecordBase) Class.forName(schemaFullName).newInstance();
                schema = record.getSchema();
                super.register(topicName, schema);
            } catch (Exception e) {
                throw new RuntimeException(String.format("Schema %s not found", schemaFullName), e);
            }
            return new SchemaDetails<Schema>(topicName, null, schema);
        }
    }
}
