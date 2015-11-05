package org.wikimedia.analytics.refinery.camus.schemaregistry;

import org.apache.avro.Schema;

/**
 * Class shared by schema registries for cache lookups
 * mmm.... is this needed
 */
public class MemorySchemaRegistryTuple implements Comparable<MemorySchemaRegistryTuple> {
    private final long id;
    private final Schema schema;

    public MemorySchemaRegistryTuple(Schema schema, long id) {
        this.schema = schema;
        this.id = id;
    }

    public Schema getSchema() {
        return schema;
    }

    public long getId() {
        return id;
    }

    @Override
    public int compareTo(MemorySchemaRegistryTuple o) {
        return Long.compare(id, o.id);
    }
}