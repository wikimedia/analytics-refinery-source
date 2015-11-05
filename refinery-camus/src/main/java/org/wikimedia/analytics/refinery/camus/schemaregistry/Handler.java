package org.wikimedia.analytics.refinery.camus.schemaregistry;

/**
 * Interface to help implement a chaain of responsability pattern.
 * Please see KafkaTopicSchemaRegistry
 */
public interface Handler<T> {
    T    getSuccessor()  ;


}
