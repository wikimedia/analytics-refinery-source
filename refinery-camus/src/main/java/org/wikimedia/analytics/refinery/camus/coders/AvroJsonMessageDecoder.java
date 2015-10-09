package org.wikimedia.analytics.refinery.camus.coders;

import com.google.gson.JsonParser;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.Message;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;
import com.linkedin.camus.etl.kafka.coders.KafkaAvroMessageDecoder;
import com.linkedin.camus.schemaregistry.CachedSchemaRegistry;
import com.linkedin.camus.schemaregistry.SchemaRegistry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.log4j.Logger;
import org.wikimedia.analytics.refinery.camus.schemaregistry.KafkaTopicSchemaRegistry;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by mviswanathan on 10/6/15.
 *
 * This Decoder helps to decode JSON encoded Avro messages that are read from
 * Kafka, and validate them against a schema with the help of a SchemaRegistry,
 * and produce Avro records of type GenericData.Record.
 *
 * The main difference between this class and the ones on Camus upstream is that,
 * it doesn't assume the convention where an Avro message has a field that is
 * fixed to denote a schemaID. Because we don't have rest based SchemaRegistry that
 * issues ids to schemas, and have only the latest version of the schema commited to
 * source, we ignore the notion of an ID. We can retrieve the underlying schema by using
 * the registry's getLatestSchemaByTopic. Another difference is that there is no notion
 * of a targetSchema, which will exist only if the incoming message was associated with a
 * schemaID for an older version of the schema, and we had to store it in the latest
 * version. In our case, there is only one version of the schema that the system
 * is aware of at any point.
 */
public class AvroJsonMessageDecoder extends MessageDecoder<Message, GenericData.Record> {

    private static final Logger log = Logger.getLogger(AvroJsonMessageDecoder.class);

    JsonParser jsonParser;
    protected DecoderFactory decoderFactory;
    protected SchemaRegistry<Schema> registry;
    Schema latestSchema;

    public AvroJsonMessageDecoder() {
        this.jsonParser = new JsonParser();
    }

    public void init(Properties props, String topicName) {
        super.init(props, topicName);
        this.props = props;
        this.topicName = topicName;

        try {
            SchemaRegistry<Schema> registry = (SchemaRegistry<Schema>) Class.forName(
                    props.getProperty(KafkaTopicSchemaRegistry.SCHEMA_REGISTRY_CLASS)).newInstance();
            log.info("Prop " + KafkaTopicSchemaRegistry.SCHEMA_REGISTRY_CLASS + " is: "
                    + props.getProperty(KafkaTopicSchemaRegistry.SCHEMA_REGISTRY_CLASS));
            log.info("Underlying schema registry for topic: " + topicName + " is: " + registry);
            registry.init(props);

            latestSchema = registry.getLatestSchemaByTopic(topicName).getSchema();
            this.registry = new CachedSchemaRegistry<Schema>(registry, props);
        } catch (Exception e) {
            throw new MessageDecoderException(e);
        }

        this.decoderFactory = DecoderFactory.get();
    }

    public class MessageDecoderHelper {
        //private Message message;
        private Schema schema;
        private final SchemaRegistry<Schema> registry;
        private final String topicName;

        public MessageDecoderHelper(SchemaRegistry<Schema> registry, String topicName) {
            this.registry = registry;
            this.topicName = topicName;
        }

        public Schema getSchema() {
            return schema;
        }

        public MessageDecoderHelper invoke() {
            schema = latestSchema;
            return this;
        }
    }

    @Override
    public CamusWrapper<GenericData.Record> decode(Message message) {
        String payloadString = new String(message.getPayload());
        try {
            MessageDecoderHelper helper = new MessageDecoderHelper(registry, topicName).invoke();
            GenericRecord datum = null;
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(helper.getSchema());
            InputStream inStream = new ByteArrayInputStream(message.getPayload());
            JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(helper.getSchema(), inStream);
            datum = (GenericRecord) reader.read(datum, jsonDecoder);
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(helper.getSchema());
            Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);

            writer.write(datum, encoder);
            encoder.flush();
            output.close();

            DatumReader<GenericRecord> avroReader = new GenericDatumReader<GenericRecord>(helper.getSchema());
            return new KafkaAvroMessageDecoder.CamusAvroWrapper((GenericData.Record) avroReader.read(null,
                    this.decoderFactory.binaryDecoder(output.toByteArray(), 0, output.toByteArray().length, null)));
        } catch (RuntimeException e) {
            log.error("Caught exception while parsing JSON string '" + payloadString + "'.");
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new MessageDecoderException(e);
        }
    }

}
