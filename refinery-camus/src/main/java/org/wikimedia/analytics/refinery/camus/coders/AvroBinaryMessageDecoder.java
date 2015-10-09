package org.wikimedia.analytics.refinery.camus.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.Message;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;
import com.linkedin.camus.schemaregistry.CachedSchemaRegistry;
import com.linkedin.camus.schemaregistry.SchemaRegistry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.Text;
import org.wikimedia.analytics.refinery.camus.schemaregistry.KafkaTopicSchemaRegistry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

/**
 * Created by mviswanathan on 10/6/15.
 *
 * This Decoder helps to decode Avro Binary messages that are read from Kafka,
 * and validate them against a schema with the help of a SchemaRegistry,
 * and produce Avro records of type GenericData.Record.
 *
 * The main difference between this class and the ones on Camus upstream is that,
 * it doesn't assume the convention where an Avro message has the first few bytes
 * fixed to denote a schemaID. Because we don't have rest based SchemaRegistry that
 * issues ids to schemas, and have only the latest version of the schema commited to
 * source, we ignore the notion of an ID. We can retrieve the underlying schema by using
 * the registry's getLatestSchemaByTopic. Another difference is that there is no notion
 * of a targetSchema, which will exist only if the incoming message was associated with a
 * schemaID for an older version of the schema, and we had to store it in the latest
 * version. In our case, there is only one version of the schema that the system
 * is aware of at any point.
 *
 */
public class AvroBinaryMessageDecoder extends MessageDecoder<Message, GenericData.Record> {
    private static final Logger log = Logger.getLogger(AvroBinaryMessageDecoder.class);

    protected DecoderFactory decoderFactory;
    protected SchemaRegistry<Schema> registry;

    @Override
    public void init(Properties props, String topicName) {
        super.init(props, topicName);
        try {
            SchemaRegistry<Schema> registry = (SchemaRegistry<Schema>) Class.forName(
                    props.getProperty(KafkaTopicSchemaRegistry.SCHEMA_REGISTRY_CLASS)).newInstance();
            log.info("Prop " + KafkaTopicSchemaRegistry.SCHEMA_REGISTRY_CLASS + " is: "
                    + props.getProperty(KafkaTopicSchemaRegistry.SCHEMA_REGISTRY_CLASS));
            log.info("Underlying schema registry for topic: " + topicName + " is: " + registry);
            registry.init(props);

            this.registry = new CachedSchemaRegistry<Schema>(registry, props);
        } catch (Exception e) {
            throw new MessageDecoderException(e);
        }

        decoderFactory = DecoderFactory.get();
    }

    public class MessageDecoderHelper {
        private ByteBuffer buffer;
        private Schema schema;
        private int start;
        private int length;
        private final SchemaRegistry<Schema> registry;
        private final String topicName;
        private byte[] payload;

        public MessageDecoderHelper(SchemaRegistry<Schema> registry, String topicName, byte[] payload) {
            this.registry = registry;
            this.topicName = topicName;
            this.payload = payload;
        }

        public ByteBuffer getBuffer() {
            return buffer;
        }

        public Schema getSchema() {
            return schema;
        }

        public int getStart() {
            return start;
        }

        public int getLength() {
            return length;
        }

        private ByteBuffer getByteBuffer(byte[] payload) {
            return ByteBuffer.wrap(payload);
        }

        public MessageDecoderHelper invoke() {
            schema = registry.getLatestSchemaByTopic(topicName).getSchema();
            buffer = getByteBuffer(payload);
            start =  buffer.arrayOffset();
            length = buffer.limit();
            return this;
        }
    }

    public CamusWrapper<GenericData.Record> decode(Message message) {
        try {
            MessageDecoderHelper helper = new MessageDecoderHelper(registry, topicName, message.getPayload()).invoke();
            DatumReader<GenericData.Record> reader = new GenericDatumReader<GenericData.Record>(helper.getSchema());

            return new CamusAvroWrapper(reader.read(null,
                    decoderFactory.binaryDecoder(helper.getBuffer().array(), helper.getStart(), helper.getLength(), null)));

        } catch (IOException e) {
            throw new MessageDecoderException(e);
        }
    }

    public static class CamusAvroWrapper extends CamusWrapper<GenericData.Record> {

        public CamusAvroWrapper(GenericData.Record record) {
            super(record);
            GenericData.Record header = (GenericData.Record) super.getRecord().get("header");
            if (header != null) {
                if (header.get("server") != null) {
                    put(new Text("server"), new Text(header.get("server").toString()));
                }
                if (header.get("service") != null) {
                    put(new Text("service"), new Text(header.get("service").toString()));
                }
            }
        }

        @Override
        public long getTimestamp() {
            GenericData.Record header = (GenericData.Record) super.getRecord().get("header");

            if (header != null && header.get("time") != null) {
                return (Long) header.get("time");
            } else if (super.getRecord().get("timestamp") != null) {
                return (Long) super.getRecord().get("timestamp");
            } else {
                return System.currentTimeMillis();
            }
        }
    }

}
