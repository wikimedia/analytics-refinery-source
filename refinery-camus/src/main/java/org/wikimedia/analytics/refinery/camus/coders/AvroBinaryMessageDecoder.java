package org.wikimedia.analytics.refinery.camus.coders;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.log4j.Logger;
import org.wikimedia.analytics.refinery.camus.schemaregistry.KafkaTopicSchemaRegistry;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.Message;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;
import com.linkedin.camus.schemaregistry.SchemaDetails;
import com.linkedin.camus.schemaregistry.SchemaNotFoundException;
import com.linkedin.camus.schemaregistry.SchemaRegistry;

/**
 * Created by mviswanathan on 10/6/15.
 *
 * This Decoder helps to decode Avro Binary messages that are read from Kafka,
 * and validate them against a schema with the help of a SchemaRegistry,
 * and produce Avro records of type GenericData.Record.
 *
 * <p>
 * The writerSchema is mandatory and will be obtained from a field in the first bytes of the message :
 * <tt>[MAGIC][LONG][AVRO BINARY]</tt>
 * <br/>
 * The targetSchema is optional and will be obtained from {@link KafkaTopicSchemaRegistry#getLatestSchemaByTopic(String)}.
 * </p>
 *
 * This decoder uses the following properties :
 * <ul>
 * <li><tt>camus.message.schema.default</tt>: (optional) the schema ID to use when no schema is found in the json body</li>
 * <li><tt>camus.message.timestamp.field</tt>: (default to "timestamp") the of the timestamp field</li>
 * <li><tt>camus.message.timestamp.format</tt>: (default to "unix_milliseconds") the format of the timestamp field (see {@link CamusAvroWrapper})</li>
 * </ul>
 */
public class AvroBinaryMessageDecoder extends MessageDecoder<Message, GenericData.Record> {
    public static final String CAMUS_SCHEMA_DEFAULT = "camus.message.schema.default";

    private static final Logger log = Logger.getLogger(AvroBinaryMessageDecoder.class);

    protected DecoderFactory decoderFactory;
    protected SchemaRegistry<Schema> registry;

    private String timestampField;
    private String timestampFormat;
    private String defaultSchemaId = null;

    @Override
    public void init(Properties props, String topicName) {
        super.init(props, topicName);
        try {
            @SuppressWarnings("unchecked")
            SchemaRegistry<Schema> registry = (SchemaRegistry<Schema>) Class.forName(
                    props.getProperty(KafkaTopicSchemaRegistry.SCHEMA_REGISTRY_CLASS)).newInstance();
            log.info("Prop " + KafkaTopicSchemaRegistry.SCHEMA_REGISTRY_CLASS + " is: "
                    + props.getProperty(KafkaTopicSchemaRegistry.SCHEMA_REGISTRY_CLASS));
            log.info("Underlying schema registry for topic: " + topicName + " is: " + registry);
            registry.init(props);
            this.registry = registry;
            defaultSchemaId = props.getProperty(CAMUS_SCHEMA_DEFAULT);
        } catch (Exception e) {
            throw new MessageDecoderException(e);
        }

        this.timestampField = props.getProperty(CamusAvroWrapper.CAMUS_MESSAGE_TIMESTAMP_FIELD, CamusAvroWrapper.DEFAULT_TIMESTAMP_FIELD);
        this.timestampFormat = props.getProperty(CamusAvroWrapper.CAMUS_MESSAGE_TIMESTAMP_FORMAT, CamusAvroWrapper.DEFAULT_TIMESTAMP_FORMAT);

        decoderFactory = DecoderFactory.get();
    }


    public CamusWrapper<GenericData.Record> decode(Message message) {
        try {
            MessageDecoderHelper helper = new MessageDecoderHelper(message.getPayload());
            Schema writerSchema = helper.getWriterSchema();
            if(writerSchema == null && defaultSchemaId != null) {
                writerSchema = registry.getSchemaByID(topicName, defaultSchemaId);
            }
            if(writerSchema == null) {
                throw new RuntimeException("No schema found for topic "
                        + topicName + ": none provided in the message body. Use "
                        + CAMUS_SCHEMA_DEFAULT + " to force a default schema.");
            }
            SchemaDetails<Schema> targetSchemaDetails = null;
            try {
                targetSchemaDetails = registry.getLatestSchemaByTopic(topicName);
            } catch(SchemaNotFoundException e) {
                // Ignore this error, targetSchema is optional.
            }
            // If a target schema has been specified use it. Use writerSchema otherwize.
            Schema targetSchema = targetSchemaDetails != null ? targetSchemaDetails.getSchema() : writerSchema;

            DatumReader<GenericData.Record> reader = new GenericDatumReader<GenericData.Record>(writerSchema, targetSchema);

            return new CamusAvroWrapper(reader.read(null,
                    decoderFactory.binaryDecoder(helper.getPayload(), helper.getStart(), helper.getLength(), null)),
                    timestampField, timestampFormat);

        } catch (IOException e) {
            throw new MessageDecoderException(e);
        }
    }

    /**
     * Utility class to help decode the schema rev id encoded into the first bytes
     * of the message.
     */
    public class MessageDecoderHelper {
        public final static byte MAGIC = 0x0;
        // Magic + 64bits long
        public final static int SCHEMA_SIZE = 8 + 1;

        private Schema writerSchema;
        private int start;
        private int length;
        private byte[] payload;

        public MessageDecoderHelper(byte[] payload) {
            this.payload = payload;
            init();
        }

        public byte[] getPayload() {
            return payload;
        }

        public int getStart() {
            return start;
        }

        public int getLength() {
            return length;
        }

        public Schema getWriterSchema() {
            return writerSchema;
        }

        private void init() {
            Long rev = getRevision();
            if(rev == null) {
                // the schema has not been encoded in avro binary
                start = 0;
                length = payload.length;
            } else {
                start = SCHEMA_SIZE;
                length = payload.length - SCHEMA_SIZE;
                writerSchema = registry.getSchemaByID(topicName, String.valueOf(rev));
            }
        }

        /**
         * @return the schema rev id or null if not found
         */
        private Long getRevision() {
            // Make sure the payload is large enough to hold
            // the magic byte and the revision id.:w
            if(payload == null || payload.length <= (SCHEMA_SIZE)) {
                return null;
            }

            try(ByteArrayInputStream bais = new ByteArrayInputStream(payload);
                    DataInputStream dis = new DataInputStream(bais)) {
                if(dis.readByte() == MAGIC) {
                    return dis.readLong();
                }
            } catch(IOException ioe) {
                // Should not happen
                throw new RuntimeException(ioe);
            }
            return null;
        }

    }
}
