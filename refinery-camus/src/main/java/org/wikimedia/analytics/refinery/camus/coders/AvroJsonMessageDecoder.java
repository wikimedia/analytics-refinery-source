package org.wikimedia.analytics.refinery.camus.coders;

import com.google.common.base.Charsets;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.Message;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;
import com.linkedin.camus.schemaregistry.SchemaDetails;
import com.linkedin.camus.schemaregistry.SchemaNotFoundException;
import com.linkedin.camus.schemaregistry.SchemaRegistry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.wikimedia.analytics.refinery.camus.schemaregistry.KafkaTopicSchemaRegistry;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by mviswanathan on 10/6/15.
 * <p>
 * This Decoder helps to decode JSON encoded Avro messages that are read from
 * Kafka, and validate them against a schema with the help of a SchemaRegistry,
 * and produce Avro records of type GenericData.Record.
 * </p>
 *
 * <p>
 * The writerSchema is mandatory and will be obtained from a field in the JSON body.
 * The targetSchema is optional and will be obtained from
 * {@link KafkaTopicSchemaRegistry#getLatestSchemaByTopic(String)}.
 * </p>
 *
 * This decoder uses the following properties :
 * <ul>
 * <li><tt>camus.message.schema.id.field</tt>: (required) the name of the
 * JSON field where the schema id can be found</li>
 * <li><tt>camus.message.schema.default</tt>: (optional) the schema ID
 * to use when no schema is found in the json body</li>
 * <li><tt>camus.message.timestamp.field</tt>: (default to "timestamp")
 * the of the timestamp field</li>
 * <li><tt>camus.message.timestamp.format</tt>: (default to "unix_milliseconds")
 * the format of the timestamp field (see {@link CamusAvroWrapper})</li>
 * </ul>
 *
 */
public class AvroJsonMessageDecoder extends MessageDecoder<Message, GenericData.Record> {
    public static final String CAMUS_SCHEMA_ID_FIELD = "camus.message.schema.id.field";
    public static final String CAMUS_SCHEMA_DEFAULT = "camus.message.schema.default";

    public static final String DEFAULT_SCHEMA_ID_FIELD = "schemaID";

    private static final Logger log = Logger.getLogger(AvroJsonMessageDecoder.class);
    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    protected DecoderFactory decoderFactory;
    protected SchemaRegistry<Schema> registry;

    private String schemaIDField;
    private String timestampField;
    private String timestampFormat;
    private String defaultSchemaId = null;

    public AvroJsonMessageDecoder() {
    }

    public void init(Properties props, String topicName) {
        super.init(props, topicName);
        this.props = props;
        this.topicName = topicName;
        this.schemaIDField = props.getProperty(CAMUS_SCHEMA_ID_FIELD, DEFAULT_SCHEMA_ID_FIELD);

        try {
            @SuppressWarnings("unchecked")
            SchemaRegistry<Schema> registry = (SchemaRegistry<Schema>) Class.forName(
                    props.getProperty(KafkaTopicSchemaRegistry.SCHEMA_REGISTRY_CLASS)).newInstance();
            log.info("Prop " + KafkaTopicSchemaRegistry.SCHEMA_REGISTRY_CLASS + " is: "
                    + props.getProperty(KafkaTopicSchemaRegistry.SCHEMA_REGISTRY_CLASS));
            log.info("Underlying schema registry for topic: " + topicName + " is: " + registry);
            registry.init(props);

            defaultSchemaId = props.getProperty(CAMUS_SCHEMA_DEFAULT);
            this.registry = registry;
        } catch (Exception e) {
            throw new MessageDecoderException(e);
        }

        this.decoderFactory = DecoderFactory.get();

        this.timestampField = props.getProperty(CamusAvroWrapper.CAMUS_MESSAGE_TIMESTAMP_FIELD, CamusAvroWrapper.DEFAULT_TIMESTAMP_FIELD);
        this.timestampFormat = props.getProperty(CamusAvroWrapper.CAMUS_MESSAGE_TIMESTAMP_FORMAT, CamusAvroWrapper.DEFAULT_TIMESTAMP_FORMAT);
    }

    @Override
    public CamusWrapper<GenericData.Record> decode(Message message) {
        Schema writerSchema = getProducerSchema(message.getPayload());
        SchemaDetails<Schema> targetSchemaDetails = null;
        try {
            targetSchemaDetails = registry.getLatestSchemaByTopic(topicName);
        } catch(SchemaNotFoundException e) {
            // Ignore this error, targetSchema is optional.
        }
        // If a target schema has been specified use it. Use writerSchema otherwise.
        Schema targetSchema = targetSchemaDetails != null ? targetSchemaDetails.getSchema() : writerSchema;
        try {
            DatumReader<GenericData.Record> reader = new GenericDatumReader<GenericData.Record>(writerSchema, targetSchema);
            InputStream inStream = new ByteArrayInputStream(message.getPayload(), 0, message.getPayload().length);
            JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(writerSchema, inStream);

            return new CamusAvroWrapper(reader.read(null, jsonDecoder),
                    timestampField, timestampFormat);
        } catch (RuntimeException e) {
            log.error("Caught exception while parsing JSON string '" + new String(message.getPayload(), 0, message.getPayload().length, Charsets.UTF_8) + "'.");
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new MessageDecoderException(e);
        }
    }

    /**
     * We need to do a first pass to extract the schema ID
     * @param payload
     * @return the schema ID encoded in the json string or
     */
    private Schema getProducerSchema(byte[] payload) {
        // We use Jackson because:
        // faster because it's a stream
        // autodetect encoding.
        String schemaId = null;
        try(JsonParser parser = JSON_FACTORY.createJsonParser(payload)) {
            while(!parser.isClosed()) {
                JsonToken token = parser.nextToken();
                if( token == null ) {
                    break;
                }
                if(token == JsonToken.FIELD_NAME && schemaIDField.equals(parser.getCurrentName())) {
                    token = parser.nextToken();
                    if(token == JsonToken.VALUE_NUMBER_INT) {
                        schemaId = String.valueOf(parser.getLongValue());
                        break;
                    } else {
                        log.warn("Schema (topic: " + topicName +") ID field" + schemaIDField
                                + ": expected a number got " + token);
                        break;
                    }
                }
            }
        } catch (IOException e) {
            log.warn("Parse error while extracting schema id from json body", e);
        }
        if(schemaId == null && defaultSchemaId != null) {
            schemaId = defaultSchemaId;
        }
        if(schemaId == null) {
            throw new RuntimeException("No schema found for topic "
                    + topicName + ": none provided in the json body. Use "
                    + CAMUS_SCHEMA_DEFAULT + " to force a default schema.");
        }
        return registry.getSchemaByID(topicName, schemaId);
    }

}
