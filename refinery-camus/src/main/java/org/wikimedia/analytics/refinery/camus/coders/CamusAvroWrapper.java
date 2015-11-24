package org.wikimedia.analytics.refinery.camus.coders;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.etl.kafka.coders.JsonStringMessageDecoder;

/**
 * A CamusWrapper inspired from {@link JsonStringMessageDecoder} which
 * handles timestamps in various formats:
 * <ul>
 *  <li>unix_milliseconds (default)</li>
 *  <li>unix_seconds</li>
 *  <li>ISO-8601 (Using JodaTime)</li>
 *  <li>Anything else will treated as a {@link SimpleDateFormat}</li>
 *  </ul>
 *
 * <p>NOTE: This wrapper will first attempt to extract the timestamp by
 * reading the <tt>time</tt> header field.</p>
 *
 * @author dcausse
 * Created on 2015-11-11
 * @since 0.0.23
 */
public class CamusAvroWrapper extends CamusWrapper<GenericData.Record> {
    private static final Logger LOGGER = Logger.getLogger(CamusAvroWrapper.class);

    public static final String CAMUS_MESSAGE_TIMESTAMP_FIELD = "camus.message.timestamp.field";
    public static final String DEFAULT_TIMESTAMP_FIELD = "timestamp";

    public static final String CAMUS_MESSAGE_TIMESTAMP_FORMAT = "camus.message.timestamp.format";
    public static final String DEFAULT_TIMESTAMP_FORMAT = "unix_milliseconds";

    private final String timestampField;
    private final String timestampFormat;

    /**
     * Builds a new CamusAvroWrapper
     * @param record the Avro record
     * @param timestampField the name of the timestamp field
     * @param timestampFormat the format used to parse the timestamp field value
     */
    public CamusAvroWrapper(GenericData.Record record, String timestampField, String timestampFormat) {
        super(record);
        this.timestampField = timestampField;
        this.timestampFormat = timestampFormat;
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
        long timestamp = 0;
        if (header != null && header.get("time") != null) {
            timestamp = (Long) header.get("time");
        } else if (super.getRecord().get(timestampField) != null) {
            timestamp = convert(super.getRecord().get(timestampField));
        }

        if(timestamp == 0) {
            LOGGER.warn("Couldn't find or parse timestamp field '" + timestampField
                    + "' in message, defaulting to current time.");
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }

    private long convert(Object o) {
        switch(timestampFormat) {
        case "":
        case "unix_milliseconds": return fromNumber(o);
        case "unix_seconds": return fromNumber(o) * 1000;
        case "ISO-8601": return fromISO8691(o);
        default: return fromSimpleDateFormat(o);
        }
    }

    private long fromNumber(Object o) {
        if(!(o instanceof Number)) {
            LOGGER.error("Could not parse timestamp expected Number got " + o.getClass().getName());
            return 0;
        }
        return ((Number) o).longValue();
    }

    private long fromISO8691(Object o) {
        // Avro will use Utf8 instead of String
        // Utf8 implements CharSequence so we use it
        if(!(o instanceof CharSequence)) {
            LOGGER.error("Could not parse timestamp expected String got " + o.getClass().getName());
            return 0;
        }
        try {
            return new DateTime(((CharSequence) o).toString()).getMillis();
        } catch(IllegalArgumentException iae) {
            LOGGER.error("Could not parse timestamp '" + o + "' as ISO-8601 while decoding message.");
        }
        return 0;
    }

    private long fromSimpleDateFormat(Object o) {
        // Avro will use Utf8 instead of String
        // Utf8 implements CharSequence so we use it
        if(!(o instanceof CharSequence)) {
            LOGGER.error("Could not parse timestamp expected String got " + o.getClass().getName());
            return 0;
        }
        // We'll build the formatter for each message...
        // This is not ideal but this is what is done in JsonStringMessageDecoder
        // from camus-kafka-coders. Not sure also where to store a shared
        // instance of SimpleDateFormat as it's not thread-safe.
        SimpleDateFormat sdf;
        try {
            sdf = new SimpleDateFormat(timestampFormat);
        } catch(IllegalArgumentException iae) {
            LOGGER.error("Could not parse timestamp format '" + timestampFormat + "'" + iae.getMessage());
            return 0;
        }
        try {
            return sdf.parse(((CharSequence) o).toString()).getTime();
        } catch (ParseException e) {
            LOGGER.error("Could not parse timestamp '" + o + "'");
        }
        return 0;
    }
}
