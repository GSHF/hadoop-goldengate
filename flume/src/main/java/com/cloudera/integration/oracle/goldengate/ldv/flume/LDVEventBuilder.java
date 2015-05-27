package com.cloudera.integration.oracle.goldengate.ldv.flume;

import com.cloudera.integration.oracle.goldengate.ldv.common.Message;
import com.google.common.base.Preconditions;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jcustenborder on 4/28/15.
 */
class LDVEventBuilder {
    static final String SCHEMA_LITERAL="flume.avro.schema.literal";
    private final Map<String, String> baseHeaders;
    private final ByteArrayOutputStream outputStream;
    private final BinaryEncoder encoder;
    private final SpecificDatumWriter<Message> datumWriter;
    private final boolean lowerCaseSchemaHeader;
    private final boolean lowerCaseTableHeader;

    public LDVEventBuilder(Map<String, String> headers,
                           int bufferSize,
                           boolean lowerCaseSchemaHeader,
                           boolean lowerCaseTableHeader){
        this.lowerCaseSchemaHeader = lowerCaseSchemaHeader;
        this.lowerCaseTableHeader = lowerCaseTableHeader;
        this.baseHeaders = new HashMap<>();

        if(null!=headers) {
            this.baseHeaders.putAll(headers);
        }

        this.outputStream = new ByteArrayOutputStream(bufferSize);
        this.encoder = EncoderFactory.get().directBinaryEncoder(this.outputStream, null);
        this.datumWriter = new SpecificDatumWriter<>(Message.SCHEMA$);
        String schema = Message.SCHEMA$.toString(false);
        this.baseHeaders.put(LDVEventBuilder.SCHEMA_LITERAL, schema);
    }

    static final String HEADER_SCHEMA="schema";
    static final String HEADER_TABLE="table";

    /**
     * Method is used to create a Flume event from a LDV message.
     * @param message
     * @return
     */
    public Event createEvent(Message message) throws IOException {
        Preconditions.checkNotNull(message, "message cannot be null");
        outputStream.reset();

        Map<String, String> headers =  new HashMap<>();
        headers.putAll(this.baseHeaders);
        for(Map.Entry<CharSequence, CharSequence> kvp:message.getMetadata().entrySet()){
            String key = (String)kvp.getKey();
            String value = (String)kvp.getValue();

            /*
            We want to downcase the name of the schema and table if configured to do so.
             */
            if(lowerCaseSchemaHeader && HEADER_SCHEMA.equalsIgnoreCase(key)){
                key = HEADER_SCHEMA;
                value = value.toLowerCase();
            } else if(lowerCaseTableHeader && HEADER_TABLE.equalsIgnoreCase(key)){
                key = HEADER_TABLE;
                value = value.toLowerCase();
            }

            headers.put(key, value);
        }

        this.datumWriter.write(message, this.encoder);
        byte[] body = this.outputStream.toByteArray();
        return EventBuilder.withBody(body, headers);
    }
}
