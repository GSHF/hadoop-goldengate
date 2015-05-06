package com.cloudera.integration.oracle.goldengate.ldv.flume;

import com.cloudera.integration.oracle.goldengate.ldv.common.Message;
import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.AbstractAvroEventSerializer;
import org.apache.flume.serialization.EventSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by jcustenborder on 5/6/15.
 */
public class MessageAvroEventSerializer extends AbstractAvroEventSerializer<Message> {
    private final OutputStream outputStream;
    private BinaryDecoder binaryDecoder = null;

    private MessageAvroEventSerializer(OutputStream outputStream){
        this.outputStream = outputStream;
    }

    @Override
    protected OutputStream getOutputStream() {
        return this.outputStream;
    }

    @Override
    protected Schema getSchema() {
        return Message.SCHEMA$;
    }

    Message message = null;
    @Override
    protected Message convert(Event event) {
        byte[] buffer = event.getBody();
        this.binaryDecoder = DecoderFactory.get().binaryDecoder(buffer, this.binaryDecoder);

        SpecificDatumReader<Message> datumReader = new SpecificDatumReader<>(Message.SCHEMA$);
        try {
            message = datumReader.read(this.message, this.binaryDecoder);
            return message;
        } catch(IOException ex){
            throw new RuntimeException("Exception thrown", ex);
        }
    }

    public static class Builder implements EventSerializer.Builder {

        @Override
        public EventSerializer build(Context context, OutputStream out) {
            MessageAvroEventSerializer writer = new MessageAvroEventSerializer(out);
            writer.configure(context);
            return writer;
        }

    }

}
