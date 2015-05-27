package com.cloudera.integration.oracle.goldengate.ldv.flume;

import com.cloudera.integration.oracle.goldengate.ldv.common.LDVFactory;
import com.cloudera.integration.oracle.goldengate.ldv.common.LDVReader;
import com.cloudera.integration.oracle.goldengate.ldv.common.Message;
import com.cloudera.integration.oracle.goldengate.ldv.common.Settings;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.ResettableInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by jcustenborder on 4/27/15.
 */
public class LDVEventDeserializer implements EventDeserializer {
    public static class Builder implements EventDeserializer.Builder {

        public static final String RECORD_LENGTH = "record.length";
        public static final String RECORD_LENGTH_ENCODING = "record.length.encoding";
        public static final String FIELD_LENGTH = "field.length";
        public static final String FIELD_LENGTH_ENCODING="field.length.encoding";
        public static final String METADATA_COLUMNS="metadata.columns";
        public static final String HEADERS="headers.";


        Settings createSettings(Context context){
            Preconditions.checkNotNull(context, "context should not be null.");

            Settings settings = new Settings();

            Preconditions.checkState(context.containsKey(RECORD_LENGTH), "%s must be configured. Valid values are 2, 4, or 8.");
            settings.setRecordLength(context.getInteger(RECORD_LENGTH));
            Preconditions.checkState(context.containsKey(FIELD_LENGTH), "%s must be configured. Valid values are 2, 4, or 8.");
            settings.setFieldLength(context.getInteger(FIELD_LENGTH));

            Preconditions.checkState(context.containsKey(RECORD_LENGTH_ENCODING), "%s must be configured. Valid values are %s", RECORD_LENGTH_ENCODING, Joiner.on(", ").join(Settings.LengthEncoding.values()));
            String lengthEncodingString = context.getString(RECORD_LENGTH_ENCODING);
            Settings.LengthEncoding lengthEncoding = Settings.LengthEncoding.valueOf(lengthEncodingString);
            settings.setRecordLengthEncoding(lengthEncoding);

            Preconditions.checkState(context.containsKey(FIELD_LENGTH_ENCODING), "%s must be configured. Valid values are %s", FIELD_LENGTH_ENCODING, Joiner.on(", ").join(Settings.LengthEncoding.values()));
            lengthEncodingString = context.getString(FIELD_LENGTH_ENCODING);
            lengthEncoding = Settings.LengthEncoding.valueOf(lengthEncodingString);
            settings.setFieldLengthEncoding(lengthEncoding);

            String metadataColumnsString = context.getString(METADATA_COLUMNS);

            if(null!=metadataColumnsString && !metadataColumnsString.isEmpty()){
                String[] metadataColumns = metadataColumnsString.split("\\s*,\\s*");
                settings.setMetadataColumns(metadataColumns);
            }

            return settings;
        }

        @Override
        public EventDeserializer build(Context context, ResettableInputStream resettableInputStream) {
            Settings settings = createSettings(context);
            LDVFactory factory = new LDVFactory(settings);
            ResettableInputStreamWrapper wrapper = new ResettableInputStreamWrapper(resettableInputStream);
            LDVReader reader = factory.openReader(wrapper);

            Map<String, String> additionalHeaders = context.getSubProperties(Builder.HEADERS);
            boolean lowerCaseSchemaHeader=context.getBoolean("lowercase.header.schema", true);
            boolean lowerCaseTableHeader=context.getBoolean("lowercase.header.table", true);
            int bufferSize = context.getInteger("buffer.size", 256 * 1024);

            LDVEventBuilder eventBuilder = new LDVEventBuilder(additionalHeaders,
                    bufferSize,
                    lowerCaseSchemaHeader,
                    lowerCaseTableHeader);

            return new LDVEventDeserializer(resettableInputStream, reader, eventBuilder);
        }
    }

    private final ResettableInputStream resettableInputStream;
    private final LDVEventBuilder eventBuilder;
    private final LDVReader reader;

    /**
     *
     * @param resettableInputStream Input stream to read from.
     * @param reader LDVReader to read from
     * @param eventBuilder
     */
    LDVEventDeserializer(
            ResettableInputStream resettableInputStream,
            LDVReader reader,
            LDVEventBuilder eventBuilder){
        this.resettableInputStream = resettableInputStream;
        this.reader = reader;
        this.eventBuilder = eventBuilder;
    }

    @Override
    public Event readEvent() throws IOException {
        if(!reader.hasNext()){
            return null;
        }

        Message message = reader.next();
        Event event = this.eventBuilder.createEvent(message);
        return event;
    }

    @Override
    public List<Event> readEvents(final int count) throws IOException {
        List<Event> events = new ArrayList<>();

        Event event;
        while((event=readEvent())!=null && events.size() < count){
            events.add(event);
        }

        return events;
    }

    @Override
    public void mark() throws IOException {
        this.resettableInputStream.mark();
    }

    @Override
    public void reset() throws IOException {
        this.resettableInputStream.reset();
    }

    @Override
    public void close() throws IOException {
        this.resettableInputStream.close();
    }
}
