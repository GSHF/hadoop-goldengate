package com.cloudera.integration.oracle.goldengate.ldv.common;

import com.cloudera.integration.oracle.goldengate.ldv.common.io.LengthFormatter;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import org.apache.commons.compress.utils.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Class is used to read LDV file generated by Oracle GoldenGate
 */
public class LDVReader implements AutoCloseable, Iterator<com.cloudera.integration.oracle.goldengate.ldv.common.Message> {
    private static final Charset charset_UTF8 = Charset.forName("UTF-8");
    private final InputStream inputStream;
    private final Settings settings;
    private final Map<Character, FieldFlag> fieldFlagLookup;
    private final LengthFormatter fieldLengthFormatter;
    private final LengthFormatter recordLengthFormatter;
    private Message message;


    LDVReader(InputStream inputStream, Settings settings) {
        Preconditions.checkNotNull(inputStream, "inputStream cannot be null");
        Preconditions.checkNotNull(settings, "settings cannot be null.");

        this.inputStream = inputStream;
        this.settings = settings;
        this.fieldFlagLookup = settings.getCharToFieldFlagLookup();
        this.fieldLengthFormatter = this.settings.getFieldLengthFormatter();
        this.recordLengthFormatter = this.settings.getRecordLengthFormatter();
    }


    @Override
    public void close() throws Exception {
        inputStream.close();
    }

    @Override
    public boolean hasNext() {
        int recordLength;

        try {
            recordLength = this.recordLengthFormatter.readLength(this.inputStream);

            if(recordLength==-1){
                return false;
            }
        } catch(IOException ex){
            return false;
        }

        byte[] recordBuffer = new byte[recordLength];
        try {
            ByteStreams.readFully(this.inputStream, recordBuffer);
            parseRecord(recordBuffer);
            return true;
        } catch(IOException ex){
            return false;
        }
    }


    private String readString(InputStream input) throws IOException {
        int fieldLength = this.fieldLengthFormatter.readLength(input);
        byte[] fieldBuffer = new byte[fieldLength];
        ByteStreams.readFully(input, fieldBuffer);
        return new String(fieldBuffer, charset_UTF8);
    }



    private void parseRecord(byte[] recordData) throws IOException {
        List<FieldValue> fields = new ArrayList<>();
        Map<CharSequence, CharSequence> metadata = new HashMap<>();

        try (ByteArrayInputStream input = new ByteArrayInputStream(recordData)) {
            for(String metadataFieldName: this.settings.metadataColumns){
                String value = readString(input);
                metadata.put(metadataFieldName, value);
            }

            int flagByte;
            while ((flagByte=input.read())!=-1) {
                Character flag = (char) flagByte;

                FieldValue.Builder fieldValueBuilder = FieldValue.newBuilder();
                FieldFlag fieldFlag = this.fieldFlagLookup.get(flag);
                Preconditions.checkNotNull(fieldFlag, "Could not find fieldflag for '%s'", flag);
                fieldValueBuilder.setFlag(fieldFlag);

                if(FieldFlag.PRESENT == fieldFlag) {
                    String data = readString(input);
                    fieldValueBuilder.setValue(data);
                } else {
                    int fieldLength = this.fieldLengthFormatter.readLength(input);
                    Preconditions.checkState(0==fieldLength, "Expected length of 0 bytes, found %s instead", fieldLength);
                    fieldValueBuilder.setValue(null);
                }

                FieldValue fieldValue = fieldValueBuilder.build();
                fields.add(fieldValue);
            }
        }

        Message.Builder messageBuilder = Message.newBuilder();
        messageBuilder.setMetadata(metadata);
        messageBuilder.setValues(fields);
        this.message = messageBuilder.build();
    }

    @Override
    public Message next() {
        return this.message;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove is not supported");
    }


}
