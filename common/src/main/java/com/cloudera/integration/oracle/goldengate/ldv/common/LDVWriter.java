package com.cloudera.integration.oracle.goldengate.ldv.common;

import com.cloudera.integration.oracle.goldengate.ldv.common.io.LengthFormatter;
import com.google.common.base.Preconditions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * Created by jcustenborder on 4/24/15.
 */
public class LDVWriter implements AutoCloseable {
    private static final Charset charset_UTF8 = Charset.forName("UTF-8");
    private final Settings settings;
    private final OutputStream outputStream;
    private final Map<FieldFlag, Integer> fieldFlagToCharLookup;
    private final ByteArrayOutputStream recordOutputStream;
    private final ByteArrayOutputStream fieldOutputStream;
    private final LengthFormatter fieldLengthFormatter;
    private final LengthFormatter recordLengthFormatter;


    LDVWriter(OutputStream outputStream, Settings settings){
        Preconditions.checkNotNull(settings, "settings cannot be null.");
        Preconditions.checkNotNull(outputStream, "outputStream cannot be null.");

        this.settings = settings;
        this.outputStream = outputStream;
        this.fieldFlagToCharLookup = this.settings.getFieldFlagToCharLookup();
        this.fieldLengthFormatter = this.settings.getFieldLengthFormatter();
        this.recordLengthFormatter = this.settings.getRecordLengthFormatter();
        this.recordOutputStream = new ByteArrayOutputStream(2048);
        this.fieldOutputStream  = new ByteArrayOutputStream(128);
    }

    void writeString(boolean metadata, String value, FieldFlag flag) throws IOException {
        this.fieldOutputStream.reset();

        if(!metadata){
            Integer flagValue = this.fieldFlagToCharLookup.get(flag);
            this.fieldOutputStream.write(flagValue);
        }

        if(FieldFlag.PRESENT == flag){
            byte[] buffer = value.getBytes(charset_UTF8);
            this.fieldLengthFormatter.writeLength(this.fieldOutputStream, buffer.length);
            this.fieldOutputStream.write(buffer);
        } else {
            this.fieldLengthFormatter.writeLength(this.fieldOutputStream, 0);
        }

        this.fieldOutputStream.writeTo(this.recordOutputStream);

    }

    public void write(Message message) throws IOException {
        this.recordOutputStream.reset();
        try(ByteArrayOutputStream recordStream = new ByteArrayOutputStream()){
            Map<CharSequence, CharSequence> metadata = message.getMetadata();
            for(String metadataColumn:this.settings.metadataColumns){
                CharSequence value = metadata.get(metadataColumn);
                writeString(true, (String)value, FieldFlag.PRESENT);
            }
            for(FieldValue fieldValue: message.getValues()){
                writeString(false, (String)fieldValue.getValue(), fieldValue.getFlag());
            }
            byte[] buffer = this.recordOutputStream.toByteArray();
            this.recordLengthFormatter.writeLength(this.outputStream, buffer.length);
            this.outputStream.write(buffer);
        }
    }

    @Override
    public void close() throws Exception {
        this.outputStream.close();
    }
}
