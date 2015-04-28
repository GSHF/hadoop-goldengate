package com.cloudera.integration.oracle.goldengate.ldv.common;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.sql.Timestamp;
import java.util.*;

/**
 * Created by jcustenborder on 4/24/15.
 */
public class LDVReaderWriterTest {
    static Random random = new Random();

    static String getRandomString(int length){
        StringBuilder builder = new StringBuilder();
        final String chars = "abcdefghijklmnopqrstuvwxyz";

        for(int i=0;i<length;i++){
            int charPos = random.nextInt(chars.length());
            char c = chars.charAt(charPos);
            builder.append(c);
        }

        return builder.toString();
    }

    Message[] buildTestDataset() {
        Message[] messages = new Message[50];
        for(int i=0;i<messages.length;i++){
            Timestamp timestamp = new Timestamp(new Date().getTime());
            final String schema = "test_schema";
            final String table ="test_table";

            Map<CharSequence, CharSequence> metadata = new HashMap<>();
            metadata.put("timestamp", timestamp.toString());
            metadata.put("schema", schema);
            metadata.put("table", table);

            Message.Builder messageBuilder = Message.newBuilder();
            messageBuilder.setMetadata(metadata);
            List<FieldValue> fieldValues = new ArrayList<>(10);

            for(int j=0;j<10;j++){
                int length = random.nextInt(50) + 1;
                int mod = length % 6;

                FieldValue.Builder fieldValueBuilder = FieldValue.newBuilder();

                switch (mod){
                    case 0:
                        fieldValueBuilder.setFlag(FieldFlag.MISSING);
                        break;
                    case 1:
                        fieldValueBuilder.setFlag(FieldFlag.NULL);
                        break;
                    default:
                        fieldValueBuilder.setFlag(FieldFlag.PRESENT);
                        break;
                }

                if(FieldFlag.PRESENT == fieldValueBuilder.getFlag()){
                    String randomString = getRandomString(length);
                    fieldValueBuilder.setValue(randomString);
                } else {
                    fieldValueBuilder.setValue(null);
                }

                FieldValue fieldValue = fieldValueBuilder.build();
                fieldValues.add(fieldValue);
            }

            messageBuilder.setValues(fieldValues);
            Message message = messageBuilder.build();
            messages[i]=message;
        }
        return messages;
    }

//    @Test
    public void foo() throws Exception {
        Message[] expected = buildTestDataset();

        Settings settings = new Settings();
        settings.metadataColumns = new String[]{"timestamp", "schema", "table"};
        settings.recordLength = 8;
        settings.recordLengthEncoding = Settings.LengthEncoding.ASCII;
        settings.fieldLength = 8;
        settings.fieldLengthEncoding = Settings.LengthEncoding.ASCII;

        LDVFactory factory = new LDVFactory(settings);

        byte[] buffer;
        try(ByteArrayOutputStream outputStream = new ByteArrayOutputStream()){
            try(LDVWriter writer = factory.openWriter(outputStream)){
                for(Message message:expected){
                    writer.write(message);
                }
            }
            buffer = outputStream.toByteArray();
        }

        Message[] actual = new Message[50];

        int i=0;
        try(ByteArrayInputStream inputStream = new ByteArrayInputStream(buffer)){
            try(LDVReader reader = factory.openReader(inputStream)){
                while(reader.hasNext()){
                    actual[i++] = reader.next();
                }
            }
        }
    }
}
