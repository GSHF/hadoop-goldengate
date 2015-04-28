package com.cloudera.integration.oracle.goldengate.ldv.common;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jcustenborder on 4/27/15.
 */
public class LDVReaderTest {

    static void addFieldValue(Message.Builder messageBuilder, String value, FieldFlag flag) {
        FieldValue.Builder fieldValueBuilder = FieldValue.newBuilder();
        fieldValueBuilder.setValue(value);
        fieldValueBuilder.setFlag(flag);
        FieldValue fieldValue = fieldValueBuilder.build();
        messageBuilder.getValues().add(fieldValue);
    }

    static Message[] expected;

    @BeforeClass
    public static void setupClass(){
        Message[] messages = new Message[7];
        List<FieldValue> fieldValues = new ArrayList<>();
        Map<CharSequence, CharSequence> headers = new HashMap<>();
        headers.put("opcode", "INS");
        headers.put("timestamp", "2015-04-27 19:18:33.696069");

        Message.Builder messageBuilder = null;

        messageBuilder = Message.newBuilder();
        messageBuilder.setMetadata(headers);
        messageBuilder.setValues(new ArrayList<FieldValue>());
        addFieldValue(messageBuilder, "1", FieldFlag.PRESENT);
        addFieldValue(messageBuilder, "Red  ", FieldFlag.PRESENT);
        addFieldValue(messageBuilder, "1", FieldFlag.PRESENT);
        messages[0] = messageBuilder.build();

        messageBuilder = Message.newBuilder();
        messageBuilder.setMetadata(headers);
        messageBuilder.setValues(new ArrayList<FieldValue>());
        addFieldValue(messageBuilder, "2", FieldFlag.PRESENT);
        addFieldValue(messageBuilder, "Green", FieldFlag.PRESENT);
        addFieldValue(messageBuilder, "2", FieldFlag.PRESENT);
        messages[1] = messageBuilder.build();

        messageBuilder = Message.newBuilder();
        messageBuilder.setMetadata(headers);
        messageBuilder.setValues(new ArrayList<FieldValue>());
        addFieldValue(messageBuilder, "3", FieldFlag.PRESENT);
        addFieldValue(messageBuilder, "Blue ", FieldFlag.PRESENT);
        addFieldValue(messageBuilder, null, FieldFlag.NULL);
        messages[2] = messageBuilder.build();

        messageBuilder = Message.newBuilder();
        messageBuilder.setMetadata(headers);
        messageBuilder.setValues(new ArrayList<FieldValue>());
        addFieldValue(messageBuilder, "4", FieldFlag.PRESENT);
        addFieldValue(messageBuilder, "Black", FieldFlag.PRESENT);
        addFieldValue(messageBuilder, null, FieldFlag.NULL);
        messages[3] = messageBuilder.build();

        messageBuilder = Message.newBuilder();
        messageBuilder.setMetadata(headers);
        messageBuilder.setValues(new ArrayList<FieldValue>());
        addFieldValue(messageBuilder, "5", FieldFlag.PRESENT);
        addFieldValue(messageBuilder, null, FieldFlag.NULL);
        addFieldValue(messageBuilder, "5", FieldFlag.PRESENT);
        messages[4] = messageBuilder.build();

        messageBuilder = Message.newBuilder();
        messageBuilder.setMetadata(headers);
        messageBuilder.setValues(new ArrayList<FieldValue>());
        addFieldValue(messageBuilder, "6", FieldFlag.PRESENT);
        addFieldValue(messageBuilder, null, FieldFlag.NULL);
        addFieldValue(messageBuilder, "6", FieldFlag.PRESENT);
        messages[5] = messageBuilder.build();

        messageBuilder = Message.newBuilder();
        messageBuilder.setMetadata(headers);
        messageBuilder.setValues(new ArrayList<FieldValue>());
        addFieldValue(messageBuilder, "7", FieldFlag.PRESENT);
        addFieldValue(messageBuilder, null, FieldFlag.NULL);
        addFieldValue(messageBuilder, null, FieldFlag.NULL);
        messages[6] = messageBuilder.build();

        expected = messages;
    }


    @Test
    public void test() throws IOException{

        Settings settings = new Settings();
        settings.fieldLengthEncoding = Settings.LengthEncoding.ASCII;
        settings.fieldLength = 8;
        settings.recordLengthEncoding = Settings.LengthEncoding.ASCII;
        settings.recordLength = 8;
        settings.metadataColumns = new String[]{"opcode", "timestamp"};

        LDVFactory factory = new LDVFactory(settings);

        List<Message> messages = new ArrayList<>();

        try(InputStream resourceAsStream = LDVReaderTest.class.getResourceAsStream("example.ldv")){
            LDVReader reader = factory.openReader(resourceAsStream);

            while(reader.hasNext()){
                Message message = reader.next();
                messages.add(message);
            }
        }

        Assert.assertEquals("messages should not be empty.", false, messages.isEmpty());
        Assert.assertEquals("messages count does not match", 7, messages.size());
        Message[] actual = messages.toArray(new Message[messages.size()]);
        Assert.assertArrayEquals(expected, actual);
    }
}
