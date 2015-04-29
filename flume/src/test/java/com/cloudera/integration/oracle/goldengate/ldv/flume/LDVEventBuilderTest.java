package com.cloudera.integration.oracle.goldengate.ldv.flume;

import com.cloudera.integration.oracle.goldengate.ldv.common.FieldFlag;
import com.cloudera.integration.oracle.goldengate.ldv.common.FieldValue;
import com.cloudera.integration.oracle.goldengate.ldv.common.Message;
import org.apache.flume.Event;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jcustenborder on 4/28/15.
 */
public class LDVEventBuilderTest {
    static Message message;

    static void addFieldValue(Message.Builder messageBuilder, String value, FieldFlag flag) {
        FieldValue.Builder fieldValueBuilder = FieldValue.newBuilder();
        fieldValueBuilder.setValue(value);
        fieldValueBuilder.setFlag(flag);
        FieldValue fieldValue = fieldValueBuilder.build();
        messageBuilder.getValues().add(fieldValue);
    }

    @BeforeClass
    public static void setupClass(){
        Map<CharSequence, CharSequence> headers = new HashMap<>();
        headers.put("opcode", "INS");
        headers.put("timestamp", "2015-04-27 19:18:33.696069");

        Message.Builder messageBuilder  = Message.newBuilder();
        messageBuilder.setMetadata(headers);
        messageBuilder.setValues(new ArrayList<FieldValue>());
        addFieldValue(messageBuilder, "1", FieldFlag.PRESENT);
        addFieldValue(messageBuilder, "Red  ", FieldFlag.PRESENT);
        addFieldValue(messageBuilder, "1", FieldFlag.PRESENT);
        message = messageBuilder.build();
    }

    LDVEventBuilder eventBuilder;

    @Before
    public void setup(){
        Map<String, String> headers = new HashMap<>();
        this.eventBuilder = new LDVEventBuilder(headers);
    }

    @Test
    public void hasLiteralHeader() throws IOException {
        Event event = this.eventBuilder.createEvent(message);
        Assert.assertNotNull("event should not be null", event);
        Assert.assertTrue("Event should contain header for " + LDVEventBuilder.SCHEMA_LITERAL, event.getHeaders().containsKey(LDVEventBuilder.SCHEMA_LITERAL));
    }
}
