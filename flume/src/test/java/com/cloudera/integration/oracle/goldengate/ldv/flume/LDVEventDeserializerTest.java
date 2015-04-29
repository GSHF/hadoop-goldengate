package com.cloudera.integration.oracle.goldengate.ldv.flume;

import com.cloudera.integration.oracle.goldengate.ldv.common.LDVFactory;
import com.cloudera.integration.oracle.goldengate.ldv.common.LDVWriter;
import com.cloudera.integration.oracle.goldengate.ldv.common.Message;
import com.cloudera.integration.oracle.goldengate.ldv.common.Settings;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.DurablePositionTracker;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.ResettableFileInputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jcustenborder on 4/28/15.
 */
public class LDVEventDeserializerTest {

    Map<String, String> params;
    LDVEventDeserializer.Builder builder;
    Settings settings;
    LDVFactory factory;
    Message[] expected;
    ResettableFileInputStream resettableFileInputStream;

    @Before
    public void setup() throws Exception{
        settings = new Settings();
        settings.setFieldLengthEncoding(Settings.LengthEncoding.ASCII);
        settings.setFieldLength(8);
        settings.setRecordLengthEncoding(Settings.LengthEncoding.ASCII);
        settings.setRecordLength(8);
        settings.setMetadataColumns(new String[]{"opcode", "timestamp"});

        factory = new LDVFactory(settings);

        params = new HashMap<>();
        params.put(LDVEventDeserializer.Builder.RECORD_LENGTH, "8");
        params.put(LDVEventDeserializer.Builder.RECORD_LENGTH_ENCODING, "ASCII");
        params.put(LDVEventDeserializer.Builder.FIELD_LENGTH, "8");
        params.put(LDVEventDeserializer.Builder.FIELD_LENGTH_ENCODING, "ASCII");
        params.put(LDVEventDeserializer.Builder.METADATA_COLUMNS, "opcode,timestamp");

        builder = new LDVEventDeserializer.Builder();

        File ldvFile = File.createTempFile("dataFile", "ldv");
        ldvFile.deleteOnExit();

        expected = TestDataHelper.getTestMessages();

        try (FileOutputStream fileOutputStream = new FileOutputStream(ldvFile)) {
            try (LDVWriter writer = factory.openWriter(fileOutputStream)) {
                for (Message message : expected)
                    writer.write(message);
            }
        }


        File positionFile = File.createTempFile("position", "state");
        positionFile.delete();
        DurablePositionTracker positionTracker = DurablePositionTracker.getInstance(positionFile, "target");
        positionFile.deleteOnExit();
        resettableFileInputStream = new ResettableFileInputStream(ldvFile, positionTracker);
    }

    @Test(expected = IllegalStateException.class)
    public void createSettings_Missing_Record_Length() {
        params.remove(LDVEventDeserializer.Builder.RECORD_LENGTH);
        Context context = new Context(params);
        builder.createSettings(context);
    }

    @Test(expected = IllegalStateException.class)
    public void createSettings_Missing_Record_Length_Encoding() {
        params.remove(LDVEventDeserializer.Builder.RECORD_LENGTH_ENCODING);
        Context context = new Context(params);
        builder.createSettings(context);
    }

    @Test(expected = IllegalStateException.class)
    public void createSettings_Missing_Field_Length() {
        params.remove(LDVEventDeserializer.Builder.FIELD_LENGTH);
        Context context = new Context(params);
        builder.createSettings(context);
    }

    @Test(expected = IllegalStateException.class)
    public void createSettings_Missing_Field_Length_Encoding() {
        params.remove(LDVEventDeserializer.Builder.FIELD_LENGTH_ENCODING);
        Context context = new Context(params);
        builder.createSettings(context);
    }

    @Test
    public void createSettings() {
        Context context = new Context(params);
        Settings settings = builder.createSettings(context);
        Assert.assertNotNull("Settings should not be null.", settings);
        Assert.assertEquals(LDVEventDeserializer.Builder.RECORD_LENGTH + " does not match", 8, settings.getRecordLength());
        Assert.assertEquals(LDVEventDeserializer.Builder.RECORD_LENGTH_ENCODING + " does not match", Settings.LengthEncoding.ASCII, settings.getRecordLengthEncoding());
        Assert.assertEquals(LDVEventDeserializer.Builder.FIELD_LENGTH + " does not match", 8, settings.getFieldLength());
        Assert.assertEquals(LDVEventDeserializer.Builder.FIELD_LENGTH_ENCODING + " does not match", Settings.LengthEncoding.ASCII, settings.getFieldLengthEncoding());
        Assert.assertArrayEquals(LDVEventDeserializer.Builder.METADATA_COLUMNS + " does not match", new String[]{"opcode", "timestamp"}, settings.getMetadataColumns());
    }

    @Test
    public void readEvents() throws Exception {
        Context context = new Context(params);
        LDVEventDeserializer.Builder builder = new LDVEventDeserializer.Builder();
        EventDeserializer eventDeserializer = builder.build(context, resettableFileInputStream);
        Assert.assertNotNull("eventDeserializer should not be null", eventDeserializer);
        List<Event> events = eventDeserializer.readEvents(expected.length * 2);
        Assert.assertEquals("expected array does not match events list",  expected.length, events.size());
        for(int i=0;i<expected.length;i++){
            Message message = expected[i];
            Event event = events.get(i);

            //Header size needs to account for the schema literal
            Assert.assertEquals(message.getMetadata().size() + 1, event.getHeaders().size());

            for(CharSequence key:message.getMetadata().keySet()){
                Assert.assertEquals(
                        String.format("Message at index %s: key %s does not match.", i, key),
                        message.getMetadata().get(key),
                        event.getHeaders().get(key)
                );
            }
        }
    }
}
