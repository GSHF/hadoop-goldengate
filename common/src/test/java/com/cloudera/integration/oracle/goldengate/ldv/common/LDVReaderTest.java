package com.cloudera.integration.oracle.goldengate.ldv.common;

import org.junit.Assert;
import org.junit.Before;
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
    static Message[] expected;

    @BeforeClass
    public static void setupClass(){
        expected = TestDataHelper.getTestMessages();
    }

    Settings settings;
    LDVFactory factory;

    @Before
    public void setup(){
        settings = new Settings();
        settings.fieldLengthEncoding = Settings.LengthEncoding.ASCII;
        settings.fieldLength = 8;
        settings.recordLengthEncoding = Settings.LengthEncoding.ASCII;
        settings.recordLength = 8;
        settings.metadataColumns = new String[]{"opcode", "timestamp"};

        factory = new LDVFactory(settings);
    }

    @Test
    public void test() throws IOException{
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
