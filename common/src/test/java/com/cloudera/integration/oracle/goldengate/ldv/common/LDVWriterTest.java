package com.cloudera.integration.oracle.goldengate.ldv.common;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;

/**
 * Created by jcustenborder on 4/28/15.
 */
public class LDVWriterTest {

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
    public void md5Check() throws Exception {
        final Message[] expected = TestDataHelper.getTestMessages();

        MessageDigest hashalg = MessageDigest.getInstance("MD5");
        byte[] actualHash;
        try(ByteArrayOutputStream fileOutputStream = new ByteArrayOutputStream()){
            try(DigestOutputStream digestOutputStream = new DigestOutputStream(fileOutputStream, hashalg)){
                digestOutputStream.on(true);
                try(LDVWriter writer = factory.openWriter(digestOutputStream)){
                    for(Message message:expected)
                        writer.write(message);
                }
                actualHash = digestOutputStream.getMessageDigest().digest();
            }
        }

        hashalg.reset();
        byte[] expectedHash;
        try(InputStream resourceAsStream = LDVReaderTest.class.getResourceAsStream("example.ldv")){
            try(DigestInputStream digestInputStream = new DigestInputStream(resourceAsStream, hashalg)){
                byte[] buffer = new byte[512];
                int length = 0;

                while((length = digestInputStream.read(buffer))>0){

                }
                expectedHash = digestInputStream.getMessageDigest().digest();
            }
        }

        Assert.assertArrayEquals(expectedHash, actualHash);
    }
}
