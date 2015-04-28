package com.cloudera.integration.oracle.goldengate.ldv.common.io;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Created by jcustenborder on 4/24/15.
 */
public class AsciiLengthFormatterTest {

    @Test
    public void prefix_8() throws IOException {
        final int expected = 29;
        final byte[] expectedBuffer = "00000029".getBytes();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(expectedBuffer);
        AsciiLengthFormatter formatter = new AsciiLengthFormatter(8);
        int length = formatter.readLength(inputStream);
        Assert.assertEquals(expected, length);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        formatter.writeLength(outputStream, expected);
        byte[] actualBuffer = outputStream.toByteArray();
        Assert.assertArrayEquals(expectedBuffer, actualBuffer);
    }

    @Test
    public void stream_finished() throws IOException {
        byte[] buffer = new byte[0];
        ByteArrayInputStream inputStream = new ByteArrayInputStream(buffer);
        AsciiLengthFormatter formatter = new AsciiLengthFormatter(8);
        int length = formatter.readLength(inputStream);
        Assert.assertEquals(-1, length);
    }

}
