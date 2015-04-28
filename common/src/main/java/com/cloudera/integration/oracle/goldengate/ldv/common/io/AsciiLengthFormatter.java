package com.cloudera.integration.oracle.goldengate.ldv.common.io;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;

/**
 * Created by jcustenborder on 4/24/15.
 */
public class AsciiLengthFormatter extends LengthFormatter {
    private static final Charset charset_UTF8 = Charset.forName("UTF-8");
    private final String format;
    private final byte[] buffer;
    public AsciiLengthFormatter(int length){
        super(length);
        this.format = "%0" + length + "d";
        this.buffer = new byte[length];
    }

    @Override
    public void writeLength(OutputStream stream, int length) throws IOException {
        String lengthPrefix = String.format(this.format, length);
        byte[] buffer = lengthPrefix.getBytes(charset_UTF8);
        stream.write(buffer);
    }

    @Override
    public int readLength(InputStream stream) throws IOException {
        int read = stream.read(buffer);

        if(read==-1){
            return -1;
        }

        Preconditions.checkState(buffer.length == read, "Expected %s bytes but read %s byte(s).", buffer.length, read);
        String input = new String(buffer);
        return Integer.parseInt(input);
    }
}
