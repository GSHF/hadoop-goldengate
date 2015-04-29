package com.cloudera.integration.oracle.goldengate.ldv.flume;

import org.apache.flume.serialization.ResettableInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by jcustenborder on 4/28/15.
 */
public class ResettableInputStreamWrapper extends InputStream {

    final ResettableInputStream resettableInputStream;

    public ResettableInputStreamWrapper(ResettableInputStream resettableInputStream) {
        this.resettableInputStream = resettableInputStream;
    }


    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public int read() throws IOException {
        return this.resettableInputStream.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return this.resettableInputStream.read(b,off,len);
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public void close() throws IOException {
        this.resettableInputStream.close();
    }

    @Override
    public synchronized void reset() throws IOException {
        this.resettableInputStream.reset();
    }


}
