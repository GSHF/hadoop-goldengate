package com.cloudera.integration.oracle.goldengate.ldv.common.io;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by jcustenborder on 4/24/15.
 */
public abstract class LengthFormatter {
    final int length;

    public LengthFormatter(int length){
        Preconditions.checkState(2==length||4==length||8==length, "Unsupported length %s. 2, 4, 8 are supported", length);
        this.length = length;
    }

    public abstract void writeLength(OutputStream stream, int length) throws IOException;
    public abstract int readLength(InputStream stream) throws IOException;
}
