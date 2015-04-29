package com.cloudera.integration.oracle.goldengate.ldv.common;

import com.google.common.base.Preconditions;

import java.io.*;

/**
 * Created by jcustenborder on 4/24/15.
 */
public class LDVFactory {
    final Settings settings;

    public LDVFactory(Settings settings){
        Preconditions.checkNotNull(settings, "settings");
        Preconditions.checkState(settings.fieldLength == 2 || settings.fieldLength == 4 || settings.fieldLength == 8,
                "fieldLength has an invalid value of %s. Valid values are 2,4,8", settings.fieldLength);
        Preconditions.checkState(settings.recordLength==2||settings.recordLength==4||settings.recordLength==8,
                "fieldLength has an invalid value of %s. Valid values are 2,4,8", settings.recordLength);
        Preconditions.checkState(settings.fieldLengthEncoding==Settings.LengthEncoding.ASCII,
                "fieldLengthEncoding of %s is not supported.", settings.fieldLengthEncoding);
        Preconditions.checkState(settings.recordLengthEncoding==Settings.LengthEncoding.ASCII,
                "recordLengthEncoding of %s is not supported.", settings.recordLengthEncoding);
        this.settings = settings;
    }

    public LDVReader openReader(String inputPath) throws IOException {
        FileInputStream inputStream = new FileInputStream(inputPath);
        return new LDVReader(inputStream, settings);
    }

    public LDVWriter openWriter(OutputStream outputStream) {
        return new LDVWriter(outputStream, settings);
    }

    public LDVReader openReader(InputStream inputStream) {
        return new LDVReader(inputStream, settings);
    }

    public LDVWriter openWriter(File outputPath) throws IOException {
        FileOutputStream outputStream = new FileOutputStream(outputPath);
        return new LDVWriter(outputStream, this.settings);
    }
}
