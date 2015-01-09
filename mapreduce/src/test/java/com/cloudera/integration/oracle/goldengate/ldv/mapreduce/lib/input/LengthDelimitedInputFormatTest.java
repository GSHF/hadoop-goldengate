/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudera.integration.oracle.goldengate.ldv.mapreduce.lib.input;

import com.cloudera.integration.oracle.goldengate.ldv.mapreduce.lib.Constants;
import com.cloudera.integration.oracle.goldengate.ldv.mapreduce.lib.FieldValueWritable;
import com.cloudera.integration.oracle.goldengate.ldv.mapreduce.lib.LengthDelimitedWritable;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Timestamp;
import org.junit.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author jcustenborder
 */
public class LengthDelimitedInputFormatTest {

  File tempFile;

  byte[] getLength(int value) {
    String length = String.format("%04d", value);
    return length.getBytes();
  }

  void writeString(String value, OutputStream buffer) throws IOException {
    byte[] buf = value.getBytes();
    byte[] lenBuff = getLength(value.length());
    buffer.write(lenBuff);
    buffer.write(buf);
  }

  String chars = "abcdefghijklmnopqrstuvwxyz";

  @Before
  public void setup() throws FileNotFoundException, IOException {
    tempFile = File.createTempFile("foo", "bar");
    tempFile.deleteOnExit();

    try (FileOutputStream iostr = new FileOutputStream(tempFile)) {
      try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
        writeString("INS", buffer);
        writeString("2014-12-31 23:06:06.255263", buffer);
        for (int i = 0; i < chars.length(); i++) {
          String value = chars.substring(0, i);
          buffer.write((int) 'P');
          writeString(value, buffer);
        }
        byte[] length = getLength(buffer.size());
        iostr.write(length);
        buffer.writeTo(iostr);
      }
    }
  }

  @Test
  public void test() throws IOException, InterruptedException {
    Configuration conf = new Configuration(false);
    conf.set("fs.default.name", "file:///");
    conf.setInt(Constants.RECORD_PREFIX_LENGTH, 4);
    conf.setInt(Constants.FIELD_PREFIX_LENGTH, 4);

    Path path = new Path(tempFile.getAbsoluteFile().toURI());

    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    LengthDelimitedInputFormat inputFormat = ReflectionUtils.newInstance(LengthDelimitedInputFormat.class, conf);
    try (LengthDelimitedRecordReader reader = (LengthDelimitedRecordReader) inputFormat.createRecordReader(null, context)) {
      FileSplit split = new FileSplit(path, 0, tempFile.length(), null);
      reader.initialize(split, context);

      while (reader.nextKeyValue()) {
        LengthDelimitedWritable writable = reader.getCurrentValue();
        Assert.assertNotNull(writable);
        Timestamp timestamp = new Timestamp(writable.getTimestamp().get());

        Assert.assertEquals("2014-12-31 23:06:06.255", timestamp.toString());
        FieldValueWritable[] writables = writable.getWritables();
        for (int i = 0; i < chars.length(); i++) {
          String value = chars.substring(0, i);
          FieldValueWritable fieldValueWritable = writables[i];
          Assert.assertEquals(value, fieldValueWritable.getData());
        }
        
//          System.out.println(reader.getCurrentValue());
      }
    }

  }

  public static void main(String... args) throws Exception {
    LengthDelimitedInputFormatTest test = new LengthDelimitedInputFormatTest();
    test.test();
  }

}
