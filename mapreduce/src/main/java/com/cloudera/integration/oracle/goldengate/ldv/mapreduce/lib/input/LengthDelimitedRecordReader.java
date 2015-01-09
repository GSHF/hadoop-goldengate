/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudera.integration.oracle.goldengate.ldv.mapreduce.lib.input;

import com.cloudera.integration.oracle.goldengate.ldv.mapreduce.lib.Constants;
import com.cloudera.integration.oracle.goldengate.ldv.mapreduce.lib.FieldValueWritable;
import com.cloudera.integration.oracle.goldengate.ldv.mapreduce.lib.LengthDelimitedWritable;
import com.google.common.base.Preconditions;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author jcustenborder
 */
public class LengthDelimitedRecordReader extends RecordReader<LongWritable, LengthDelimitedWritable> {

  static final Charset charset_UTF8 = Charset.forName("UTF-8");
  FSDataInputStream inputStream;
  LongWritable key = new LongWritable();
  LengthDelimitedWritable value = new LengthDelimitedWritable();
  int sizeRecordLength;
  int sizeFieldLength;
  FileSplit fileSplit;
  byte[] recordLengthBuffer;
  byte[] fieldLengthBuffer;

  @Override
  public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
    this.fileSplit = (FileSplit) is;

    sizeRecordLength = tac.getConfiguration().getInt(Constants.RECORD_PREFIX_LENGTH, -1);
    Preconditions.checkArgument(sizeRecordLength > 0, Constants.RECORD_PREFIX_LENGTH + " must be configured.");
    Preconditions.checkArgument(sizeRecordLength == 2 || sizeRecordLength == 4 || sizeRecordLength == 8, Constants.RECORD_PREFIX_LENGTH + " must be either 2, 4, or 8.");
    recordLengthBuffer = new byte[sizeRecordLength];

    sizeFieldLength = tac.getConfiguration().getInt(Constants.FIELD_PREFIX_LENGTH, -1);
    Preconditions.checkArgument(sizeFieldLength > 0, Constants.FIELD_PREFIX_LENGTH + " must be configured.");
    Preconditions.checkArgument(sizeFieldLength == 2 || sizeFieldLength == 4 || sizeFieldLength == 8, Constants.FIELD_PREFIX_LENGTH + " must be either 2, 4, or 8.");
    fieldLengthBuffer = new byte[sizeFieldLength];

    FileSystem fileSystem = this.fileSplit.getPath().getFileSystem(tac.getConfiguration());
    int inputBufferSize = tac.getConfiguration().getInt(Constants.INPUT_BUFFER_SIZE, 5 * 1024 * 1024);
    this.inputStream = fileSystem.open(this.fileSplit.getPath(), inputBufferSize);
  }

  int getLengthAscii(byte[] buffer) {
    String asciiNumber = new String(buffer, charset_UTF8);
    return Integer.parseInt(asciiNumber);
  }

  int getRecordLength(byte[] buffer) {
    return getLengthAscii(buffer);
  }

  int getFieldLength(byte[] buffer) {
    return getLengthAscii(buffer);
  }

  long recordNumber = 0L;

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    try {
      try {
        inputStream.readFully(recordLengthBuffer);
      } catch (EOFException ex) {
        return false;
      }
      int recordLength = getRecordLength(recordLengthBuffer);
      byte[] recordBuffer = new byte[recordLength];
      this.inputStream.readFully(recordBuffer);
      parseRecord(recordBuffer);
      recordNumber++;
      return true;
    } catch (NumberFormatException ex) {
      throw new IOException(
              String.format("offset='%s' path='%s'",
                      Long.toHexString(this.inputStream.getPos()),
                      fileSplit.getPath()
              ), ex);
    }

  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return this.key;
  }

  @Override
  public LengthDelimitedWritable getCurrentValue() throws IOException, InterruptedException {
    return this.value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (inputStream.getPos() == 0 || this.fileSplit.getLength() == 0) {
      return 0F;
    }

    return (float) (inputStream.getPos() / this.fileSplit.getLength());
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }

  String readString(InputStream input) throws IOException {
    input.read(fieldLengthBuffer);
    int fieldLength = getFieldLength(fieldLengthBuffer);
    byte[] fieldBuffer = new byte[fieldLength];
    input.read(fieldBuffer);
    String fieldValue = new String(fieldBuffer, charset_UTF8);
    return fieldValue;
  }

  void parseRecord(byte[] recordData) throws IOException {
    List<FieldValueWritable> fields = new ArrayList<>();

    try (ByteArrayInputStream input = new ByteArrayInputStream(recordData)) {
      String ggOpCode = readString(input);
      value.setOperation(ggOpCode);
      String ggTimeStamp = readString(input);
      value.setTimestamp(ggTimeStamp);

      while (input.available() > 0) {
        char flag = (char) input.read();
        String data = readString(input);
        FieldValueWritable fieldValue = new FieldValueWritable();
        fieldValue.setFlag(flag);
        if ('P' == flag) {
          fieldValue.setData(data);
        } else {
          fieldValue.setData(null);
        }

        fields.add(fieldValue);
      }
    }

    this.key.set(recordNumber);
    this.value.setFieldValues(fields);
  }

}
