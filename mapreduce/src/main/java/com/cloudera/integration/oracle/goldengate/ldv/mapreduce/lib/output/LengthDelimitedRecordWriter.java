package com.cloudera.integration.oracle.goldengate.ldv.mapreduce.lib.output;

import com.cloudera.integration.oracle.goldengate.ldv.mapreduce.lib.Constants;
import com.cloudera.integration.oracle.goldengate.ldv.mapreduce.lib.FieldValueWritable;
import com.cloudera.integration.oracle.goldengate.ldv.mapreduce.lib.LengthDelimitedWritable;
import com.google.common.base.Preconditions;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author jcustenborder
 */
class LengthDelimitedRecordWriter extends RecordWriter<NullWritable, LengthDelimitedWritable> {

  final FSDataOutputStream fSDataOutputStream;
  int sizeRecordLength;
  int sizeFieldLength;
  String recordPrefixFormat;
  String fieldPrefixFormat;

  ByteArrayOutputStream recordBuffer;

  public LengthDelimitedRecordWriter(Configuration conf, FSDataOutputStream fSDataOutputStream) {
    this.fSDataOutputStream = fSDataOutputStream;
    sizeRecordLength = conf.getInt(Constants.RECORD_PREFIX_LENGTH, -1);
    Preconditions.checkArgument(sizeRecordLength > 0, Constants.RECORD_PREFIX_LENGTH + " must be configured.");
    Preconditions.checkArgument(sizeRecordLength == 2 || sizeRecordLength == 4 || sizeRecordLength == 8, Constants.RECORD_PREFIX_LENGTH + " must be either 2, 4, or 8.");
    recordPrefixFormat = "%0" + (sizeRecordLength - 1) + "d";

    sizeFieldLength = conf.getInt(Constants.FIELD_PREFIX_LENGTH, -1);
    Preconditions.checkArgument(sizeFieldLength > 0, Constants.FIELD_PREFIX_LENGTH + " must be configured.");
    Preconditions.checkArgument(sizeFieldLength == 2 || sizeFieldLength == 4 || sizeFieldLength == 8, Constants.FIELD_PREFIX_LENGTH + " must be either 2, 4, or 8.");
    fieldPrefixFormat = "%0" + (sizeFieldLength - 1) + "d";

    int recordBufferLength = conf.getInt(Constants.RECORD_BUFFER_SIZE, 256 * 1024);
    this.recordBuffer = new ByteArrayOutputStream(recordBufferLength);
  }

  void writeFlag(int value) throws IOException {
    this.recordBuffer.write(value);
  }

  void writeString(String value) throws IOException {
    byte[] buffer = null == value ? new byte[0] : value.getBytes();
    String fieldLengthPrefix = String.format(fieldPrefixFormat, buffer.length);
    byte[] fieldLengthBuffer = fieldLengthPrefix.getBytes();
    this.recordBuffer.write(fieldLengthBuffer);
    this.recordBuffer.write(buffer);
  }

  private void writeMetadataColumns(LengthDelimitedWritable v) throws IOException {
    String operation = v.getOperation().toString();
    writeString(operation);
    Timestamp timestamp = new Timestamp(v.getTimestamp().get());
    String time = timestamp.toString();
    writeString(time);
  }

  @Override
  public void write(NullWritable k, LengthDelimitedWritable v) throws IOException, InterruptedException {
    recordBuffer.reset();

    writeMetadataColumns(v);
    FieldValueWritable[] fields = (FieldValueWritable[]) v.getFieldValues().get();
    for (FieldValueWritable field : fields) {
      writeFlag(field.getFlag());
      writeString(field.getData());
    }

    String recordLengthPrefix = String.format(recordPrefixFormat, recordBuffer.size());
    byte[] recordLengthPrefixBuffer = recordLengthPrefix.getBytes();
    this.fSDataOutputStream.write(recordLengthPrefixBuffer);
    this.recordBuffer.writeTo(this.fSDataOutputStream);
  }

  @Override
  public void close(TaskAttemptContext tac) throws IOException, InterruptedException {
    this.fSDataOutputStream.close();
  }

}
