package com.cloudera.integration.oracle.goldengate.ldv.mapreduce.lib.input;

import com.cloudera.integration.oracle.goldengate.ldv.mapreduce.lib.LengthDelimitedWritable;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 *
 * @author jcustenborder
 */
public class LengthDelimitedInputFormat extends FileInputFormat<LongWritable, LengthDelimitedWritable> {

  
  
  @Override
  public RecordReader<LongWritable, LengthDelimitedWritable> createRecordReader(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
    return new LengthDelimitedRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }
}
