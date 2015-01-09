package com.cloudera.integration.oracle.goldengate.ldv.mapreduce.lib.output;

import com.cloudera.integration.oracle.goldengate.ldv.mapreduce.lib.LengthDelimitedWritable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author jcustenborder
 */
public class LengthDelimitedOutputFormat extends FileOutputFormat<NullWritable, LengthDelimitedWritable> {

  @Override
  public RecordWriter<NullWritable, LengthDelimitedWritable> getRecordWriter(TaskAttemptContext tac) throws IOException, InterruptedException {
    Configuration conf = tac.getConfiguration();
    Path workFilePath = getDefaultWorkFile(tac, ".ldv");
    FileSystem fs = workFilePath.getFileSystem(conf);
    FSDataOutputStream fileOut = fs.create(workFilePath, false);
    return new LengthDelimitedRecordWriter(conf, fileOut);
  }

}
