/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudera.integration.oracle.goldengate.ldv.mapreduce.lib.output;

import com.cloudera.integration.oracle.goldengate.ldv.mapreduce.lib.FieldValueWritable;
import com.cloudera.integration.oracle.goldengate.ldv.mapreduce.lib.LengthDelimitedWritable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

/**
 *
 * @author jcustenborder
 */
public class LengthDelimitedOutputFormatTest {

  @Test
  public void test() throws IOException, InterruptedException {

  }
}
