/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudera.integration.oracle.goldengate.ldv.mapreduce.lib;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author jcustenborder
 */
public class LengthDelimitedWritableTest extends WritableTest<LengthDelimitedWritable> {

  @Override
  protected LengthDelimitedWritable createWritable() {
    return new LengthDelimitedWritable();
  }

  @Test
  public void setTimeStampLong() throws IOException {
    this.input.setTimestamp(new Date().getTime());
    this.input.setOperation("INS");

    List<FieldValueWritable> writables = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      FieldValueWritable writable = new FieldValueWritable();
      int mod = (i % 3) + 1;

      switch (mod) {
        case 1:
          writable.setFlag('P');
          writable.setData("This is test data");
          break;
        case 2:
          writable.setFlag('M');
          break;
        case 3:
          writable.setFlag('N');
          writable.setData(null);
          break;
      }

      writables.add(writable);
    }

    this.input.setFieldValues(writables);
    serialize();

  }

  @Test
  public void setTimeStampString() throws IOException {
    Timestamp timestamp = new Timestamp(new Date().getTime());
    this.input.setTimestamp(timestamp.toString());
    this.input.setOperation("INS");

    List<FieldValueWritable> writables = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      FieldValueWritable writable = new FieldValueWritable();
      int mod = (i % 3) + 1;

      switch (mod) {
        case 1:
          writable.setFlag('P');
          writable.setData("This is test data");
          break;
        case 2:
          writable.setFlag('M');
          break;
        case 3:
          writable.setFlag('N');
          writable.setData(null);
          break;
      }

      writables.add(writable);
    }

    this.input.setFieldValues(writables);
    serialize();
  }

  @Test
  public void setTimeStampText() throws IOException {
    Timestamp timestamp = new Timestamp(new Date().getTime());
    this.input.setTimestamp(timestamp.toString());
    this.input.setOperation(new Text("INS"));

    List<FieldValueWritable> writables = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      FieldValueWritable writable = new FieldValueWritable();
      int mod = (i % 3) + 1;

      switch (mod) {
        case 1:
          writable.setFlag('P');
          writable.setData("This is test data");
          break;
        case 2:
          writable.setFlag('M');
          break;
        case 3:
          writable.setFlag('N');
          writable.setData(null);
          break;
      }

      writables.add(writable);
    }

    this.input.setFieldValues(writables);
    serialize();
  }

  @Override
  protected void assertWritable() {
    Assert.assertEquals(this.input.operation, this.output.operation);
    Assert.assertEquals(this.input.timestamp, this.output.timestamp);
//    Assert.assertEquals(this.input.fieldValues, this.output.fieldValues);
  }
}
