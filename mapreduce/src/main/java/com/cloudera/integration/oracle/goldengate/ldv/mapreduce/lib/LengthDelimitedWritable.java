package com.cloudera.integration.oracle.goldengate.ldv.mapreduce.lib;

import com.google.common.base.Preconditions;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author jcustenborder
 */
public class LengthDelimitedWritable implements Writable {

  LongWritable timestamp = new LongWritable();
  Text operation = new Text();
  ArrayWritable fieldValues = new ArrayWritable(FieldValueWritable.class);

  public LongWritable getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(String value) {
    Preconditions.checkNotNull(value, "value cannot be null.");
    Timestamp ts = Timestamp.valueOf(value);
    Long time = ts.getTime();
    this.timestamp.set(time);
  }

  public void setTimestamp(LongWritable value) {
    this.timestamp = value;
  }

  public void setTimestamp(Long value) {
    this.timestamp.set(value);
  }

  public Text getOperation() {
    return operation;
  }

  public void setOperation(String value) {
    this.operation.set(value);
  }

  public void setOperation(Text operation) {
    this.operation = operation;
  }

  public ArrayWritable getFieldValues() {
    return fieldValues;
  }

  public void setFieldValues(ArrayWritable values) {
    this.fieldValues = values;
  }

  public void setFieldValues(List<FieldValueWritable> values) {
    FieldValueWritable[] valuesArr = values.toArray(new FieldValueWritable[values.size()]);
    this.fieldValues.set(valuesArr);
  }

  public FieldValueWritable[] getWritables(){
    return (FieldValueWritable[])this.getFieldValues().get();
  }
  
  @Override
  public void write(DataOutput d) throws IOException {
    this.timestamp.write(d);
    this.operation.write(d);
    this.fieldValues.write(d);
  }

  @Override
  public void readFields(DataInput di) throws IOException {
    this.timestamp.readFields(di);
    this.operation.readFields(di);
    this.fieldValues.readFields(di);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("opcode='");
    builder.append(this.operation);
    builder.append("' ");

    builder.append("timestamp='");
    builder.append(this.timestamp);
    builder.append("'");    
    
    FieldValueWritable[] writables = getWritables();
    for(FieldValueWritable writable:writables){
      builder.append(" (");
      builder.append(writable);
      builder.append(")");
    }
    
    return builder.toString();
  }

}
