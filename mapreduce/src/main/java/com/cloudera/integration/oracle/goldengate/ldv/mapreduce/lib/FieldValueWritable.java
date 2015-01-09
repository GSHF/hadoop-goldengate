/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudera.integration.oracle.goldengate.ldv.mapreduce.lib;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author jcustenborder
 */
public class FieldValueWritable implements Writable {

  int flag;
  String data;

  public String getData() {
    return data;
  }

  public char getFlag() {
    return (char) flag;
  }

  public void setData(String data) {
    this.data = data;
  }

  public void setFlag(char value) {
    this.flag = value;
  }

  public void setFlag(int value) {
    setFlag((char) value);
  }

  @Override
  public void write(DataOutput d) throws IOException {
    d.writeChar(flag);

    if (null == this.data) {
      d.writeBoolean(true);
    } else {
      d.writeBoolean(false);
      d.writeUTF(this.data);
    }

  }

  @Override
  public void readFields(DataInput di) throws IOException {
    this.flag = di.readChar();
    boolean isDataNull = di.readBoolean();
    if (isDataNull) {
      this.data = null;
    } else {
      this.data = di.readUTF();
    }
  }

  @Override
  public String toString() {
    return String.format("Flag = '%s' Length='%s' Data='%s'", (char) this.flag, null == this.data ? 0 : this.data.length(), this.data);
  }
}
