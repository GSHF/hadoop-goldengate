/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudera.integration.oracle.goldengate.ldv.mapreduce.lib;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.junit.Before;

public abstract class WritableTest<K extends Writable> {

  protected abstract K createWritable();
  protected abstract void assertWritable();
  protected K input;
  protected K output;
  
  @Before
  public void setup() {
    this.input = createWritable();
    this.output = createWritable();
  }

  
  protected void serialize() throws IOException {
    byte[] buffer;
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
      try (DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
        input.write(dataOutputStream);
        dataOutputStream.flush();
      }
      buffer = byteArrayOutputStream.toByteArray();
    }
    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(buffer)) {
      try (DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream)) {
        output.readFields(dataInputStream);
      }
    }
    assertWritable();
  }

}
