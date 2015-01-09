/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudera.integration.oracle.goldengate.ldv.mapreduce.lib;

import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author jcustenborder
 */
public class FieldValueWritableTest extends WritableTest<FieldValueWritable> {

  @Test
  public void fieldPresent() throws IOException {
    this.input.setData("This is testing data");
    this.input.setFlag('P');
    serialize();
  }

  @Test
  public void flagInt() throws IOException {
    this.input.setData("This is testing data");
    this.input.setFlag((int)'P');
    serialize();
  }
  
  @Test
  public void nullField() throws IOException {
    this.input.setData(null);
    this.input.setFlag('N');
    serialize();
  }
  
   @Test
  public void test_toString() throws IOException {
    this.input.setData(null);
    this.input.setFlag('N');
    serialize();
    System.out.println(this.input);
  }
  

  @Override
  protected FieldValueWritable createWritable() {
    return new FieldValueWritable();
  }

  @Override
  protected void assertWritable() {
    Assert.assertEquals("data does not match", this.input.getData(), this.output.getData());
    Assert.assertEquals("data does not match", this.input.getFlag(), this.output.getFlag());
  }

}
