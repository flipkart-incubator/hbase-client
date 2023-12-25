package com.flipkart.yak.models;

import org.apache.hadoop.hbase.CompareOperator;

public class CheckVerifyData {

  private final String cf;
  private final String qualifier;
  private final byte[] data;
  private CompareOperator compareOperator;

  CheckVerifyData(String cf, String column, byte[] data, CompareOperator compareOperator) {
    this.cf = cf;
    this.qualifier = column;
    this.data = data;
    this.compareOperator = compareOperator;
  }

  public String getCf() {
    return cf;
  }

  public String getQualifier() {
    return qualifier;
  }

  public byte[] getData() {
    return data;
  }

  public CompareOperator getCompareOperator() {
    return this.compareOperator;
  }
}
