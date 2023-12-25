package com.flipkart.yak.models;

import org.apache.hadoop.hbase.util.Bytes;

public class IndexableCell extends Cell {

  private final IndexType indexType;

  public IndexableCell(IndexType indexType, long sortNumber) {
    super(Bytes.toBytes(sortNumber));
    this.indexType = indexType;
  }

  public IndexType getIndexType() {
    return indexType;
  }

}
