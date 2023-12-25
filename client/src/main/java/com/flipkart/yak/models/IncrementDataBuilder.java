package com.flipkart.yak.models;

import java.util.ArrayList;
import java.util.List;

public class IncrementDataBuilder {

  private final String tableName;
  private List<IncrementData.ColumnData> columnDataList;
  private byte[] rowkey;
  private byte[] partitionKey;

  public IncrementDataBuilder(String tableName) {
    this.tableName = tableName;
  }

  public IncrementDataBuilder withRowKey(byte[] key) {
    this.rowkey = key;
    return this;
  }

  public IncrementDataBuilder withPartitionKey(byte[] partitionKey) {
    this.partitionKey = partitionKey;
    return this;
  }

  public IncrementDataBuilder addColumn(byte[] family, byte[] qualifier, long amount) {
    if (columnDataList == null) {
      columnDataList = new ArrayList<>();
    }
    this.columnDataList.add(new IncrementData.ColumnData(family, qualifier, amount));
    return this;
  }

  public IncrementData build() {
    return new IncrementData(tableName, rowkey, partitionKey, columnDataList);
  }
}
