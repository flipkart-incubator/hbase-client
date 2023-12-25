package com.flipkart.yak.models;

import java.util.*;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.KeyValue;

public class DeleteDataBuilder {

  private final String tableName;
  private byte[] rowKey;
  private Map<String, Set<String>> cfMap;
  private byte[] partitionKey;
  private Optional<KeyValue.Type> type = Optional.empty();

  public DeleteDataBuilder(String tableName) {
    this.tableName = tableName;
  }

  public DeleteDataBuilder withRowKey(byte[] key) {
    this.rowKey = key;
    return this;
  }

  public DeleteDataBuilder withPartitionKey(byte[] partitionKey) {
    this.partitionKey = partitionKey;
    return this;
  }

  public DeleteDataBuilder withType(KeyValue.Type type) {
    this.type = Optional.of(type);
    return this;
  }

  public DeleteDataBuilder addColumn(String cf, String column) {
    if (cfMap == null) {
      cfMap = new HashMap<>();
    }
    Set<String> cols = cfMap.getOrDefault(cf, new HashSet<>());
    cols.add(column);
    cfMap.put(cf, cols);
    return this;
  }

  public DeleteDataBuilder addCf(String cf) {
    if (cfMap == null) {
      cfMap = new HashMap<>();
    }
    cfMap.put(cf, new HashSet<>()); //empty set signifies delete entire family
    return this;
  }

  public DeleteData build() {
    return new DeleteData(tableName, rowKey, partitionKey, cfMap, type);
  }

  public CheckAndDeleteData buildWithCheckAndVerifyColumn(String cf, String column, byte[] data) {
    CheckVerifyData checkVerifyData = new CheckVerifyData(cf, column, data, CompareOperator.EQUAL);
    return new CheckAndDeleteData(tableName, rowKey, partitionKey, cfMap, checkVerifyData, type);
  }

  public CheckAndDeleteData buildWithCheckAndVerifyColumn(String cf, String column, byte[] data, CompareOperator compareOp) {
    CheckVerifyData checkVerifyData = new CheckVerifyData(cf, column, data, compareOp);
    return new CheckAndDeleteData(tableName, rowKey, partitionKey, cfMap, checkVerifyData, type);
  }

  public CheckAndDeleteData buildWithCheckAndVerifyColumn(String cf, String column, byte[] data, String compareOp) {
    CheckVerifyData checkVerifyData = new CheckVerifyData(cf, column, data, CompareOperator.valueOf(compareOp));
    return new CheckAndDeleteData(tableName, rowKey, partitionKey, cfMap, checkVerifyData, type);
  }
}
