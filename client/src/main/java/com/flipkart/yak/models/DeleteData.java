package com.flipkart.yak.models;

import org.apache.hadoop.hbase.KeyValue;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class DeleteData extends IdentifierData {

  private final byte[] row;
  private final Map<String, Set<String>> cfs;
  private final byte[] partitionKey;
  private final KeyValue.Type type;

  DeleteData(String tableName, byte[] row, Map<String, Set<String>> cfs, Optional<KeyValue.Type> type) {
    super(tableName);
    this.row = row;
    this.cfs = cfs;
    this.partitionKey = null;
    this.type = type.isPresent() ? type.get() : KeyValue.Type.Delete;
  }

  DeleteData(String tableName, byte[] row, byte[] partitionKey, Map<String, Set<String>> cfs, Optional<KeyValue.Type> type) {
    super(tableName);
    this.row = row;
    this.cfs = cfs;
    this.partitionKey = partitionKey;
    this.type = type.isPresent() ? type.get() : KeyValue.Type.Delete;
  }

  public byte[] getRow() {
    return row;
  }

  public KeyValue.Type getType() { return type; }

  public Map<String, Set<String>> getCfs() {
    return cfs;
  }

  public byte[] getPartitionKey() {
    return partitionKey;
  }
}
