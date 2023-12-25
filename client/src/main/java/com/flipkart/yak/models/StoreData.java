package com.flipkart.yak.models;

import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.hbase.client.Durability;

public class StoreData extends IdentifierData {

  private final byte[] row;
  private final byte[] partitionKey;
  private final Map<String, ColumnsMap> cfs;
  private final Optional<Long> ttl;
  private final Optional<Durability> durabilityOverride; // if set will override Durability set at StoreConfig

  StoreData(String tableName, byte[] row, Map<String, ColumnsMap> cfs, Optional<Long> ttl,
      Optional<Durability> durabilityOverride) {
    super(tableName);
    this.row = row;
    this.cfs = cfs;
    this.ttl = ttl;
    this.durabilityOverride = durabilityOverride;
    this.partitionKey = null;
  }

  StoreData(String tableName, byte[] row, byte[] partitionKey, Map<String, ColumnsMap> cfs, Optional<Long> ttl,
            Optional<Durability> durabilityOverride) {
    super(tableName);
    this.row = row;
    this.partitionKey = partitionKey;
    this.cfs = cfs;
    this.ttl = ttl;
    this.durabilityOverride = durabilityOverride;
  }

  public byte[] getRow() {
    return row;
  }

  public Map<String, ColumnsMap> getCfs() {
    return cfs;
  }

  public Optional<Long> getTTL() {
    return ttl;
  }

  public Optional<Durability> getDurabilityOverride() {
    return durabilityOverride;
  }

  public byte[] getPartitionKey() {
    return partitionKey;
  }
}
