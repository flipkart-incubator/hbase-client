package com.flipkart.yak.models;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Durability;

public class StoreDataBuilder {

  private final String tableName;
  private byte[] rowKey;
  private byte[] partitionKey;
  private Map<String, ColumnsMap> cfMap;
  private Optional<Long> ttl = Optional.empty();
  private Optional<Durability> durabilityOverride = Optional.empty();

  public StoreDataBuilder(String tablename) {
    this.tableName = tablename;
  }

  public StoreDataBuilder withRowKey(byte[] key) {
    this.rowKey = key;
    return this;
  }

  public StoreDataBuilder withPartitionKey(byte[] partitionKey) {
    this.partitionKey = partitionKey;
    return this;
  }

  public StoreDataBuilder withTTLInMilliseconds(long ttl) {
    this.ttl = Optional.of(ttl);
    return this;
  }

  public StoreDataBuilder withDurability(Durability durability) {
    this.durabilityOverride = Optional.of(durability);
    return this;
  }

  public StoreDataBuilder addColumn(String cf, String column, byte[] data) {
    if (cfMap == null) {
      cfMap = new HashMap<>();
    }
    ColumnsMap colMap = cfMap.getOrDefault(cf, new ColumnsMap());
    Cell cell = colMap.getOrDefault(column, new Cell(data));
    colMap.put(column, cell);
    cfMap.put(cf, colMap);
    return this;
  }

  public StoreDataBuilder addIndex(String cf, String indexKey, IndexType type) {
    if (cfMap == null) {
      cfMap = new HashMap<>();
    }
    ColumnsMap colMap = cfMap.getOrDefault(cf, new ColumnsMap());
    Cell cell = colMap.getOrDefault(indexKey, new IndexableCell(type, System.currentTimeMillis()));
    colMap.put(indexKey, cell);
    cfMap.put(cf, colMap);
    return this;
  }

  public StoreData build() {
    return new StoreData(tableName, rowKey, partitionKey, cfMap, ttl, durabilityOverride);
  }

  public CheckAndStoreData buildWithCheckAndVerifyColumn(String cf, String column, byte[] data) {
    CheckVerifyData checkPutVerifyData = new CheckVerifyData(cf, column, data, CompareOperator.EQUAL);
    return new CheckAndStoreData(tableName, rowKey, partitionKey, cfMap, ttl, durabilityOverride, checkPutVerifyData);
  }

  public CheckAndStoreData buildWithCheckAndVerifyColumn(String cf, String column, byte[] data,
      CompareOperator compareOp) {
    CheckVerifyData checkPutVerifyData = new CheckVerifyData(cf, column, data, compareOp);
    return new CheckAndStoreData(tableName, rowKey, partitionKey, cfMap, ttl, durabilityOverride, checkPutVerifyData);
  }

  public CheckAndStoreData buildWithCheckAndVerifyColumn(String cf, String column, byte[] data, String compareOp) {
    CheckVerifyData checkPutVerifyData = new CheckVerifyData(cf, column, data, CompareOperator.valueOf(compareOp));
    return new CheckAndStoreData(tableName, rowKey, partitionKey, cfMap, ttl, durabilityOverride, checkPutVerifyData);
  }
}
