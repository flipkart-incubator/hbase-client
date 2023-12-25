package com.flipkart.yak.models;

import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.hbase.client.Durability;

public class CheckAndStoreData extends StoreData {

  private final CheckVerifyData verifyData;

  CheckAndStoreData(String tableName, byte[] row, Map<String, ColumnsMap> cfs, Optional<Long> ttl,
      Optional<Durability> durabilityOverride, CheckVerifyData verifyData) {
    super(tableName, row, cfs, ttl, durabilityOverride);
    this.verifyData = verifyData;
  }

  CheckAndStoreData(String tableName, byte[] rowKey, byte[] partitionKey, Map<String, ColumnsMap> cfs, Optional<Long> ttl,
                    Optional<Durability> durabilityOverride, CheckVerifyData verifyData) {
    super(tableName, rowKey, partitionKey, cfs, ttl, durabilityOverride);
    this.verifyData = verifyData;
  }

  public CheckVerifyData getVerifyData() {
    return verifyData;
  }

}
