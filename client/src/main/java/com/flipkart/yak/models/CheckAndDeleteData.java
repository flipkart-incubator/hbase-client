package com.flipkart.yak.models;

import org.apache.hadoop.hbase.KeyValue;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class CheckAndDeleteData extends DeleteData {

  private final CheckVerifyData verifyData;

  CheckAndDeleteData(String tableName, byte[] row, Map<String, Set<String>> cfs, CheckVerifyData verifyData, Optional<KeyValue.Type> type) {
    super(tableName, row, cfs, type);
    this.verifyData = verifyData;
  }

  CheckAndDeleteData(String tableName, byte[] row, byte[] partitionKey, Map<String, Set<String>> cfs, CheckVerifyData verifyData, Optional<KeyValue.Type> type) {
    super(tableName, row, partitionKey, cfs, type);
    this.verifyData = verifyData;
  }

  public CheckVerifyData getVerifyData() {
    return verifyData;
  }

}
