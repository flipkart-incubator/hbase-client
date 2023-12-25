package com.flipkart.yak.models;

import java.util.List;

public class CheckAndMutateData extends MutateData {

  private final CheckVerifyData verifyData;

  CheckAndMutateData(String tableName, byte[] row, List<StoreData> storeDataList, CheckVerifyData verifyData) {
    super(tableName, row, storeDataList);
    this.verifyData = verifyData;
  }

  protected CheckAndMutateData(String tableName, byte[] row, List<StoreData> storeDataList, List<DeleteData> deleteDataList, CheckVerifyData verifyData) {
    super(tableName, row, storeDataList, deleteDataList);
    this.verifyData = verifyData;
  }

  public CheckVerifyData getVerifyData() {
    return verifyData;
  }
}
