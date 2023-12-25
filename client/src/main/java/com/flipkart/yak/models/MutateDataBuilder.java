package com.flipkart.yak.models;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.CompareOperator;

public class MutateDataBuilder {

  private final String tableName;
  private byte[] rowKey;
  private List<StoreData> storeDataList = new ArrayList<>();
  private List<DeleteData> deleteDataList = new ArrayList<>();

  public MutateDataBuilder(String tableName) {
    this.tableName = tableName;
  }

  public MutateDataBuilder withRowKey(byte[] rowKey) {
    this.rowKey = rowKey;
    return this;
  }

  public MutateDataBuilder addStoreData(StoreData storeData) {
    if (storeDataList == null) {
      storeDataList = new ArrayList<>();
    }
    storeDataList.add(storeData);
    return this;
  }

  public MutateDataBuilder addDeleteData(DeleteData deleteData) {
    if (deleteDataList == null) {
      deleteDataList = new ArrayList<>();
    }
    deleteDataList.add(deleteData);
    return this;
  }

  public MutateData build() {
    return new MutateData(tableName, rowKey, storeDataList, deleteDataList);
  }

  public CheckAndMutateData buildWithCheckAndVerifyColumn(String cf, String column, byte[] data) {
    CheckVerifyData checkPutVerifyData = new CheckVerifyData(cf, column, data, CompareOperator.EQUAL);
    return new CheckAndMutateData(tableName, rowKey, storeDataList, deleteDataList, checkPutVerifyData);
  }

  public CheckAndMutateData buildWithCheckAndVerifyColumn(String cf, String column, byte[] data, CompareOperator compareOp) {
    CheckVerifyData checkPutVerifyData = new CheckVerifyData(cf, column, data, compareOp);
    return new CheckAndMutateData(tableName, rowKey, storeDataList, deleteDataList, checkPutVerifyData);
  }

  public CheckAndMutateData buildWithCheckAndVerifyColumn(String cf, String column, byte[] data, String compareOp) {
    CheckVerifyData checkPutVerifyData = new CheckVerifyData(cf, column, data, CompareOperator.valueOf(compareOp));
    return new CheckAndMutateData(tableName, rowKey, storeDataList, deleteDataList, checkPutVerifyData);
  }
}
