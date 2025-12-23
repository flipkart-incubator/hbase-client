package com.flipkart.yak.models;

import java.util.ArrayList;
import java.util.List;

public class BatchDataBuilder {

  private final String tableName;
  private List<StoreData> storeDataList = new ArrayList<>();
  private List<DeleteData> deleteDataList = new ArrayList<>();

  public BatchDataBuilder(String tableName) {
    this.tableName = tableName;
  }

  public BatchDataBuilder addStoreData(StoreData storeData) {
    this.storeDataList.add(storeData);
    return this;
  }

  public BatchDataBuilder addAllStoreData(List<StoreData> storeData) {
    this.storeDataList.addAll(storeData);
    return this;
  }

  public BatchDataBuilder addDeleteData(DeleteData deleteData) {
    this.deleteDataList.add(deleteData);
    return this;
  }

  public BatchDataBuilder addAllDeleteData(List<DeleteData> deleteData) {
    this.deleteDataList.addAll(deleteData);
    return this;
  }

  public BatchData build() {
    return new BatchData(tableName, storeDataList, deleteDataList);
  }
}

