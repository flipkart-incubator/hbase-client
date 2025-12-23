package com.flipkart.yak.models;

import java.util.ArrayList;
import java.util.List;

public class BatchData extends IdentifierData {

  private final List<StoreData> storeDataList;
  private final List<DeleteData> deleteDataList;

  protected BatchData(String tableName, List<StoreData> storeDataList, List<DeleteData> deleteDataList) {
    super(tableName);
    this.storeDataList = (storeDataList != null) ? storeDataList : new ArrayList<>();
    this.deleteDataList = (deleteDataList != null) ? deleteDataList : new ArrayList<>();
  }

  public List<StoreData> getStoreDataList() {
    return storeDataList;
  }

  public List<DeleteData> getDeleteDataList() {
    return deleteDataList;
  }
}


