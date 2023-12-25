package com.flipkart.yak.models;

import java.util.ArrayList;
import java.util.List;

public class MutateData extends IdentifierData {

  private final byte[] row;
  private final List<StoreData> storeDataList;
  private final List<DeleteData> deleteDataList;

  protected MutateData(String tableName, byte[] row, List<StoreData> storeDataList, List<DeleteData> deleteDataList) {
    super(tableName);
    this.row = row;
    this.storeDataList = storeDataList;
    this.deleteDataList = deleteDataList;
  }

  MutateData(String tableName, byte[] row, List<StoreData> storeDataList) {
    super(tableName);
    this.row = row;
    this.storeDataList = storeDataList;
    this.deleteDataList = new ArrayList<>();
  }

  public byte[] getRow() {
    return row;
  }

  public List<StoreData> getStoreDataList() {
    return storeDataList;
  }

  public List<DeleteData> getDeleteDataList() {
    return deleteDataList;
  }
}
