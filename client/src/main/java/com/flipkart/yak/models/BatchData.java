package com.flipkart.yak.models;

import java.util.ArrayList;
import java.util.List;

/**
 * {@inheritDoc}
 *
 * A container for batch operations, holding lists of data to be stored and deleted.
 */
public class BatchData extends IdentifierData {

  private final List<StoreData> storeDataList;
  private final List<DeleteData> deleteDataList;

  /**
   * Constructs a new BatchData instance.
   *
   * @param tableName The name of the table for the batch operation.
   * @param storeDataList A list of {@link StoreData} objects to be stored.
   * @param deleteDataList A list of {@link DeleteData} objects to be deleted.
   */
  protected BatchData(String tableName, List<StoreData> storeDataList, List<DeleteData> deleteDataList) {
    super(tableName);
    this.storeDataList = (storeDataList != null) ? new ArrayList<>(storeDataList) : new ArrayList<>();
    this.deleteDataList = (deleteDataList != null) ? new ArrayList<>(deleteDataList) : new ArrayList<>();
  }

  /**
   * Gets the list of data to be stored.
   *
   * @return A list of {@link StoreData} objects.
   */
  public List<StoreData> getStoreDataList() {
    return storeDataList;
  }

  /**
   * Gets the list of data to be deleted.
   *
   * @return A list of {@link DeleteData} objects.
   */
  public List<DeleteData> getDeleteDataList() {
    return deleteDataList;
  }
}


