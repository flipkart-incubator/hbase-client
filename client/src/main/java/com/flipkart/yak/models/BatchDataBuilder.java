package com.flipkart.yak.models;

import java.util.ArrayList;
import java.util.List;

/**
 * A builder class for creating instances of {@link BatchData}.
 * This class follows the builder pattern to construct a batch of store and delete operations.
 */
public class BatchDataBuilder {

  private final String tableName;
  private List<StoreData> storeDataList = new ArrayList<>();
  private List<DeleteData> deleteDataList = new ArrayList<>();

  /**
   * Constructs a new BatchDataBuilder for a given table.
   *
   * @param tableName The name of the table for the batch operations. Cannot be null or empty.
   * @throws IllegalArgumentException if the tableName is null or empty.
   */
  public BatchDataBuilder(String tableName) {
    if (tableName == null || tableName.trim().isEmpty()) {
      throw new IllegalArgumentException("tableName cannot be null or empty");
    }
    this.tableName = tableName;
  }

  /**
   * Adds a single {@link StoreData} object to the batch.
   *
   * @param storeData The store data to add.
   * @return The current {@link BatchDataBuilder} instance.
   */
  public BatchDataBuilder addStoreData(StoreData storeData) {
    this.storeDataList.add(storeData);
    return this;
  }

  /**
   * Adds a list of {@link StoreData} objects to the batch.
   *
   * @param storeData The list of store data to add.
   * @return The current {@link BatchDataBuilder} instance.
   */
  public BatchDataBuilder addAllStoreData(List<StoreData> storeData) {
    this.storeDataList.addAll(storeData);
    return this;
  }

  /**
   * Adds a single {@link DeleteData} object to the batch.
   *
   * @param deleteData The delete data to add.
   * @return The current {@link BatchDataBuilder} instance.
   */
  public BatchDataBuilder addDeleteData(DeleteData deleteData) {
    this.deleteDataList.add(deleteData);
    return this;
  }

  /**
   * Adds a list of {@link DeleteData} objects to the batch.
   *
   * @param deleteData The list of delete data to add.
   * @return The current {@link BatchDataBuilder} instance.
   */
  public BatchDataBuilder addAllDeleteData(List<DeleteData> deleteData) {
    this.deleteDataList.addAll(deleteData);
    return this;
  }

  /**
   * Builds the final {@link BatchData} object.
   *
   * @return A new {@link BatchData} instance containing the added store and delete operations.
   * @throws IllegalStateException if no store or delete data has been added.
   */
  public BatchData build() {
    if (storeDataList.size() + deleteDataList.size() == 0) {
      throw new IllegalStateException("At least one StoreData or DeleteData must be added to build BatchData");
    }
    return new BatchData(tableName, storeDataList, deleteDataList);
  }
}