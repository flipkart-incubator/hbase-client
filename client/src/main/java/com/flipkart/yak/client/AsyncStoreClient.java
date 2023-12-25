package com.flipkart.yak.client;

import com.flipkart.yak.models.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.client.AsyncConnection;

/**
 * Asynchronous implementation of {@link com.flipkart.yak.client.AsyncStoreClient}. This uses underlying {@link
 * org.apache.hadoop.hbase.client.AsyncConnection} and {@link org.apache.hadoop.hbase.client.AsyncTable} to perform CRUD
 * operations on underlying store
 */
public interface AsyncStoreClient {

  /**
   * Used to perform Increment operations on a single row atomically.
   * If the column value does not yet exist it is initialized to amount and written to the specified column.
   *
   * @param incrementData The {@link IncrementData} object that specifies which columns to increment and by what value
   *
   * @return The {@link CompletableFuture} async object that returns {@link ResultMap} if operation was successful and
   * exception if any other failure.
   */
  public CompletableFuture<ResultMap> increment(IncrementData incrementData);

  /**
   * Atomically checks if a row/family/qualifier value matches the expected value. If it does, it adds the put. If the
   * passed value is null, the check is for the lack of column (ie: non-existance)
   *
   * @param data The {@link CheckAndStoreData} object that specifies what to check and put
   *
   * @return The {@link CompletableFuture} async object that returns if operation was successfully inserted or not and
   * if any exception thrown.
   *
   * @since 2.1.0
   */
  public CompletableFuture<Boolean> checkAndPut(CheckAndStoreData data);

  /**
   * Appends the family/qualifier value to the row matching rowkey specified in {@link StoreData}. It can override the
   * value for the specified column qualifier if already exists
   *
   * @param data The {@link StoreData} object that specifies what to append
   *
   * @return The {@link CompletableFuture} async object that returns complete row if operation was successfully inserted
   * and if any exception thrown.
   *
   * @since 2.1.0
   */
  public CompletableFuture<ResultMap> append(StoreData data);

  /**
   * Inserts a new row. If row key already exists, will result into error
   *
   * @param data The {@link StoreData} object that specifies what to put
   *
   * @return The {@link CompletableFuture} async object that returns nothing if operation was successfully inserted and
   * exception if any failure.
   *
   * @since 2.1.0
   */
  public CompletableFuture<Void> put(StoreData data);

  /**
   * Puts some data in the table, in batch. This can be used for group commit, or for submitting user defined batches.
   *
   * @param data The list of mutations to apply specified by {@link StoreData} objects. The batch put is done by
   *             aggregating the iteration of the Puts over the write buffer at the client-side for a single RPC call.
   *
   * @return The {@link List} of {@link CompletableFuture} async object that returns nothing if operation was
   * successfully inserted and exception if any failure for each of the insert in order.
   *
   * @since 2.1.0
   */
  public List<CompletableFuture<Void>> put(List<StoreData> data);

  /**
   * Atomically checks if a row/family/qualifier value matches the expected value. If it does, it deletes the
   * row/family/qualifier.
   *
   * @param data The {@link CheckAndDeleteData} object that specifies what to check and delete
   *
   * @return The {@link CompletableFuture} async object that returns if operation was successfully deleted or not and if
   * any exception thrown.
   *
   * @since 2.1.0
   */
  public CompletableFuture<Boolean> checkAndDelete(CheckAndDeleteData data);

  /**
   * Delete the row/family/qualifier
   *
   * @param data The {@link DeleteData} object that specifies what to delete
   *
   * @return The {@link CompletableFuture} async object that returns nothing if operation was successfully deleted and
   * exception if any failure.
   *
   * @since 2.1.0
   */
  public CompletableFuture<Void> delete(DeleteData data);

  /**
   * Performs multiple mutations atomically on a single row.
   *
   * @param data The {@link MutateData} object that specifies the mutations to perform
   *
   * @return The {@link CompletableFuture} async object that returns nothing if operation was successfully inserted and
   * exception if any failure.
   *
   * @since 2.4.0
   */
  public CompletableFuture<Void> mutate(MutateData data);

  /**
   * Atomically checks if a row/family/qualifier value matches the expected value. If it does, it performs the mutation.
   *
   * @param data The {@link CheckAndMutateData} object that specifies what to check and mutate
   *
   * The {@link CompletableFuture} async object that returns if operation was successfully mutated or not and
   * if any exception thrown.
   *
   * @since 2.4.0
   */
  public CompletableFuture<Boolean> checkAndMutate(CheckAndMutateData data);

  /**
   * Deletes some data in the table, in batch. This can be used for group commit, or for submitting user defined
   * batches.
   *
   * @param data The {@link List} of deletions to apply specified by {@link DeleteData} objects. The batch delete is
   *             done by aggregating the iteration of the Deletes over the write buffer at the client-side for a single
   *             RPC call.
   *
   * @return The {@link List} of {@link CompletableFuture} async object that returns nothing if operation was
   * successfully deleted and exception if any failure for each of the insert in order.
   *
   * @since 2.1.0
   */
  public List<CompletableFuture<Void>> delete(List<DeleteData> data);

  /**
   * Gets the row/family/qualifier
   *
   * @param <T> {@link GetRow} and its subclasses
   * @param row The {@link GetRow} and its sub classes that specifies what to get
   *
   * @return The {@link CompletableFuture} async object that returns {@link ResultMap} if operation was successfully and
   * {@link com.flipkart.yak.client.exceptions.StoreDataNotFoundException} if data was not found and exception if any
   * other failure.
   *
   * @since 2.1.0
   */
  public <T extends GetRow> CompletableFuture<ResultMap> get(T row);

  /**
   * Gets some data from table, in batch. This can be used for group fetches, or for submitting user defined batches.
   *
   * @param rows The {@link List} of {@link GetRow} and its sub classes to fetch the data. The batch get is done by
   *             aggregating the iteration of the gets over the read buffer at the client-side for a single RPC call.
   *
   * @return The {@link List} of {@link CompletableFuture} async object that returns {@link ResultMap} if operation was
   * successful in getting or throws ${@link com.flipkart.yak.client.exceptions.StoreDataNotFoundException} if data
   * doesn't exist and exception if any failure for each of the gets in order.
   *
   * @since 2.1.0
   */
  public List<CompletableFuture<ResultMap>> get(List<? extends GetRow> rows);

  /**
   * Extracts {@link List} of {@link Cell} from the given {@link GetCellByIndex} object.
   *
   * @param row The {@link GetCellByIndex} object that specifies what data to fetch and from which row.
   *
   * @return The {@link CompletableFuture}  if the data coming from the specified row, if it exists. If the row for
   * specified index key specified doesn't exist then {@link com.flipkart.yak.client.exceptions.StoreDataNotFoundException}
   * will be thrown and other exception if not.
   *
   * @since 2.1.0
   */
  public CompletableFuture<List<Cell>> getByIndex(GetCellByIndex row);

  /**
   * Extracts {@link List} of {@link ColumnsMap} from the given {@link GetColumnsMapByIndex} object.
   *
   * @param row The {@link GetColumnsMapByIndex} object that specifies what data to fetch and from which row.
   *
   * @return The {@link CompletableFuture}  if the data coming from the specified row, if it exists. If the row for
   * specified index key specified doesn't exist then {@link com.flipkart.yak.client.exceptions.StoreDataNotFoundException}
   * will be thrown and other exception if not.
   *
   * @since 2.1.0
   */
  public CompletableFuture<List<ColumnsMap>> getByIndex(GetColumnsMapByIndex row);

  /**
   * Scans the data with given scan query object {@link ScanData} and provides list of {@link ResultMap}
   *
   * @param data The {@link ScanData} with scan details provided
   *
   * @return The {@link CompletableFuture} of ResultMap Or Exception if there is a failure in performing scan query
   *
   * @since 2.1.3
   */
  public CompletableFuture<Map<String, ResultMap>> scan(ScanData data);

  /**
   * Shutdown async client
   *
   * @throws IOException          On closing connections
   * @throws InterruptedException On interrupting threads which are being shutdown
   * @since 2.1.0
   */
  public void shutdown() throws IOException, InterruptedException;

  /**
   * Get Async Connection to perforom operations which are not supported by the client
   *
   * @return
   */
  public AsyncConnection getConnection();

  /**
   * Enrich the row key with the specified distributed key
   *
   * @param tableName to table name for which row key to be enriched belongs to
   * @param rowKey       the row key which needs to be enriched
   *
   * @return the enriched key
   *
   * @since 2.1.0
   */
  public byte[] getDistributedKey(String tableName, byte[] rowKey);

  /**
   * Enrich the row key with the specified distributed key
   *
   * @param tableName to table name for which row key to be enriched belongs to
   * @param rowKey       the row key which needs to be enriched
   * @param partitionKey the key to partition on
   *
   * @return the enriched key
   *
   * @since 2.1.0
   */
  public byte[] getDistributedKey(String tableName, byte[] rowKey, byte[] partitionKey);
}
