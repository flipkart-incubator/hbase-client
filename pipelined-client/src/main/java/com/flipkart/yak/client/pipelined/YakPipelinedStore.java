package com.flipkart.yak.client.pipelined;

import com.flipkart.yak.client.AsyncStoreClient;
import com.flipkart.yak.client.pipelined.decorator.circuitbreakers.CircuitBreakerDecorator;
import com.flipkart.yak.client.pipelined.exceptions.PipelinedStoreDataCorruptException;
import com.flipkart.yak.client.pipelined.intent.IntentStoreClient;
import com.flipkart.yak.client.pipelined.models.CircuitBreakerSettings;
import com.flipkart.yak.client.pipelined.models.IntentWriteRequest;
import com.flipkart.yak.client.pipelined.models.MasterSlaveReplicaSet;
import com.flipkart.yak.client.pipelined.models.PipelinedResponse;
import com.flipkart.yak.client.pipelined.models.ReplicaSet;
import com.flipkart.yak.client.pipelined.models.SiteId;
import com.flipkart.yak.client.pipelined.models.StoreOperationResponse;
import com.flipkart.yak.client.pipelined.route.HotRouter;
import com.flipkart.yak.models.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * This interface providers asynchronous callbacks style of interacting with store client with multiple hbase cluster
 * deployments in a {@link ReplicaSet} style selected for individual operations.
 * <br>
 * Each replica set is a set of {@link SiteId}. An Implementation of it is {@link MasterSlaveReplicaSet}, a site can be
 * considered a Primary or Secondary depending on what you want to do with this operation. For Example:
 * <br>
 * You want to make a put call for row1 on SiteA and if it fails, you want to make a put call to SiteB. Here is how you
 * would define your replica set
 * <br>
 * <pre>new MasterSlaveReplicaSet(siteA, Arrays.asList(siteB)) </pre>
 *
 * @param <T> {@link Class} Route Key data type which is used by {@link HotRouter} to select the route
 * @param <U> {@link IntentWriteRequest} Type of Intent Data based on {@link IntentStoreClient} implementation which can
 *            be used to persist in intent store
 * @param <V> {@link CircuitBreakerSettings} Type of Circuit breaker settings based on implementation of {@link
 *            CircuitBreakerDecorator} which is used to wrap around the api calls select the route
 */
@SuppressWarnings("java:S1214")
public interface YakPipelinedStore<T, U extends IntentWriteRequest, V extends CircuitBreakerSettings> {

  String PUT_METHOD_NAME = "put";
  String GET_METHOD_NAME = "get";
  String SCAN_METHOD_NAME = "scan";
  String DELETE_METHOD_NAME = "delete";
  String CHECK_PUT_METHOD_NAME = "checkAndPut";
  String GET_INDEX_METHOD_NAME = "getByIndex";
  String APPEND_METHOD_NAME = "append";
  String CHECK_DELETE_METHOD_NAME = "checkAndDelete";
  String INCREMENT_METHOD_NAME = "increment";

  /**
   * Used to perform Increment operations on a single row atomically
   * If the column value does not yet exist it is initialized to amount and written to the specified column.
   *
   * @param incrementData The {@link IncrementData} object that specifies which column to increment and by what value
   * @param handler       Callback method to collect the response when the asynchronous response is ready
   *
   */
  void increment(IncrementData incrementData, Optional<T> routeKey, Optional<U> intentData,
                 Optional<V> circuitBreakerSettings,
                 BiConsumer<PipelinedResponse<StoreOperationResponse<ResultMap>>, Throwable> handler);

  /**
   * @param data                   {@link StoreData} to put the row
   * @param routeKey               An {@link Optional} route key which is used for selecting the {@link ReplicaSet} by
   *                               {@link HotRouter} for this operation
   * @param intentData             An {@link Optional} {@link IntentWriteRequest} which is written by specific
   *                               implementation {@link IntentStoreClient}
   * @param circuitBreakerSettings An {@link Optional} {@link CircuitBreakerSettings} used by the circuit breaker if
   *                               wrapped around by circuit breaker decorator
   * @param handler                Callback method to collect the response when the asynchronous response is ready
   */
  void put(StoreData data, Optional<T> routeKey, Optional<U> intentData, Optional<V> circuitBreakerSettings,
      BiConsumer<PipelinedResponse<StoreOperationResponse<Void>>, Throwable> handler);

  /**
   * @param data                   {@link List} of {@link StoreData} to put the rows in batches
   * @param routeKey               An {@link Optional} route key which is used for selecting the {@link ReplicaSet} by
   *                               {@link HotRouter} for this operation
   * @param intentData             An {@link Optional} {@link IntentWriteRequest} which is written by specific
   *                               implementation {@link IntentStoreClient}
   * @param circuitBreakerSettings An {@link Optional} {@link CircuitBreakerSettings} used by the circuit breaker if
   *                               wrapped around by circuit breaker decorator
   * @param handler                Callback method to collect the response when the asynchronous response is ready
   */
  void put(List<StoreData> data, Optional<T> routeKey, Optional<U> intentData,
      Optional<V> circuitBreakerSettings,
      BiConsumer<PipelinedResponse<List<StoreOperationResponse<Void>>>, Throwable> handler);

  /**
   * @param data                   {@link CheckAndStoreData} checkes row/family/qualifier value matches the expected
   *                               value and updates if matches else responds with false
   * @param routeKey               An {@link Optional} route key which is used for selecting the {@link ReplicaSet} by
   *                               {@link HotRouter} for this operation
   * @param intentData             An {@link Optional} {@link IntentWriteRequest} which is written by specific
   *                               implementation {@link IntentStoreClient}
   * @param circuitBreakerSettings An {@link Optional} {@link CircuitBreakerSettings} used by the circuit breaker if
   *                               wrapped around by circuit breaker decorator
   * @param handler                Callback method to collect the response when the asynchronous response is ready
   */
  void checkAndPut(CheckAndStoreData data, Optional<T> routeKey, Optional<U> intentData,
      Optional<V> circuitBreakerSettings,
      BiConsumer<PipelinedResponse<StoreOperationResponse<Boolean>>, Throwable> handler);

  /**
   * @param data                   {@link StoreData} object for identifying the row to append the family/qualifier to
   * @param routeKey               An {@link Optional} route key which is used for selecting the {@link ReplicaSet} by
   *                               {@link HotRouter} for this operation
   * @param intentData             An {@link Optional} {@link IntentWriteRequest} which is written by specific
   *                               implementation {@link IntentStoreClient}
   * @param circuitBreakerSettings An {@link Optional} {@link CircuitBreakerSettings} used by the circuit breaker if
   *                               wrapped around by circuit breaker decorator
   * @param handler                Callback method to collect the response when the asynchronous response is ready
   */
  void append(StoreData data, Optional<T> routeKey, Optional<U> intentData, Optional<V> circuitBreakerSettings,
      BiConsumer<PipelinedResponse<StoreOperationResponse<ResultMap>>, Throwable> handler);

  /**
   * @param data                   {@link List} of {@link DeleteData} to delete the rows in batches
   * @param routeKey               An {@link Optional} route key which is used for selecting the {@link ReplicaSet} by
   *                               {@link HotRouter} for this operation
   * @param intentData             An {@link Optional} {@link IntentWriteRequest} which is written by specific
   *                               implementation {@link IntentStoreClient}
   * @param circuitBreakerSettings An {@link Optional} {@link CircuitBreakerSettings} used by the circuit breaker if
   *                               wrapped around by circuit breaker decorator
   * @param handler                Callback method to collect the response when the asynchronous response is ready
   */
  void delete(List<DeleteData> data, Optional<T> routeKey, Optional<U> intentData,
      Optional<V> circuitBreakerSettings,
      BiConsumer<PipelinedResponse<List<StoreOperationResponse<Void>>>, Throwable> handler);

  /**
   * @param data                   {@link CheckAndStoreData} checkes row/family/qualifier value matches the expected
   *                               value and deletes if matches else responds with false
   * @param routeKey               An {@link Optional} route key which is used for selecting the {@link ReplicaSet} by
   *                               {@link HotRouter} for this operation
   * @param intentData             An {@link Optional} {@link IntentWriteRequest} which is written by specific
   *                               implementation {@link IntentStoreClient}
   * @param circuitBreakerSettings An {@link Optional} {@link CircuitBreakerSettings} used by the circuit breaker if
   *                               wrapped around by circuit breaker decorator
   * @param handler                Callback method to collect the response when the asynchronous response is ready
   */
  void checkAndDelete(CheckAndDeleteData data, Optional<T> routeKey, Optional<U> intentData,
      Optional<V> circuitBreakerSettings,
      BiConsumer<PipelinedResponse<StoreOperationResponse<Boolean>>, Throwable> handler);

  /**
   * Scan the table with {@link ScanData}
   *
   * @param data                   {@link ScanData} scan query
   * @param routeKey               An {@link Optional} route key which is used for selecting the {@link ReplicaSet} by
   *                               {@link HotRouter} for this operation
   * @param intentData             An {@link Optional} {@link IntentWriteRequest} which is written by specific
   *                               implementation {@link IntentStoreClient}
   * @param circuitBreakerSettings An {@link Optional} {@link CircuitBreakerSettings} used by the circuit breaker if
   *                               wrapped around by circuit breaker decorator
   * @param handler                Callback method to collect the response when the asynchronous response is ready
   */
  void scan(ScanData data, Optional<T> routeKey, Optional<U> intentData, Optional<V> circuitBreakerSettings,
      BiConsumer<PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>>, Throwable> handler);

  /**
   * @param <X>                    {@link GetRow} and its subclasses
   * @param row                    {@link GetRow} and its sub classes to fetch the row/family/qualifier
   * @param routeKey               An {@link Optional} route key which is used for selecting the {@link ReplicaSet} by
   *                               {@link HotRouter} for this operation
   * @param intentData             An {@link Optional} {@link IntentWriteRequest} which is written by specific
   *                               implementation {@link IntentStoreClient}
   * @param circuitBreakerSettings An {@link Optional} {@link CircuitBreakerSettings} used by the circuit breaker if
   *                               wrapped around by circuit breaker decorator
   * @param handler                Callback method to collect the response when the asynchronous response is ready
   */
  <X extends GetRow> void get(X row, Optional<T> routeKey, Optional<U> intentData,
      Optional<V> circuitBreakerSettings,
      BiConsumer<PipelinedResponse<StoreOperationResponse<ResultMap>>, Throwable> handler);

  /**
   * @param rows                   {@link List} of {@link GetRow} and its sub classes to fetch the row/family/qualifier
   * @param routeKey               An {@link Optional} route key which is used for selecting the {@link ReplicaSet} by
   *                               {@link HotRouter} for this operation
   * @param intentData             An {@link Optional} {@link IntentWriteRequest} which is written by specific
   *                               implementation {@link IntentStoreClient}
   * @param circuitBreakerSettings An {@link Optional} {@link CircuitBreakerSettings} used by the circuit breaker if
   *                               wrapped around by circuit breaker decorator
   * @param handler                Callback method to collect the response when the asynchronous response is ready
   */
  void get(List<? extends GetRow> rows, Optional<T> routeKey, Optional<U> intentData,
      Optional<V> circuitBreakerSettings,
      BiConsumer<PipelinedResponse<List<StoreOperationResponse<ResultMap>>>, Throwable> handler);

  /**
   * @param indexLookup            {@link GetColumnsMapByIndex} for looking up the {@link ColumnsMap} by index key
   * @param routeKey               An {@link Optional} route key which is used for selecting the {@link ReplicaSet} by
   *                               {@link HotRouter} for this operation
   * @param intentData             An {@link Optional} {@link IntentWriteRequest} which is written by specific
   *                               implementation {@link IntentStoreClient}
   * @param circuitBreakerSettings An {@link Optional} {@link CircuitBreakerSettings} used by the circuit breaker if
   *                               wrapped around by circuit breaker decorator
   * @param handler                Callback method to collect the response when the asynchronous response is ready
   */
  void getByIndex(GetColumnsMapByIndex indexLookup, Optional<T> routeKey, Optional<U> intentData,
      Optional<V> circuitBreakerSettings,
      BiConsumer<PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>>, Throwable> handler);

  /**
   * @param indexLookup            {@link GetCellByIndex} for looking up the {@link Cell} by index key
   * @param routeKey               An {@link Optional} route key which is used for selecting the {@link ReplicaSet} by
   *                               {@link HotRouter} for this operation
   * @param intentData             An {@link Optional} {@link IntentWriteRequest} which is written by specific
   *                               implementation {@link IntentStoreClient}
   * @param circuitBreakerSettings An {@link Optional} {@link CircuitBreakerSettings} used by the circuit breaker if
   *                               wrapped around by circuit breaker decorator
   * @param handler                Callback method to collect the response when the asynchronous response is ready
   */
  void getByIndex(GetCellByIndex indexLookup, Optional<T> routeKey, Optional<U> intentData,
      Optional<V> circuitBreakerSettings,
      BiConsumer<PipelinedResponse<StoreOperationResponse<List<Cell>>>, Throwable> handler);

  /**
   * Get underlying AsyncStoreClient that is going to be used for making query for a specific route key.
   *
   * @param routeKey An {@link Optional} route key which is used for selecting the {@link ReplicaSet} by {@link
   *                 HotRouter} for this operation
   *
   * @return list of store client instances which will be tried out in sequence based on route key
   *
   * @throws PipelinedStoreDataCorruptException on handling data corruptions which should be recovered outside of store
   *                                            client
   */
  List<AsyncStoreClient> getAsyncStoreClient(Optional<T> routeKey)
      throws PipelinedStoreDataCorruptException;

  /**
   * Get underlying AsyncStoreClient that is going to be used for making query for a specific a site.
   *
   * @param site siteId to get specific store client while writing scripts
   *
   * @return an instance of store client with reference to the site passed as parameter
   *
   */
  AsyncStoreClient getAsyncStoreClient(SiteId site);

  /**
   * Shutdown all resources
   *
   * @throws IOException          io exception on shutdown
   * @throws InterruptedException interrupt while shutting down
   */
  void shutdown() throws IOException, InterruptedException;
}
