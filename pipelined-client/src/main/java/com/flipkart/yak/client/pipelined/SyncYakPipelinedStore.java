package com.flipkart.yak.client.pipelined;

import com.flipkart.yak.client.pipelined.decorator.circuitbreakers.CircuitBreakerDecorator;
import com.flipkart.yak.client.pipelined.intent.IntentStoreClient;
import com.flipkart.yak.client.pipelined.models.CircuitBreakerSettings;
import com.flipkart.yak.client.pipelined.models.IntentWriteRequest;
import com.flipkart.yak.client.pipelined.models.PipelinedResponse;
import com.flipkart.yak.client.pipelined.models.ReplicaSet;
import com.flipkart.yak.client.pipelined.models.StoreOperationResponse;
import com.flipkart.yak.client.pipelined.route.HotRouter;
import com.flipkart.yak.models.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * This is just a wrapper around {@link YakPipelinedStore} if your application want to handle in a synchronous way
 *
 * @param <T> {@link Class} Route Key data type which is used by {@link HotRouter} to select the route
 * @param <U> {@link IntentWriteRequest} Type of Intent Data based on {@link IntentStoreClient} implementation which can
 *            be used to persist in intent store
 * @param <V> {@link CircuitBreakerSettings} Type of Circuit breaker settings based on implementation of {@link
 *            CircuitBreakerDecorator} which is used to wrap around the api calls select the route
 */
public interface SyncYakPipelinedStore<T, U extends IntentWriteRequest, V extends CircuitBreakerSettings> {

  /**
   * Used to perform Increment operations on a single row atomically
   * If the column value does not yet exist it is initialized to amount and written to the specified column.
   *
   * @param routeKey               An {@link Optional} route key which is used for selecting the {@link ReplicaSet} by
   *                               {@link HotRouter} for this operation
   * @param intentData             An {@link Optional} {@link IntentWriteRequest} which is written by specific
   *                               implementation {@link IntentStoreClient}
   * @param circuitBreakerSettings An {@link Optional} {@link CircuitBreakerSettings} used by the circuit breaker if
   *                               wrapped around by circuit breaker decorator
   * @param incrementData          The {@link IncrementData} object that specifies which column to increment and by what value
   *
   * @return {@link PipelinedResponse} with error response if fails, else {@link ResultMap} response
   */
  PipelinedResponse<StoreOperationResponse<ResultMap>> increment(IncrementData incrementData,
      Optional<T> routeKey, Optional<U> intentData, Optional<V> circuitBreakerSettings)
      throws ExecutionException, InterruptedException, TimeoutException;

  /**
   * @param data                   {@link StoreData} to put the row
   * @param routeKey               An {@link Optional} route key which is used for selecting the {@link ReplicaSet} by
   *                               {@link HotRouter} for this operation
   * @param intentData             An {@link Optional} {@link IntentWriteRequest} which is written by specific
   *                               implementation {@link IntentStoreClient}
   * @param circuitBreakerSettings An {@link Optional} {@link CircuitBreakerSettings} used by the circuit breaker if
   *                               wrapped around by circuit breaker decorator
   *
   * @return {@link PipelinedResponse} with error response if fails, else null response
   *
   * @throws ExecutionException   if upstream {@link java.util.concurrent.CompletableFuture} throws with failure
   * @throws InterruptedException if upstream {@link java.util.concurrent.CompletableFuture} was interrupted
   */

  PipelinedResponse<StoreOperationResponse<Void>> put(StoreData data, Optional<T> routeKey,
      Optional<U> intentData, Optional<V> circuitBreakerSettings)
      throws ExecutionException, InterruptedException, TimeoutException;

  /**
   * @param data                   {@link List} of {@link StoreData} to put the rows in batches
   * @param routeKey               An {@link Optional} route key which is used for selecting the {@link ReplicaSet} by
   *                               {@link HotRouter} for this operation
   * @param intentData             An {@link Optional} {@link IntentWriteRequest} which is written by specific
   *                               implementation {@link IntentStoreClient}
   * @param circuitBreakerSettings An {@link Optional} {@link CircuitBreakerSettings} used by the circuit breaker if
   *                               wrapped around by circuit breaker decorator
   *
   * @return {@link PipelinedResponse} with error response if fails, else null response for each of the individual puts
   *
   * @throws ExecutionException   if upstream {@link java.util.concurrent.CompletableFuture} throws with failure
   * @throws InterruptedException if upstream {@link java.util.concurrent.CompletableFuture} was interrupted
   */
  PipelinedResponse<List<StoreOperationResponse<Void>>> put(List<StoreData> data, Optional<T> routeKey,
      Optional<U> intentData, Optional<V> circuitBreakerSettings)
      throws ExecutionException, InterruptedException, TimeoutException;

  /**
   * @param data                   {@link CheckAndStoreData} checkes row/family/qualifier value matches the expected
   *                               value and updates if matches else responds with false
   * @param routeKey               An {@link Optional} route key which is used for selecting the {@link ReplicaSet} by
   *                               {@link HotRouter} for this operation
   * @param intentData             An {@link Optional} {@link IntentWriteRequest} which is written by specific
   *                               implementation {@link IntentStoreClient}
   * @param circuitBreakerSettings An {@link Optional} {@link CircuitBreakerSettings} used by the circuit breaker if
   *                               wrapped around by circuit breaker decorator
   *
   * @return {@link PipelinedResponse} with error response if fails, else boolean response based on the outcome of cas
   * operation
   *
   * @throws ExecutionException   if upstream {@link java.util.concurrent.CompletableFuture} throws with failure
   * @throws InterruptedException if upstream {@link java.util.concurrent.CompletableFuture} was interrupted
   */
  PipelinedResponse<StoreOperationResponse<Boolean>> checkAndPut(CheckAndStoreData data, Optional<T> routeKey,
      Optional<U> intentData, Optional<V> circuitBreakerSettings)
      throws ExecutionException, InterruptedException, TimeoutException;

  /**
   * @param data                   {@link StoreData} object for identifying the row to append the family/qualifier to
   * @param routeKey               An {@link Optional} route key which is used for selecting the {@link ReplicaSet} by
   *                               {@link HotRouter} for this operation
   * @param intentData             An {@link Optional} {@link IntentWriteRequest} which is written by specific
   *                               implementation {@link IntentStoreClient}
   * @param circuitBreakerSettings An {@link Optional} {@link CircuitBreakerSettings} used by the circuit breaker if
   *                               wrapped around by circuit breaker decorator
   *
   * @return {@link PipelinedResponse} with error response if fails, else {@link ResultMap} response based on outcome of
   * append operation
   *
   * @throws ExecutionException   if upstream {@link java.util.concurrent.CompletableFuture} throws with failure
   * @throws InterruptedException if upstream {@link java.util.concurrent.CompletableFuture} was interrupted
   */
  PipelinedResponse<StoreOperationResponse<ResultMap>> append(StoreData data, Optional<T> routeKey,
      Optional<U> intentData, Optional<V> circuitBreakerSettings)
      throws ExecutionException, InterruptedException, TimeoutException;

  /**
   * @param data                   {@link List} of {@link DeleteData} to delete the rows in batches
   * @param routeKey               An {@link Optional} route key which is used for selecting the {@link ReplicaSet} by
   *                               {@link HotRouter} for this operation
   * @param intentData             An {@link Optional} {@link IntentWriteRequest} which is written by specific
   *                               implementation {@link IntentStoreClient}
   * @param circuitBreakerSettings An {@link Optional} {@link CircuitBreakerSettings} used by the circuit breaker if
   *                               wrapped around by circuit breaker decorator
   *
   * @return {@link PipelinedResponse} with error response if fails, else null response for each of the delete operation
   * in the {@link List} of {@link DeleteData}
   *
   * @throws ExecutionException   if upstream {@link java.util.concurrent.CompletableFuture} throws with failure
   * @throws InterruptedException if upstream {@link java.util.concurrent.CompletableFuture} was interrupted
   */
  PipelinedResponse<List<StoreOperationResponse<Void>>> delete(List<DeleteData> data, Optional<T> routeKey,
      Optional<U> intentData, Optional<V> circuitBreakerSettings)
      throws ExecutionException, InterruptedException, TimeoutException;

  /**
   * @param data                   {@link CheckAndStoreData} checkes row/family/qualifier value matches the expected
   *                               value and deletes if matches else responds with false
   * @param routeKey               An {@link Optional} route key which is used for selecting the {@link ReplicaSet} by
   *                               {@link HotRouter} for this operation
   * @param intentData             An {@link Optional} {@link IntentWriteRequest} which is written by specific
   *                               implementation {@link IntentStoreClient}
   * @param circuitBreakerSettings An {@link Optional} {@link CircuitBreakerSettings} used by the circuit breaker if
   *                               wrapped around by circuit breaker decorator
   *
   * @return {@link PipelinedResponse} with error response if fails, else boolean response based on the outcome of
   * operation
   *
   * @throws ExecutionException   if upstream {@link java.util.concurrent.CompletableFuture} throws with failure
   * @throws InterruptedException if upstream {@link java.util.concurrent.CompletableFuture} was interrupted
   */
  PipelinedResponse<StoreOperationResponse<Boolean>> checkAndDelete(CheckAndDeleteData data,
      Optional<T> routeKey, Optional<U> intentData, Optional<V> circuitBreakerSettings)
      throws ExecutionException, InterruptedException, TimeoutException;

  /**
   * @param data                   {@link ScanData} scan data to scan the table
   * @param routeKey               An {@link Optional} route key which is used for selecting the {@link ReplicaSet} by
   *                               {@link HotRouter} for this operation
   * @param intentData             An {@link Optional} {@link IntentWriteRequest} which is written by specific
   *                               implementation {@link IntentStoreClient}
   * @param circuitBreakerSettings An {@link Optional} {@link CircuitBreakerSettings} used by the circuit breaker if
   *                               wrapped around by circuit breaker decorator
   *
   * @return {@link PipelinedResponse} with error response if fails, else {@link ResultMap} response
   *
   * @throws ExecutionException   if upstream {@link java.util.concurrent.CompletableFuture} throws with failure
   * @throws InterruptedException if upstream {@link java.util.concurrent.CompletableFuture} was interrupted
   */
  PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>> scan(ScanData data, Optional<T> routeKey,
      Optional<U> intentData, Optional<V> circuitBreakerSettings)
      throws ExecutionException, InterruptedException, TimeoutException;

  /**
   * @param <X>                    {@link GetRow} and its subclasses
   * @param row                    {@link GetRow} and its sub classes to fetch the row/family/qualifier
   * @param routeKey               An {@link Optional} route key which is used for selecting the {@link ReplicaSet} by
   *                               {@link HotRouter} for this operation
   * @param intentData             An {@link Optional} {@link IntentWriteRequest} which is written by specific
   *                               implementation {@link IntentStoreClient}
   * @param circuitBreakerSettings An {@link Optional} {@link CircuitBreakerSettings} used by the circuit breaker if
   *                               wrapped around by circuit breaker decorator
   *
   * @return {@link PipelinedResponse} with error response if fails, else {@link ResultMap} response
   *
   * @throws ExecutionException   if upstream {@link java.util.concurrent.CompletableFuture} throws with failure
   * @throws InterruptedException if upstream {@link java.util.concurrent.CompletableFuture} was interrupted
   */
  <X extends GetRow> PipelinedResponse<StoreOperationResponse<ResultMap>> get(X row, Optional<T> routeKey,
      Optional<U> intentData, Optional<V> circuitBreakerSettings)
      throws ExecutionException, InterruptedException, TimeoutException;

  /**
   * @param rows                   {@link List} of {@link GetRow} and its sub classes to fetch the row/family/qualifier
   * @param routeKey               An {@link Optional} route key which is used for selecting the {@link ReplicaSet} by
   *                               {@link HotRouter} for this operation
   * @param intentData             An {@link Optional} {@link IntentWriteRequest} which is written by specific
   *                               implementation {@link IntentStoreClient}
   * @param circuitBreakerSettings An {@link Optional} {@link CircuitBreakerSettings} used by the circuit breaker if
   *                               wrapped around by circuit breaker decorator
   *
   * @return {@link PipelinedResponse} with error response if fails, else {@link ResultMap} response for each of the
   * rows individually
   *
   * @throws ExecutionException   if upstream {@link java.util.concurrent.CompletableFuture} throws with failure
   * @throws InterruptedException if upstream {@link java.util.concurrent.CompletableFuture} was interrupted
   */
  PipelinedResponse<List<StoreOperationResponse<ResultMap>>> get(List<? extends GetRow> rows,
      Optional<T> routeKey, Optional<U> intentData, Optional<V> circuitBreakerSettings)
      throws ExecutionException, InterruptedException, TimeoutException;

  /**
   * @param indexLookup            {@link GetColumnsMapByIndex} for looking up the {@link ColumnsMap} by index key
   * @param routeKey               An {@link Optional} route key which is used for selecting the {@link ReplicaSet} by
   *                               {@link HotRouter} for this operation
   * @param intentData             An {@link Optional} {@link IntentWriteRequest} which is written by specific
   *                               implementation {@link IntentStoreClient}
   * @param circuitBreakerSettings An {@link Optional} {@link CircuitBreakerSettings} used by the circuit breaker if
   *                               wrapped around by circuit breaker decorator
   *
   * @return {@link PipelinedResponse} with error response if fails, else {@link List} of {@link ColumnsMap} response
   *
   * @throws ExecutionException   if upstream {@link java.util.concurrent.CompletableFuture} throws with failure
   * @throws InterruptedException if upstream {@link java.util.concurrent.CompletableFuture} was interrupted
   */
  PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>> getByIndex(GetColumnsMapByIndex indexLookup,
      Optional<T> routeKey, Optional<U> intentData, Optional<V> circuitBreakerSettings)
      throws ExecutionException, InterruptedException, TimeoutException;

  /**
   * @param indexLookup            {@link GetCellByIndex} for looking up the {@link Cell} by index key
   * @param routeKey               An {@link Optional} route key which is used for selecting the {@link ReplicaSet} by
   *                               {@link HotRouter} for this operation
   * @param intentData             An {@link Optional} {@link IntentWriteRequest} which is written by specific
   *                               implementation {@link IntentStoreClient}
   * @param circuitBreakerSettings An {@link Optional} {@link CircuitBreakerSettings} used by the circuit breaker if
   *                               wrapped around by circuit breaker decorator
   *
   * @return {@link PipelinedResponse} with error response if fails, else {@link List} of {@link Cell} response
   *
   * @throws ExecutionException   if upstream {@link java.util.concurrent.CompletableFuture} throws with failure
   * @throws InterruptedException if upstream {@link java.util.concurrent.CompletableFuture} was interrupted
   */
  PipelinedResponse<StoreOperationResponse<List<Cell>>> getByIndex(GetCellByIndex indexLookup,
      Optional<T> routeKey, Optional<U> intentData, Optional<V> circuitBreakerSettings)
      throws ExecutionException, InterruptedException, TimeoutException;

  /**
   * Shutdown all resources
   *
   * @throws IOException          io exception on shutdown
   * @throws InterruptedException interrupt while shutting down
   */
  void shutdown() throws IOException, InterruptedException;
}
