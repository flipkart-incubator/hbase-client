package com.flipkart.yak.client.pipelined;

import com.flipkart.yak.client.pipelined.exceptions.PipelinedClientInitializationException;
import com.flipkart.yak.client.pipelined.models.CircuitBreakerSettings;
import com.flipkart.yak.client.pipelined.models.IntentWriteRequest;
import com.flipkart.yak.client.pipelined.models.PipelinedResponse;
import com.flipkart.yak.client.pipelined.models.StoreOperationResponse;
import com.flipkart.yak.models.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

/**
 * {@inheritDoc}
 */
@SuppressWarnings({"common-java:DuplicatedBlocks", "java:S3740"})
public class SyncYakPipelinedStoreImpl<T, U extends IntentWriteRequest, V extends CircuitBreakerSettings>
    implements SyncYakPipelinedStore<T, U, V> {

  private final String UNEXPECTED_ERROR_MESSAGE = "Received unexpected response";
  private final YakPipelinedStore pipelinedStore;
  private int readTimeoutInMillis = 60000; //Default
  private int writeTimeoutInMillis = 60000; //Default

  public SyncYakPipelinedStoreImpl(YakPipelinedStore pipelinedStore) {
    this.pipelinedStore = pipelinedStore;
  }

  public SyncYakPipelinedStoreImpl<T, U, V> setReadTimeoutInMillis(int readTimeoutInMillis) {
    this.readTimeoutInMillis = readTimeoutInMillis;
    return this;
  }

  public SyncYakPipelinedStoreImpl<T, U, V> setWriteTimeoutInMillis(int writeTimeoutInMillis) {
    this.writeTimeoutInMillis = writeTimeoutInMillis;
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PipelinedResponse<StoreOperationResponse<ResultMap>> increment(IncrementData incrementData,
      Optional<T> routeKey, Optional<U> intentData, Optional<V> circuitBreakerSettings)
      throws ExecutionException, InterruptedException, TimeoutException {
      CompletableFuture<PipelinedResponse<StoreOperationResponse<ResultMap>>> completableFuture =
          new CompletableFuture<>();

      pipelinedStore.increment(incrementData, routeKey, intentData, circuitBreakerSettings,
          (BiConsumer<PipelinedResponse<StoreOperationResponse<ResultMap>>, Throwable>) (response, error) -> {
              if (error != null) {
                  completableFuture.completeExceptionally(error);
              } else if (response != null) {
                  completableFuture.complete(response);
              } else {
                  error = new PipelinedClientInitializationException(UNEXPECTED_ERROR_MESSAGE);
                  completableFuture.completeExceptionally(error);
              }
          });

      return completableFuture.get(readTimeoutInMillis, TimeUnit.MILLISECONDS);
  }

  /**
   * {@inheritDoc}
   */
  @Override public PipelinedResponse<StoreOperationResponse<Void>> put(StoreData data, Optional<T> routeKey,
      Optional<U> intentData, Optional<V> circuitBreakerSettings)
      throws ExecutionException, InterruptedException, TimeoutException {

    CompletableFuture<PipelinedResponse<StoreOperationResponse<Void>>> completableFuture = new CompletableFuture<>();

    pipelinedStore.put(data, routeKey, intentData, circuitBreakerSettings,
            (BiConsumer<PipelinedResponse<StoreOperationResponse<Void>>, Throwable>) (response, error) -> {
              if (error != null) {
                completableFuture.completeExceptionally(error);
              } else if (response != null) {
                completableFuture.complete(response);
              } else {
                error = new PipelinedClientInitializationException(UNEXPECTED_ERROR_MESSAGE);
                completableFuture.completeExceptionally(error);
              }
            });

    return completableFuture.get(writeTimeoutInMillis, TimeUnit.MILLISECONDS);
  }

  /**
   * {@inheritDoc}
   */
  @Override public PipelinedResponse<List<StoreOperationResponse<Void>>> put(List<StoreData> data, Optional<T> routeKey,
      Optional<U> intentData, Optional<V> circuitBreakerSettings)
      throws ExecutionException, InterruptedException, TimeoutException {

    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<Void>>>> completableFuture =
        new CompletableFuture<>();

    pipelinedStore.put(data, routeKey, intentData, circuitBreakerSettings,
            (BiConsumer<PipelinedResponse<List<StoreOperationResponse<Void>>>, Throwable>) (response, error) -> {
              if (error != null) {
                completableFuture.completeExceptionally(error);
              } else if (response != null) {
                completableFuture.complete(response);
              } else {
                error = new PipelinedClientInitializationException(UNEXPECTED_ERROR_MESSAGE);
                completableFuture.completeExceptionally(error);
              }
            });

    return completableFuture.get(writeTimeoutInMillis, TimeUnit.MILLISECONDS);
  }

  /**
   * {@inheritDoc}
   */
  @Override public PipelinedResponse<StoreOperationResponse<Boolean>> checkAndPut(CheckAndStoreData data,
      Optional<T> routeKey, Optional<U> intentData, Optional<V> circuitBreakerSettings)
      throws ExecutionException, InterruptedException, TimeoutException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> completableFuture = new CompletableFuture<>();

    pipelinedStore.checkAndPut(data, routeKey, intentData, circuitBreakerSettings,
            (BiConsumer<PipelinedResponse<StoreOperationResponse<Boolean>>, Throwable>) (response, error) -> {
              if (error != null) {
                completableFuture.completeExceptionally(error);
              } else if (response != null) {
                completableFuture.complete(response);
              } else {
                error = new PipelinedClientInitializationException(UNEXPECTED_ERROR_MESSAGE);
                completableFuture.completeExceptionally(error);
              }
            });

    return completableFuture.get(writeTimeoutInMillis, TimeUnit.MILLISECONDS);
  }

  /**
   * {@inheritDoc}
   */
  @Override public PipelinedResponse<StoreOperationResponse<ResultMap>> append(StoreData data, Optional<T> routeKey,
      Optional<U> intentData, Optional<V> circuitBreakerSettings)
      throws ExecutionException, InterruptedException, TimeoutException {

    CompletableFuture<PipelinedResponse<StoreOperationResponse<ResultMap>>> completableFuture =
        new CompletableFuture<>();

    pipelinedStore.append(data, routeKey, intentData, circuitBreakerSettings,
            (BiConsumer<PipelinedResponse<StoreOperationResponse<ResultMap>>, Throwable>) (response, error) -> {
              if (error != null) {
                completableFuture.completeExceptionally(error);
              } else if (response != null) {
                completableFuture.complete(response);
              } else {
                error = new PipelinedClientInitializationException(UNEXPECTED_ERROR_MESSAGE);
                completableFuture.completeExceptionally(error);
              }
            });

    return completableFuture.get(writeTimeoutInMillis, TimeUnit.MILLISECONDS);
  }

  /**
   * {@inheritDoc}
   */
  @Override public PipelinedResponse<List<StoreOperationResponse<Void>>> delete(List<DeleteData> data,
      Optional<T> routeKey, Optional<U> intentData, Optional<V> circuitBreakerSettings)
      throws ExecutionException, InterruptedException, TimeoutException {

    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<Void>>>> completableFuture =
        new CompletableFuture<>();

    pipelinedStore.delete(data, routeKey, intentData, circuitBreakerSettings,
            (BiConsumer<PipelinedResponse<List<StoreOperationResponse<Void>>>, Throwable>) (response, error) -> {
              if (error != null) {
                completableFuture.completeExceptionally(error);
              } else if (response != null) {
                completableFuture.complete(response);
              } else {
                error = new PipelinedClientInitializationException(UNEXPECTED_ERROR_MESSAGE);
                completableFuture.completeExceptionally(error);
              }
            });

    return completableFuture.get(writeTimeoutInMillis, TimeUnit.MILLISECONDS);
  }

  /**
   * {@inheritDoc}
   */
  @Override public PipelinedResponse<StoreOperationResponse<Boolean>> checkAndDelete(CheckAndDeleteData data,
      Optional<T> routeKey, Optional<U> intentData, Optional<V> circuitBreakerSettings)
      throws ExecutionException, InterruptedException, TimeoutException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> completableFuture = new CompletableFuture<>();

    pipelinedStore.checkAndDelete(data, routeKey, intentData, circuitBreakerSettings,
            (BiConsumer<PipelinedResponse<StoreOperationResponse<Boolean>>, Throwable>) (response, error) -> {
              if (error != null) {
                completableFuture.completeExceptionally(error);
              } else if (response != null) {
                completableFuture.complete(response);
              } else {
                error = new PipelinedClientInitializationException(UNEXPECTED_ERROR_MESSAGE);
                completableFuture.completeExceptionally(error);
              }
            });

    return completableFuture.get(writeTimeoutInMillis, TimeUnit.MILLISECONDS);
  }

  @Override public PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>> scan(ScanData data, Optional<T> routeKey,
      Optional<U> intentData, Optional<V> circuitBreakerSettings)
      throws ExecutionException, InterruptedException, TimeoutException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>>> completableFuture =
        new CompletableFuture<>();

    pipelinedStore.scan(data, routeKey, intentData, circuitBreakerSettings,
            (BiConsumer<PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>>, Throwable>) (response, error) -> {
              if (error != null) {
                completableFuture.completeExceptionally(error);
              } else if (response != null) {
                completableFuture.complete(response);
              } else {
                error = new PipelinedClientInitializationException(UNEXPECTED_ERROR_MESSAGE);
                completableFuture.completeExceptionally(error);
              }
            });

    return completableFuture.get(readTimeoutInMillis, TimeUnit.MILLISECONDS);
  }

  /**
   * {@inheritDoc}
   */
  @Override public <X extends GetRow> PipelinedResponse<StoreOperationResponse<ResultMap>> get(X row,
      Optional<T> routeKey, Optional<U> intentData, Optional<V> circuitBreakerSettings)
      throws ExecutionException, InterruptedException, TimeoutException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<ResultMap>>> completableFuture =
        new CompletableFuture<>();

    pipelinedStore.get(row, routeKey, intentData, circuitBreakerSettings,
            (BiConsumer<PipelinedResponse<StoreOperationResponse<ResultMap>>, Throwable>) (response, error) -> {
              if (error != null) {
                completableFuture.completeExceptionally(error);
              } else if (response != null) {
                completableFuture.complete(response);
              } else {
                error = new PipelinedClientInitializationException(UNEXPECTED_ERROR_MESSAGE);
                completableFuture.completeExceptionally(error);
              }
            });

    return completableFuture.get(readTimeoutInMillis, TimeUnit.MILLISECONDS);
  }

  /**
   * {@inheritDoc}
   */
  @Override public PipelinedResponse<List<StoreOperationResponse<ResultMap>>> get(List<? extends GetRow> rows,
      Optional<T> routeKey, Optional<U> intentData, Optional<V> circuitBreakerSettings)
      throws ExecutionException, InterruptedException, TimeoutException {

    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<ResultMap>>>> completableFuture =
        new CompletableFuture<>();

    pipelinedStore.get(rows, routeKey, intentData, circuitBreakerSettings,
            (BiConsumer<PipelinedResponse<List<StoreOperationResponse<ResultMap>>>, Throwable>) (response, error) -> {
              if (error != null) {
                completableFuture.completeExceptionally(error);
              } else if (response != null) {
                completableFuture.complete(response);
              } else {
                error = new PipelinedClientInitializationException(UNEXPECTED_ERROR_MESSAGE);
                completableFuture.completeExceptionally(error);
              }
            });

    return completableFuture.get(readTimeoutInMillis, TimeUnit.MILLISECONDS);
  }

  /**
   * {@inheritDoc}
   */
  @Override public PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>> getByIndex(
      GetColumnsMapByIndex indexLookup, Optional<T> routeKey, Optional<U> intentData,
      Optional<V> circuitBreakerSettings)
      throws ExecutionException, InterruptedException, TimeoutException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>>> completableFuture =
        new CompletableFuture<>();

    pipelinedStore.getByIndex(indexLookup, routeKey, intentData, circuitBreakerSettings,
            (BiConsumer<PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>>, Throwable>) (response, error) -> {
              if (error != null) {
                completableFuture.completeExceptionally(error);
              } else if (response != null) {
                completableFuture.complete(response);
              } else {
                error = new PipelinedClientInitializationException(UNEXPECTED_ERROR_MESSAGE);
                completableFuture.completeExceptionally(error);
              }
            });

    return completableFuture.get(readTimeoutInMillis, TimeUnit.MILLISECONDS);
  }

  /**
   * {@inheritDoc}
   */
  @Override public PipelinedResponse<StoreOperationResponse<List<Cell>>> getByIndex(GetCellByIndex indexLookup,
      Optional<T> routeKey, Optional<U> intentData, Optional<V> circuitBreakerSettings)
      throws ExecutionException, InterruptedException, TimeoutException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<List<Cell>>>> completableFuture =
        new CompletableFuture<>();

    pipelinedStore.getByIndex(indexLookup, routeKey, intentData, circuitBreakerSettings,
            (BiConsumer<PipelinedResponse<StoreOperationResponse<List<Cell>>>, Throwable>) (response, error) -> {
              if (error != null) {
                completableFuture.completeExceptionally(error);
              } else if (response != null) {
                completableFuture.complete(response);
              } else {
                error = new PipelinedClientInitializationException(UNEXPECTED_ERROR_MESSAGE);
                completableFuture.completeExceptionally(error);
              }
            });

    return completableFuture.get(readTimeoutInMillis, TimeUnit.MILLISECONDS);
  }

    @Override
    public void shutdown() throws IOException, InterruptedException {
        pipelinedStore.shutdown();
    }
}
