package com.flipkart.yak.client.pipelined.decorator.intent;

import com.flipkart.yak.client.AsyncStoreClient;
import com.flipkart.yak.client.pipelined.YakPipelinedStore;
import com.flipkart.yak.client.pipelined.exceptions.PipelinedStoreDataCorruptException;
import com.flipkart.yak.client.pipelined.intent.IntentStoreClient;
import com.flipkart.yak.client.pipelined.models.CircuitBreakerSettings;
import com.flipkart.yak.client.pipelined.models.IntentWriteRequest;
import com.flipkart.yak.client.pipelined.models.PipelinedResponse;
import com.flipkart.yak.client.pipelined.models.SiteId;
import com.flipkart.yak.client.pipelined.models.StoreOperationResponse;
import com.flipkart.yak.models.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

@SuppressWarnings({"java:S3740", "common-java:DuplicatedBlocks"})
public abstract class IntentStoreDecorator<T, U extends IntentWriteRequest, V extends CircuitBreakerSettings>
    implements YakPipelinedStore<T, U, V> {
  protected YakPipelinedStore pipelinedStore;
  protected IntentStoreClient intentStoreClient;

  public IntentStoreDecorator(YakPipelinedStore pipelinedStore, IntentStoreClient intentStoreClient) {
    this.pipelinedStore = pipelinedStore;
    this.intentStoreClient = intentStoreClient;
  }

  @Override public void increment(IncrementData incrementData, Optional<T> routeMeta, Optional<U> intentData,
                                  Optional<V> circuitBreakerSettings,
                                  BiConsumer<PipelinedResponse<StoreOperationResponse<ResultMap>>, Throwable> handler) {
    pipelinedStore.increment(incrementData, routeMeta, intentData, circuitBreakerSettings, handler);
  }

  @Override
  public void scan(ScanData data, Optional<T> routeMeta, Optional<U> intentData, Optional<V> circuitBreakerSettings,
                   BiConsumer<PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>>, Throwable> handler) {
    pipelinedStore.scan(data, routeMeta, intentData, circuitBreakerSettings, handler);
  }

  @Override public <X extends GetRow> void get(X row, Optional<T> routeMeta, Optional<U> intentData,
                                               Optional<V> circuitBreakerSettings,
                                               BiConsumer<PipelinedResponse<StoreOperationResponse<ResultMap>>, Throwable> handler) {
    pipelinedStore.get(row, routeMeta, intentData, circuitBreakerSettings, handler);
  }

  @Override public void get(List<? extends GetRow> rows, Optional<T> routeMeta, Optional<U> intentData,
                            Optional<V> circuitBreakerSettings,
                            BiConsumer<PipelinedResponse<List<StoreOperationResponse<ResultMap>>>, Throwable> handler) {
    pipelinedStore.get(rows, routeMeta, intentData, circuitBreakerSettings, handler);
  }

  @Override public void getByIndex(GetColumnsMapByIndex indexLookup, Optional<T> routeMeta, Optional<U> intentData,
                                   Optional<V> circuitBreakerSettings,
                                   BiConsumer<PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>>, Throwable> handler) {
    pipelinedStore.getByIndex(indexLookup, routeMeta, intentData, circuitBreakerSettings, handler);
  }

  @Override public void getByIndex(GetCellByIndex indexLookup, Optional<T> routeMeta, Optional<U> intentData,
                                   Optional<V> circuitBreakerSettings,
                                   BiConsumer<PipelinedResponse<StoreOperationResponse<List<Cell>>>, Throwable> handler) {
    pipelinedStore.getByIndex(indexLookup, routeMeta, intentData, circuitBreakerSettings, handler);
  }

  @Override public List<AsyncStoreClient> getAsyncStoreClient(Optional<T> routeMeta)
      throws PipelinedStoreDataCorruptException {
    return pipelinedStore.getAsyncStoreClient(routeMeta);
  }

  @Override public AsyncStoreClient getAsyncStoreClient(SiteId siteId) {
    return pipelinedStore.getAsyncStoreClient(siteId);
  }

  @Override public void shutdown() {
  }
}
