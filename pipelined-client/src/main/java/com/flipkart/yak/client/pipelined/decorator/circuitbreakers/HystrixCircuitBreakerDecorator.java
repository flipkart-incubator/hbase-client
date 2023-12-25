package com.flipkart.yak.client.pipelined.decorator.circuitbreakers;

import com.flipkart.yak.client.pipelined.YakPipelinedStore;
import com.flipkart.yak.client.pipelined.circuitbreakers.StoreClientHystrixObservableCommand;
import com.flipkart.yak.client.pipelined.decorator.intent.YakIntentStoreDecorator;
import com.flipkart.yak.client.pipelined.models.HystrixSettings;
import com.flipkart.yak.client.pipelined.models.IntentWriteRequest;
import com.flipkart.yak.client.pipelined.models.PipelinedRequest;
import com.flipkart.yak.client.pipelined.models.PipelinedResponse;
import com.flipkart.yak.client.pipelined.models.StoreOperationResponse;
import com.flipkart.yak.models.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import rx.Observable;
import rx.Observer;

@SuppressWarnings("java:S3740")
public class HystrixCircuitBreakerDecorator<T, U extends IntentWriteRequest>
    extends CircuitBreakerDecorator<T, U, HystrixSettings> {

  public HystrixCircuitBreakerDecorator(YakPipelinedStore pipelinedStore) {
    super(pipelinedStore);
  }

  private <T> void handleHystrixRequest(HystrixSettings hystrixSettings,
      BiConsumer<PipelinedResponse<T>, Throwable> handler, PipelinedRequest request) {
    Observable<PipelinedResponse> observable =
        new StoreClientHystrixObservableCommand(pipelinedStore, request, hystrixSettings.getSettings()).observe();
    observable.subscribe(new Observer<PipelinedResponse>() {
      PipelinedResponse response;

      @Override public void onCompleted() {
        handler.accept(response, null);
      }

      @Override public void onError(Throwable e) {
        handler.accept(null, e.getCause());
      }

      @Override public void onNext(PipelinedResponse pipelinedResponse) {
        this.response = pipelinedResponse;
      }
    });
  }

  private Optional<HystrixSettings> getHystrixSettings(Optional<HystrixSettings> storeHystrixSettings,
      Optional<U> intentData) {
    if (pipelinedStore instanceof YakIntentStoreDecorator) {
      if (intentData.isPresent() && intentData.get().getCircuitBreakerOptional().isPresent()) {
        return intentData.get().getCircuitBreakerOptional();
      } else {
        return Optional.empty();
      }
    } else {
      return storeHystrixSettings;
    }
  }

  @Override
  public void increment(IncrementData incrementData, Optional<T> routeKey,
                        Optional<U> intentData, Optional<HystrixSettings> circuitBreakerSettings,
                        BiConsumer<PipelinedResponse<StoreOperationResponse<ResultMap>>, Throwable> handler) {

    Optional<HystrixSettings> settings = getHystrixSettings(circuitBreakerSettings, intentData);

    if (settings.isPresent()) {
      List<Class> parameterTypes =
        Stream.of(incrementData.getClass(), routeKey.getClass(), intentData.getClass(), circuitBreakerSettings.getClass())
          .collect(Collectors.toList());
      List<Object> parameters =
        Stream.of(incrementData, routeKey, intentData, circuitBreakerSettings).collect(Collectors.toList());

      handleHystrixRequest(settings.get(), handler,
        new PipelinedRequest(INCREMENT_METHOD_NAME, parameterTypes, parameters));
    } else {
      super.increment(incrementData, routeKey, intentData, circuitBreakerSettings, handler);
    }
  }

  @Override public void put(StoreData data, Optional<T> routeKey, Optional<U> intentData,
      Optional<HystrixSettings> circuitBreakerSettings,
      BiConsumer<PipelinedResponse<StoreOperationResponse<Void>>, Throwable> handler) {

    Optional<HystrixSettings> settings = getHystrixSettings(circuitBreakerSettings, intentData);
    if (settings.isPresent()) {
      List<Class> parameterTypes =
          Stream.of(data.getClass(), routeKey.getClass(), intentData.getClass(), circuitBreakerSettings.getClass())
              .collect(Collectors.toList());
      List<Object> parameters =
          Stream.of(data, routeKey, intentData, circuitBreakerSettings).collect(Collectors.toList());

      handleHystrixRequest(settings.get(), handler, new PipelinedRequest(PUT_METHOD_NAME, parameterTypes, parameters));
    } else {
      super.put(data, routeKey, intentData, circuitBreakerSettings, handler);
    }
  }

  @Override public void put(List<StoreData> data, Optional<T> routeKey, Optional<U> intentData,
      Optional<HystrixSettings> circuitBreakerSettings,
      BiConsumer<PipelinedResponse<List<StoreOperationResponse<Void>>>, Throwable> handler) {

    Optional<HystrixSettings> settings = getHystrixSettings(circuitBreakerSettings, intentData);

    if (settings.isPresent()) {
      List<Class> parameterTypes =
          Stream.of(List.class, routeKey.getClass(), intentData.getClass(), circuitBreakerSettings.getClass())
              .collect(Collectors.toList());
      List<Object> parameters =
          Stream.of(data, routeKey, intentData, circuitBreakerSettings).collect(Collectors.toList());

      handleHystrixRequest(settings.get(), handler, new PipelinedRequest(PUT_METHOD_NAME, parameterTypes, parameters));
    } else {
      super.put(data, routeKey, intentData, circuitBreakerSettings, handler);
    }
  }

  @Override public void checkAndPut(CheckAndStoreData data, Optional<T> routeKey, Optional<U> intentData,
      Optional<HystrixSettings> circuitBreakerSettings,
      BiConsumer<PipelinedResponse<StoreOperationResponse<Boolean>>, Throwable> handler) {

    Optional<HystrixSettings> settings = getHystrixSettings(circuitBreakerSettings, intentData);

    if (settings.isPresent()) {
      List<Class> parameterTypes =
          Stream.of(data.getClass(), routeKey.getClass(), intentData.getClass(), circuitBreakerSettings.getClass())
              .collect(Collectors.toList());
      List<Object> parameters =
          Stream.of(data, routeKey, intentData, circuitBreakerSettings).collect(Collectors.toList());

      handleHystrixRequest(settings.get(), handler,
          new PipelinedRequest(CHECK_PUT_METHOD_NAME, parameterTypes, parameters));
    } else {
      super.checkAndPut(data, routeKey, intentData, circuitBreakerSettings, handler);
    }
  }

  @Override public void append(StoreData data, Optional<T> routeKey, Optional<U> intentData,
      Optional<HystrixSettings> circuitBreakerSettings,
      BiConsumer<PipelinedResponse<StoreOperationResponse<ResultMap>>, Throwable> handler) {

    Optional<HystrixSettings> settings = getHystrixSettings(circuitBreakerSettings, intentData);
    if (settings.isPresent()) {
      List<Class> parameterTypes =
          Stream.of(data.getClass(), routeKey.getClass(), intentData.getClass(), circuitBreakerSettings.getClass())
              .collect(Collectors.toList());
      List<Object> parameters =
          Stream.of(data, routeKey, intentData, circuitBreakerSettings).collect(Collectors.toList());

      handleHystrixRequest(settings.get(), handler,
          new PipelinedRequest(APPEND_METHOD_NAME, parameterTypes, parameters));
    } else {
      super.append(data, routeKey, intentData, circuitBreakerSettings, handler);
    }

  }

  @Override public void delete(List<DeleteData> data, Optional<T> routeKey, Optional<U> intentData,
      Optional<HystrixSettings> circuitBreakerSettings,
      BiConsumer<PipelinedResponse<List<StoreOperationResponse<Void>>>, Throwable> handler) {

    Optional<HystrixSettings> settings = getHystrixSettings(circuitBreakerSettings, intentData);
    if (settings.isPresent()) {
      List<Class> parameterTypes =
          Stream.of(List.class, routeKey.getClass(), intentData.getClass(), circuitBreakerSettings.getClass())
              .collect(Collectors.toList());
      List<Object> parameters =
          Stream.of(data, routeKey, intentData, circuitBreakerSettings).collect(Collectors.toList());

      handleHystrixRequest(settings.get(), handler,
          new PipelinedRequest(DELETE_METHOD_NAME, parameterTypes, parameters));
    } else {
      super.delete(data, routeKey, intentData, circuitBreakerSettings, handler);
    }
  }

  @Override public void checkAndDelete(CheckAndDeleteData data, Optional<T> routeKey, Optional<U> intentData,
      Optional<HystrixSettings> circuitBreakerSettings,
      BiConsumer<PipelinedResponse<StoreOperationResponse<Boolean>>, Throwable> handler) {

    Optional<HystrixSettings> settings = getHystrixSettings(circuitBreakerSettings, intentData);
    if (settings.isPresent()) {
      List<Class> parameterTypes =
          Stream.of(data.getClass(), routeKey.getClass(), intentData.getClass(), circuitBreakerSettings.getClass())
              .collect(Collectors.toList());
      List<Object> parameters =
          Stream.of(data, routeKey, intentData, circuitBreakerSettings).collect(Collectors.toList());

      handleHystrixRequest(settings.get(), handler,
          new PipelinedRequest(CHECK_DELETE_METHOD_NAME, parameterTypes, parameters));
    } else {
      super.checkAndDelete(data, routeKey, intentData, circuitBreakerSettings, handler);
    }
  }

  @Override public void scan(ScanData data, Optional<T> routeKey, Optional<U> intentData,
      Optional<HystrixSettings> circuitBreakerSettings,
      BiConsumer<PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>>, Throwable> handler) {

    Optional<HystrixSettings> settings = getHystrixSettings(circuitBreakerSettings, intentData);
    if (settings.isPresent()) {
      List<Class> parameterTypes =
          Stream.of(data.getClass(), routeKey.getClass(), intentData.getClass(), circuitBreakerSettings.getClass())
              .collect(Collectors.toList());
      List<Object> parameters =
          Stream.of(data, routeKey, intentData, circuitBreakerSettings).collect(Collectors.toList());

      handleHystrixRequest(settings.get(), handler, new PipelinedRequest(SCAN_METHOD_NAME, parameterTypes, parameters));
    } else {
      super.scan(data, routeKey, intentData, circuitBreakerSettings, handler);
    }
  }

  @Override public <X extends GetRow> void get(X row, Optional<T> routeKey, Optional<U> intentData,
      Optional<HystrixSettings> circuitBreakerSettings,
      BiConsumer<PipelinedResponse<StoreOperationResponse<ResultMap>>, Throwable> handler) {

    Optional<HystrixSettings> settings = getHystrixSettings(circuitBreakerSettings, intentData);
    if (settings.isPresent()) {
      Class<?> clazz = (!row.getClass().equals(GetRow.class)) ? GetRow.class : row.getClass();
      List<Class> parameterTypes =
          Stream.of(clazz, routeKey.getClass(), intentData.getClass(), circuitBreakerSettings.getClass())
              .collect(Collectors.toList());
      List<Object> parameters =
          Stream.of(row, routeKey, intentData, circuitBreakerSettings).collect(Collectors.toList());

      handleHystrixRequest(settings.get(), handler, new PipelinedRequest(GET_METHOD_NAME, parameterTypes, parameters));
    } else {
      super.get(row, routeKey, intentData, circuitBreakerSettings, handler);
    }
  }

  @Override public void get(List<? extends GetRow> rows, Optional<T> routeKey, Optional<U> intentData,
      Optional<HystrixSettings> circuitBreakerSettings,
      BiConsumer<PipelinedResponse<List<StoreOperationResponse<ResultMap>>>, Throwable> handler) {

    Optional<HystrixSettings> settings = getHystrixSettings(circuitBreakerSettings, intentData);
    if (settings.isPresent()) {
      List<Class> parameterTypes =
          Stream.of(List.class, routeKey.getClass(), intentData.getClass(), circuitBreakerSettings.getClass())
              .collect(Collectors.toList());
      List<Object> parameters =
          Stream.of(rows, routeKey, intentData, circuitBreakerSettings).collect(Collectors.toList());

      handleHystrixRequest(settings.get(), handler, new PipelinedRequest(GET_METHOD_NAME, parameterTypes, parameters));
    } else {
      super.get(rows, routeKey, intentData, circuitBreakerSettings, handler);
    }
  }

  @Override public void getByIndex(GetColumnsMapByIndex indexLookup, Optional<T> routeKey, Optional<U> intentData,
      Optional<HystrixSettings> circuitBreakerSettings,
      BiConsumer<PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>>, Throwable> handler) {

    Optional<HystrixSettings> settings = getHystrixSettings(circuitBreakerSettings, intentData);
    if (settings.isPresent()) {
      List<Class> parameterTypes = Stream
          .of(indexLookup.getClass(), routeKey.getClass(), intentData.getClass(), circuitBreakerSettings.getClass())
          .collect(Collectors.toList());
      List<Object> parameters =
          Stream.of(indexLookup, routeKey, intentData, circuitBreakerSettings).collect(Collectors.toList());

      handleHystrixRequest(settings.get(), handler,
          new PipelinedRequest(GET_INDEX_METHOD_NAME, parameterTypes, parameters));
    } else {
      super.getByIndex(indexLookup, routeKey, intentData, circuitBreakerSettings, handler);
    }
  }

  @Override public void getByIndex(GetCellByIndex indexLookup, Optional<T> routeKey, Optional<U> intentData,
      Optional<HystrixSettings> circuitBreakerSettings,
      BiConsumer<PipelinedResponse<StoreOperationResponse<List<Cell>>>, Throwable> handler) {

    Optional<HystrixSettings> settings = getHystrixSettings(circuitBreakerSettings, intentData);
    if (settings.isPresent()) {
      List<Class> parameterTypes = Stream
          .of(indexLookup.getClass(), routeKey.getClass(), intentData.getClass(), circuitBreakerSettings.getClass())
          .collect(Collectors.toList());
      List<Object> parameters =
          Stream.of(indexLookup, routeKey, intentData, circuitBreakerSettings).collect(Collectors.toList());

      handleHystrixRequest(settings.get(), handler,
          new PipelinedRequest(GET_INDEX_METHOD_NAME, parameterTypes, parameters));
    } else {
      super.getByIndex(indexLookup, routeKey, intentData, circuitBreakerSettings, handler);
    }
  }
}
