package com.flipkart.yak.client.pipelined.decorator.intent;

import com.flipkart.yak.client.pipelined.YakPipelinedStore;
import com.flipkart.yak.client.pipelined.intent.IntentStoreClient;
import com.flipkart.yak.client.pipelined.models.CircuitBreakerSettings;
import com.flipkart.yak.client.pipelined.models.PipelinedRequest;
import com.flipkart.yak.client.pipelined.models.PipelinedResponse;
import com.flipkart.yak.client.pipelined.models.StoreOperationResponse;
import com.flipkart.yak.client.pipelined.models.YakIntentWriteRequest;
import com.flipkart.yak.models.*;

import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("java:S3740")
public class YakIntentStoreDecorator<T, V extends CircuitBreakerSettings>
    extends IntentStoreDecorator<T, YakIntentWriteRequest, V> {
  private Class<?> storeClazz;

  public YakIntentStoreDecorator(YakPipelinedStore pipelinedStore, IntentStoreClient intentStoreClient) {
    super(pipelinedStore, intentStoreClient);
    storeClazz = pipelinedStore.getClass();
  }

  @SuppressWarnings("java:S3776")
  private <T> void handleIntentWrite(Optional<YakIntentWriteRequest> intentData,
      BiConsumer<PipelinedResponse<T>, Throwable> handler, PipelinedRequest request) {
    Class<?>[] parameterTypes = new Class[request.getParameters().size()];
    request.getParameterTypes().toArray(parameterTypes);

    if (intentData.isPresent()) {
      BiConsumer<PipelinedResponse<StoreOperationResponse<Void>>, Throwable> intentHandler = (response, error) -> {
        if (error == null && response != null && response.getOperationResponse() != null
            && response.getOperationResponse().getError() != null) {
          error = response.getOperationResponse().getError();
        }
        if (error != null) {
          handler.accept(null, error);
        } else {
          try {
            storeClazz.getMethod(request.getMethodName(), parameterTypes)
                .invoke(pipelinedStore, request.getParameters().toArray());
          } catch (Exception ex) {
            handler.accept(null, ex);
          }
        }
      };
      intentStoreClient.write(intentData.get(), intentHandler);
    } else {
      try {
        storeClazz.getMethod(request.getMethodName(), parameterTypes)
            .invoke(pipelinedStore, request.getParameters().toArray());
      } catch (Exception ex) {
        handler.accept(null, ex);
      }
    }
  }

  @Override
  public void increment(IncrementData incrementData, Optional<T> routeMeta, Optional<YakIntentWriteRequest> intentData,
                        Optional<V> circuitBreakerSettings,
                        BiConsumer<PipelinedResponse<StoreOperationResponse<ResultMap>>, Throwable> handler) {
    List<Class> parameterTypes = Stream
      .of(incrementData.getClass(), routeMeta.getClass(), intentData.getClass(), circuitBreakerSettings.getClass(),
        BiConsumer.class).collect(Collectors.toList());
    List<Object> parameters =
      Stream.of(incrementData, routeMeta, intentData, circuitBreakerSettings, handler).collect(Collectors.toList());

    handleIntentWrite(intentData, handler, new PipelinedRequest(INCREMENT_METHOD_NAME, parameterTypes, parameters));
  }

  @Override public void put(StoreData data, Optional<T> routeMeta, Optional<YakIntentWriteRequest> intentData,
                            Optional<V> circuitBreakerSettings,
                            BiConsumer<PipelinedResponse<StoreOperationResponse<Void>>, Throwable> handler) {

    List<Class> parameterTypes = Stream
        .of(data.getClass(), routeMeta.getClass(), intentData.getClass(), circuitBreakerSettings.getClass(),
            BiConsumer.class).collect(Collectors.toList());
    List<Object> parameters =
        Stream.of(data, routeMeta, intentData, circuitBreakerSettings, handler).collect(Collectors.toList());

    handleIntentWrite(intentData, handler, new PipelinedRequest(PUT_METHOD_NAME, parameterTypes, parameters));
  }

  @Override public void put(List<StoreData> data, Optional<T> routeMeta, Optional<YakIntentWriteRequest> intentData,
                            Optional<V> circuitBreakerSettings,
                            BiConsumer<PipelinedResponse<List<StoreOperationResponse<Void>>>, Throwable> handler) {
    List<Class> parameterTypes = Stream
        .of(List.class, routeMeta.getClass(), intentData.getClass(), circuitBreakerSettings.getClass(), BiConsumer.class)
        .collect(Collectors.toList());
    List<Object> parameters =
        Stream.of(data, routeMeta, intentData, circuitBreakerSettings, handler).collect(Collectors.toList());

    handleIntentWrite(intentData, handler, new PipelinedRequest(PUT_METHOD_NAME, parameterTypes, parameters));
  }

  @Override
  public void checkAndPut(CheckAndStoreData data, Optional<T> routeMeta, Optional<YakIntentWriteRequest> intentData,
                          Optional<V> circuitBreakerSettings,
                          BiConsumer<PipelinedResponse<StoreOperationResponse<Boolean>>, Throwable> handler) {
    List<Class> parameterTypes = Stream
        .of(data.getClass(), routeMeta.getClass(), intentData.getClass(), circuitBreakerSettings.getClass(),
            BiConsumer.class).collect(Collectors.toList());
    List<Object> parameters =
        Stream.of(data, routeMeta, intentData, circuitBreakerSettings, handler).collect(Collectors.toList());

    handleIntentWrite(intentData, handler, new PipelinedRequest(CHECK_PUT_METHOD_NAME, parameterTypes, parameters));
  }

  @Override public void append(StoreData data, Optional<T> routeMeta, Optional<YakIntentWriteRequest> intentData,
                               Optional<V> circuitBreakerSettings,
                               BiConsumer<PipelinedResponse<StoreOperationResponse<ResultMap>>, Throwable> handler) {
    List<Class> parameterTypes = Stream
        .of(data.getClass(), routeMeta.getClass(), intentData.getClass(), circuitBreakerSettings.getClass(),
            BiConsumer.class).collect(Collectors.toList());
    List<Object> parameters =
        Stream.of(data, routeMeta, intentData, circuitBreakerSettings, handler).collect(Collectors.toList());

    handleIntentWrite(intentData, handler, new PipelinedRequest(APPEND_METHOD_NAME, parameterTypes, parameters));
  }

  @Override public void delete(List<DeleteData> data, Optional<T> routeMeta, Optional<YakIntentWriteRequest> intentData,
                               Optional<V> circuitBreakerSettings,
                               BiConsumer<PipelinedResponse<List<StoreOperationResponse<Void>>>, Throwable> handler) {
    List<Class> parameterTypes = Stream
        .of(List.class, routeMeta.getClass(), intentData.getClass(), circuitBreakerSettings.getClass(), BiConsumer.class)
        .collect(Collectors.toList());
    List<Object> parameters =
        Stream.of(data, routeMeta, intentData, circuitBreakerSettings, handler).collect(Collectors.toList());

    handleIntentWrite(intentData, handler, new PipelinedRequest(DELETE_METHOD_NAME, parameterTypes, parameters));
  }

  @Override
  public void checkAndDelete(CheckAndDeleteData data, Optional<T> routeMeta, Optional<YakIntentWriteRequest> intentData,
                             Optional<V> circuitBreakerSettings,
                             BiConsumer<PipelinedResponse<StoreOperationResponse<Boolean>>, Throwable> handler) {
    List<Class> parameterTypes = Stream
        .of(data.getClass(), routeMeta.getClass(), intentData.getClass(), circuitBreakerSettings.getClass(),
            BiConsumer.class).collect(Collectors.toList());
    List<Object> parameters =
        Stream.of(data, routeMeta, intentData, circuitBreakerSettings, handler).collect(Collectors.toList());

    handleIntentWrite(intentData, handler, new PipelinedRequest(CHECK_DELETE_METHOD_NAME, parameterTypes, parameters));
  }
}
