package com.flipkart.yak.client.pipelined.circuitbreakers;

import com.flipkart.yak.client.pipelined.exceptions.HystrixEventUnsubscribedException;
import com.flipkart.yak.client.pipelined.models.PipelinedRequest;
import com.flipkart.yak.client.pipelined.models.PipelinedResponse;
import com.netflix.hystrix.HystrixObservableCommand;
import com.flipkart.yak.client.pipelined.YakPipelinedStore;
import java.util.function.BiConsumer;
import rx.Observable;
import rx.Subscriber;

/**
 * A {@link HystrixObservableCommand} wrapper over {@link YakPipelinedStore}
 */
@SuppressWarnings("java:S3740")
public class StoreClientHystrixObservableCommand extends HystrixObservableCommand<PipelinedResponse> {
  private final PipelinedRequest request;
  private YakPipelinedStore store;
  private Class<?> storeClazz;

  public StoreClientHystrixObservableCommand(YakPipelinedStore store, PipelinedRequest request, Setter setter) {
    super(setter);
    this.request = request;
    this.store = store;
    storeClazz = store.getClass();
  }

  private void addHandlerToSubscriber(Subscriber subscriber) {
    BiConsumer<PipelinedResponse, Throwable> handler = (response, error) -> {
      if (error != null) {
        subscriber.onError(error);
      } else {
        subscriber.onNext(response);
        subscriber.onCompleted();
      }
    };

    request.getParameterTypes().add(BiConsumer.class);
    request.getParameters().add(handler);
  }

  /**
   * Invokes methods from underlying {@link YakPipelinedStore}
   *
   * @return Observable on {@link PipelinedResponse}
   */
  @Override protected Observable<PipelinedResponse> construct() {
    return Observable.create(subscriber -> {
      try {
        if (!subscriber.isUnsubscribed()) {
          addHandlerToSubscriber(subscriber);

          Class<?>[] parameterTypes = new Class[request.getParameters().size()];
          request.getParameterTypes().toArray(parameterTypes);
          storeClazz.getMethod(request.getMethodName(), parameterTypes)
              .invoke(store, request.getParameters().toArray());
        } else {
          subscriber.onError(new HystrixEventUnsubscribedException("Unsubscribed from callback"));
        }
      } catch (Exception ex) {
        subscriber.onError(ex);
      }
    });
  }
}
