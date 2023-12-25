package com.flipkart.yak.client.pipelined.models;

import com.flipkart.yak.models.StoreData;
import java.util.Optional;

@SuppressWarnings("java:S3740")
public class YakIntentWriteRequest<T, U extends CircuitBreakerSettings> extends IntentWriteRequest {
  private StoreData storeData;

  public YakIntentWriteRequest(StoreData storeData, Optional<T> routeKeyOptional, Optional<U> circuitBreakerOptional) {
    super(routeKeyOptional, circuitBreakerOptional);
    this.storeData = storeData;
  }

  public StoreData getStoreData() {
    return storeData;
  }
}
