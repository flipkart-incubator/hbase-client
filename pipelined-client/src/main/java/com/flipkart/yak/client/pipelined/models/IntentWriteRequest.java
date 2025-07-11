package com.flipkart.yak.client.pipelined.models;

import java.util.Optional;

@SuppressWarnings("java:S3740")
public abstract class IntentWriteRequest<T, U> extends IntentSettings {
  public IntentWriteRequest(Optional<T> routeMetaOptional, Optional<U> circuitBreakerOptional) {
    super(routeMetaOptional, circuitBreakerOptional);
  }
}
