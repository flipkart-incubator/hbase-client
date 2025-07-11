package com.flipkart.yak.client.pipelined.models;

import java.util.Optional;

public class IntentSettings<T, U extends CircuitBreakerSettings> {
  protected Optional<T> routeMetaOptional;
  protected Optional<U> circuitBreakerOptional;

  public IntentSettings(Optional<T> routeMetaOptional, Optional<U> circuitBreakerOptional) {
    this.routeMetaOptional = routeMetaOptional;
    this.circuitBreakerOptional = circuitBreakerOptional;
  }

  public Optional<U> getCircuitBreakerOptional() {
    return circuitBreakerOptional;
  }

  public Optional<T> getRouteMetaOptional() {
    return routeMetaOptional;
  }
}
