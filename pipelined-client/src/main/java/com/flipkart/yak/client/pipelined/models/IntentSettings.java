package com.flipkart.yak.client.pipelined.models;

import java.util.Optional;

public class IntentSettings<T, U extends CircuitBreakerSettings> {
  protected Optional<T> routeKeyOptional;
  protected Optional<U> circuitBreakerOptional;

  public IntentSettings(Optional<T> routeKeyOptional, Optional<U> circuitBreakerOptional) {
    this.routeKeyOptional = routeKeyOptional;
    this.circuitBreakerOptional = circuitBreakerOptional;
  }

  public Optional<U> getCircuitBreakerOptional() {
    return circuitBreakerOptional;
  }

  public Optional<T> getRouteKeyOptional() {
    return routeKeyOptional;
  }
}
