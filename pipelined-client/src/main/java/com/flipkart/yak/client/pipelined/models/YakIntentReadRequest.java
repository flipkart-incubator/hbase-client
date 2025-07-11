package com.flipkart.yak.client.pipelined.models;

import com.flipkart.yak.models.GetRow;
import java.util.Optional;

@SuppressWarnings("java:S3740")
public class YakIntentReadRequest<T, U extends CircuitBreakerSettings> extends IntentReadRequest {
  private GetRow row;

  public YakIntentReadRequest(GetRow row, Optional<T> routeMetaOptional, Optional<U> circuitBreakerOptional) {
    super(routeMetaOptional, circuitBreakerOptional);
    this.row = row;
  }

  public GetRow getRow() {
    return row;
  }
}

