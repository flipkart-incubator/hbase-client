package com.flipkart.yak.client.pipelined.models;

/**
 * Pipelined response
 *
 * @param <T> Generic Response type
 */
public class PipelinedResponse<T> {
  private T operationResponse;
  private boolean isStale = false;

  public PipelinedResponse(T operationResponse, boolean isStale) {
    this.operationResponse = operationResponse;
    this.isStale = isStale;
  }

  public T getOperationResponse() {
    return operationResponse;
  }

  public boolean isStale() {
    return isStale;
  }
}
