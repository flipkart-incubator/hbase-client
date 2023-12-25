package com.flipkart.yak.client.pipelined.models;

public class StoreOperationResponse<T> {
  private final T value;
  private final Throwable error;
  private final SiteId site;

  public StoreOperationResponse(T value, Throwable error, SiteId site) {
    this.value = value;
    this.error = error;
    this.site = site;
  }

  public T getValue() {
    return value;
  }

  public Throwable getError() {
    return error;
  }

  public SiteId getSite() {
    return site;
  }

  @Override public String toString() {
    return "SiteOperationResponse{" + "value=" + value + ", error=" + error + ", site=" + site + '}';
  }
}
