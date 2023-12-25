package com.flipkart.yak.client.pipelined.exceptions;

public class HystrixEventUnsubscribedException extends Exception {
  public HystrixEventUnsubscribedException(String message) {
    super(message);
  }
}
