package com.flipkart.yak.client.pipelined.exceptions;

public class IntentClientInitializationException extends Exception {
  public IntentClientInitializationException(Throwable e) {
    super(e);
  }

  public IntentClientInitializationException(String message) {
    super(message);
  }
}
