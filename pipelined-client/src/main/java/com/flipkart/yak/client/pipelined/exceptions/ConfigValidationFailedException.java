package com.flipkart.yak.client.pipelined.exceptions;

public class ConfigValidationFailedException extends Exception {
  public ConfigValidationFailedException(Throwable e) {
    super(e);
  }
}
