package com.flipkart.yak.client.pipelined.exceptions;

public class NoSiteAvailableToHandleException extends RuntimeException {
  public NoSiteAvailableToHandleException(Throwable e) {
    super(e);
  }

  public NoSiteAvailableToHandleException(String message) {
    super(message);
  }

  public NoSiteAvailableToHandleException() {
    this("No site is available to route the traffic");
  }
}
