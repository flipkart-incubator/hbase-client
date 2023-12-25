package com.flipkart.yak.client.exceptions;

public class StoreException extends Exception {
  public StoreException(Throwable e) {
    super(e);
  }

  public StoreException(String msg) {
    super(msg);
  }
}
