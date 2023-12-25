package com.flipkart.yak.client.exceptions;

public class StoreDataNotFoundException extends StoreException {
  public StoreDataNotFoundException() {
    super("Data Not Found");
  }
}
