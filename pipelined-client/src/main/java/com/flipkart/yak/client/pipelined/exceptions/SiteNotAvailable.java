package com.flipkart.yak.client.pipelined.exceptions;

public class SiteNotAvailable extends Exception {
  public SiteNotAvailable() {
    super("Site is not available for taking any queries");
  }
}
