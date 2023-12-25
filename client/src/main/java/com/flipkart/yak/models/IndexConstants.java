package com.flipkart.yak.models;

public class IndexConstants {

  private IndexConstants() {
    throw new IllegalStateException();
  }

  @SuppressWarnings("java:S2386")
  public static final byte[] INDEX_CF = "i".getBytes();
  @SuppressWarnings("java:S2386")
  public static final byte[] INDEX_COL = "index".getBytes();
}
