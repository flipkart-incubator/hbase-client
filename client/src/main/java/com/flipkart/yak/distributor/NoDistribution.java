package com.flipkart.yak.distributor;

public enum NoDistribution implements KeyDistributor {
  INSTANCE;

  @SuppressWarnings("java:S1168")
  @Override public byte[] partitionHint(byte[] key) {
    return null;
  }

}
