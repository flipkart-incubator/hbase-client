package com.flipkart.yak.models;

public interface IndexLookup {

  IndexAttributes getIndexAtt();

  byte[] getKey();

  String getIndexTableName();

}
