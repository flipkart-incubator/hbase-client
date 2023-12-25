package com.flipkart.yak.models;

public abstract class IndexAttributes {

  private final IndexType indexType;

  public IndexAttributes(IndexType type) {
    this.indexType = type;
  }

  public IndexType getIndexType() {
    return indexType;
  }

}
