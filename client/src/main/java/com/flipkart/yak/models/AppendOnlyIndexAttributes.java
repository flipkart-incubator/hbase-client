package com.flipkart.yak.models;

public class AppendOnlyIndexAttributes extends IndexAttributes {

  private final int pageSize;
  private final int offset;

  public AppendOnlyIndexAttributes(int offset, int pageSize) {
    super(IndexType.APPEND_ONLY);
    this.offset = offset;
    this.pageSize = pageSize;
  }

  public int getPageSize() {
    return pageSize;
  }

  public int getOffset() {
    return offset;
  }

}
