package com.flipkart.yak.models;

import java.util.Arrays;

/**
 * Request Model holds information to purge Invalid Indexes
 *
 * @author gokulvanan.v
 */
public class IndexDeleteData extends IdentifierData {

  private final byte[] cf;
  private final byte[] qualifier;
  private final byte[] indexKey;
  private final byte[] rowKey;

  public IndexDeleteData(String tableName, byte[] cf, byte[] qualifier, byte[] indexKey, byte[] row) {
    super(tableName);
    this.cf = cf;
    this.qualifier = qualifier;
    this.indexKey = indexKey;
    this.rowKey = row;
  }

  public byte[] getCf() {
    return cf;
  }

  public byte[] getQualifier() {
    return qualifier;
  }

  public byte[] getIndexKey() {
    return indexKey;
  }

  public byte[] getRowKey() {
    return rowKey;
  }

  public boolean isAppendableIndex() {
    return Arrays.equals(IndexConstants.INDEX_COL, qualifier);
  }
}
