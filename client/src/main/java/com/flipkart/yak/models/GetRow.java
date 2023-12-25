package com.flipkart.yak.models;

import java.util.Optional;

import org.apache.hadoop.hbase.filter.Filter;

/**
 * Request model used to Get ResultMap Response for a rowKey. check com.flipkart.transact.keyval.models.ResultMap for
 * responseMode.
 *
 * @author gokulvanan.v
 */
public class GetRow extends IdentifierData {

  protected final byte[] key;

  protected final byte[] partitionKey;

  protected final Optional<Filter> filter;
  protected final int maxVersions;

  GetRow(String tableName, byte[] row, Optional<Filter> filter, int maxVersions) {
    super(tableName);
    this.key = row;
    this.filter = filter;
    this.maxVersions = maxVersions;
    this.partitionKey = null;
  }

  GetRow(String tableName, byte[] row, byte[] partitionKey, Optional<Filter> filter, int maxVersions) {
    super(tableName);
    this.key = row;
    this.filter = filter;
    this.partitionKey = partitionKey;
    this.maxVersions = maxVersions;
  }

  public byte[] getKey() {
    return key;
  }

  public Optional<Filter> getFilter() {
    return this.filter;
  }

  public byte[] getPartitionKey() {
    return partitionKey;
  }

  public int getMaxVersions() {
    return maxVersions;
  }
}
