package com.flipkart.yak.models;

import java.util.Optional;

import org.apache.hadoop.hbase.filter.Filter;

/**
 * Request model to get ColumnsMap in response. The ColumnsMap returned corresponds to the columnFamily specified in
 * this request.
 *
 * @author gokulvanan.v
 */

public class GetColumnMap extends GetRow {

  protected final byte[] cf;

  GetColumnMap(String tableName, byte[] row, byte[] cf, Optional<Filter> filter, int maxVersions) {
    super(tableName, row, filter, maxVersions);
    this.cf = cf;
  }

  GetColumnMap(String tableName, byte[] row, byte[] partitionKey, byte[] cf, Optional<Filter> filter, int maxVersions) {
    super(tableName, row, partitionKey, filter, maxVersions);
    this.cf = cf;
  }

  public byte[] getCf() {
    return cf;
  }

}
