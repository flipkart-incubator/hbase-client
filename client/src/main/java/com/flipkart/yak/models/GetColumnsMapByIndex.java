package com.flipkart.yak.models;

import java.util.Optional;

import org.apache.hadoop.hbase.filter.Filter;

public class GetColumnsMapByIndex extends GetColumnMap implements IndexLookup {

  private final IndexAttributes indexAtt;
  private static final int MAX_VERSIONS = 1;

  GetColumnsMapByIndex(IndexAttributes indexAtt, String tableName, byte[] indexKey, byte[] dataCf,
      Optional<Filter> filter) {
    super(tableName, indexKey, dataCf, filter, MAX_VERSIONS);
    this.indexAtt = indexAtt;
  }

  @Override public IndexAttributes getIndexAtt() {
    return indexAtt;
  }

}
