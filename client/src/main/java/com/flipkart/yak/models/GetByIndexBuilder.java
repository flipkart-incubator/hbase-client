package com.flipkart.yak.models;

import java.util.Optional;

import org.apache.hadoop.hbase.util.Bytes;

public class GetByIndexBuilder {

  private final String tableName;
  private final String cf;
  private String indexKey;
  private IndexAttributes indexAtt;

  public GetByIndexBuilder(String tableName, String cf) {
    this.tableName = tableName;
    this.cf = cf;
  }

  public GetByIndexBuilder withIndexKey(String indexKey, IndexAttributes att) {
    this.indexKey = indexKey;
    this.indexAtt = att;
    return this;
  }

  public GetCellByIndex buildForCloumn(String column) {
    return new GetCellByIndex(indexAtt, tableName, Bytes.toBytes(indexKey), Bytes.toBytes(cf), Bytes.toBytes(column));
  }

  public GetColumnsMapByIndex build() {
    return new GetColumnsMapByIndex(indexAtt, tableName, Bytes.toBytes(indexKey), Bytes.toBytes(cf), Optional.empty());
  }

}
