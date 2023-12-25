package com.flipkart.yak.models;

public class GetCellByIndex extends GetCell implements IndexLookup {

  private final IndexAttributes indexAtt;
  private static final int MAX_VERSIONS = 1;

  GetCellByIndex(IndexAttributes indexAtt, String tableName, byte[] indexKey, byte[] dataCf, byte[] dataQualifier) {
    super(tableName, indexKey, dataCf, dataQualifier, MAX_VERSIONS);
    this.indexAtt = indexAtt;
  }

  @Override public IndexAttributes getIndexAtt() {
    return indexAtt;
  }

}
