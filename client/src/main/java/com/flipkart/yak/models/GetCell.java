package com.flipkart.yak.models;

import java.util.Optional;

public class GetCell extends GetColumnMap {
  protected final byte[] qualifier;

  GetCell(String tableName, byte[] row, byte[] cf, byte[] qualifer, int maxVersions) {
    super(tableName, row, cf, Optional.empty(), maxVersions);
    this.qualifier = qualifer;
  }

  GetCell(String tableName, byte[] row, byte[] partitionKey, byte[] cf, byte[] qualifer, int maxVersions) {
    super(tableName, row, partitionKey, cf, Optional.empty(), maxVersions);
    this.qualifier = qualifer;
  }

  public byte[] getQualifier() {
    return qualifier;
  }

}
