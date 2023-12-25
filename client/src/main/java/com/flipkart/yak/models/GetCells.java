package com.flipkart.yak.models;

import java.util.Optional;
import java.util.Set;

/**
 * Useful abstraction to fetch different cells in a single column
 *
 * @author gokulvanan.v
 */
public class GetCells extends GetRow {

  private final Set<ColTupple> fetchSet;

  GetCells(String tableName, byte[] row, Set<ColTupple> fetchSet, int maxVersions) {
    super(tableName, row, Optional.empty(), maxVersions);
    this.fetchSet = fetchSet;
  }

  GetCells(String tableName, byte[] row, byte[] partitionKey, Set<ColTupple> fetchSet, int maxVersions) {
    super(tableName, row, partitionKey, Optional.empty(), maxVersions);
    this.fetchSet = fetchSet;
  }

  public Set<ColTupple> getFetchSet() {
    return this.fetchSet;
  }

}
