package com.flipkart.yak.models;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

public class GetDataBuilder {

  private final String tableName;
  private byte[] rowKey;
  private byte[] partitionKey;
  private Optional<Set<ColTupple>> colTupple = Optional.empty(); // Used for getCells
  private Optional<Filter> filter = Optional.empty();

  protected int maxVersions = 1;

  public GetDataBuilder(String tableName) {
    this.tableName = tableName;
  }

  public GetDataBuilder withRowKey(byte[] key) {
    this.rowKey = key;
    return this;
  }

  public GetDataBuilder withPartitionKey(byte[] partitionKey) {
    this.partitionKey = partitionKey;
    return this;
  }

  /**
   * Will not be honored if you specify buildForColumn or addColl.
   *
   * @param filter a Filter Object
   *
   * @return GetDataBuilder
   */
  public GetDataBuilder withFilter(Filter filter) {
    this.filter = Optional.ofNullable(filter);
    return this;
  }

  public GetDataBuilder withMaxVersions(int maxVersions) {
    this.maxVersions = maxVersions;
    return this;
  }

  public GetCell buildForColumn(String cf, String column) {
    return new GetCell(tableName, rowKey, partitionKey, Bytes.toBytes(cf), Bytes.toBytes(column), maxVersions);
  }

  public GetColumnMap buildForColFamily(String cf) {
    return new GetColumnMap(tableName, rowKey, partitionKey, Bytes.toBytes(cf), filter, maxVersions);
  }

  public GetRow build() {
    if (this.colTupple.isPresent()) {
      return new GetCells(tableName, rowKey, partitionKey, colTupple.get(), maxVersions);
    } else {
      return new GetRow(tableName, rowKey, partitionKey, filter, maxVersions);
    }
  }

  public GetDataBuilder addColl(String cf, String col) {
    if (!colTupple.isPresent()) {
      this.colTupple = Optional.of(new HashSet<>());
    }
    Set<ColTupple> colTupp = this.colTupple.get();
    colTupp.add(new ColTupple(Bytes.toBytes(cf), Bytes.toBytes(col)));
    return this;
  }
}
