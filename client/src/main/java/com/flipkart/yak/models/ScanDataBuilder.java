package com.flipkart.yak.models;

import java.util.ArrayList;
import java.util.Optional;

import org.apache.hadoop.hbase.filter.FilterList;

/**
 * Created by kaustav.das on 03/09/18.
 */

public class ScanDataBuilder {

  private ScanData scanData;

  public ScanDataBuilder(String tableName) {
    this.scanData = new ScanData(tableName);
  }

  public ScanDataBuilder withFilterList(FilterList filter) {
    scanData.setFilter(filter);
    return this;
  }

  public ScanDataBuilder withPartitionKey(byte[] partitionKey) {
    scanData.setPartitionKey(partitionKey);
    return this;
  }

  public ScanDataBuilder withStartRow(byte[] startRow) {
    scanData.setStartRow(startRow);
    return this;
  }

  public ScanDataBuilder withStopRow(byte[] stopRow) {
    scanData.setStopRow(stopRow);
    return this;
  }

  public ScanDataBuilder witMaxVersions(int maxVersions) {
    scanData.setMaxVersions(maxVersions);
    return this;
  }

  public ScanDataBuilder withBatch(int batch) {
    scanData.setBatch(batch);
    return this;
  }

  public ScanDataBuilder withAllowPartialResults(boolean allowPartialResults) {
    scanData.setAllowPartialResults(allowPartialResults);
    return this;
  }

  public ScanDataBuilder withMaxResultSize(long maxResultSize) {
    scanData.setMaxResultSize(maxResultSize);
    return this;
  }

  public ScanDataBuilder withCacheBlocks(boolean cacheBlocks) {
    scanData.setCacheBlocks(cacheBlocks);
    return this;
  }

  public ScanDataBuilder withReversed(boolean reversed) {
    scanData.setReversed(reversed);
    return this;
  }

  public ScanDataBuilder withCacheSize(int caching) {
    scanData.setCaching(caching);
    return this;
  }

  public ScanDataBuilder withMinStamp(long minStamp) {
    scanData.setMinStamp(minStamp);
    return this;
  }

  public ScanDataBuilder withMaxStamp(long maxStamp) {
    scanData.setMaxStamp(maxStamp);
    return this;
  }

  public ScanDataBuilder withLimit(int limit) {
    scanData.setLimit(limit);
    return this;
  }

  public ScanDataBuilder withColumnQualifier(ColTupple colTupple) {
    if (scanData.getColumnfamilies() == null) {
      scanData.setColumnfamilies(new ArrayList<>());
    }
    if (!scanData.getColumnfamilies().contains(colTupple)) {
      scanData.getColumnfamilies().add(colTupple);
    }

    return this;
  }

  public ScanData build() {
    return scanData;
  }
}
