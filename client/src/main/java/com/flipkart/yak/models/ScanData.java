package com.flipkart.yak.models;

/*
 * Created by Amanraj on 02/01/19 .
 */

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.filter.FilterList;

public class ScanData {

  private FilterList filter = null;
  private byte[] startRow = HConstants.EMPTY_START_ROW;
  private byte[] stopRow = HConstants.EMPTY_END_ROW;
  private int maxVersions = 1;
  private int batch = -1;
  private boolean allowPartialResults = false;
  private long maxResultSize = -1L;
  private boolean cacheBlocks = true;
  private boolean reversed = false;
  private int caching = -1;
  private int limit = -1;
  private long minStamp = 0L;
  private long maxStamp = Long.MAX_VALUE;
  private List<ColTupple> columnfamilies = new ArrayList<>();
  private final String tableName;
  private byte[] partitionKey;
  private boolean includeStopRow = false;
  private boolean includeStartRow = true;

  public boolean shouldIncludeStopRow() {
      return includeStopRow;
  }

  public void setIncludeStopRow(boolean includeStopRow) {
      this.includeStopRow = includeStopRow;
  }

  public boolean shouldIncludeStartRow() {
      return includeStartRow;
  }

  public void setIncludeStartRow(boolean includeStartRow) {
      this.includeStartRow = includeStartRow;
  }


  public ScanData(String tableName) {
    this.tableName = tableName;
  }

  public FilterList getFilter() {
    return filter;
  }

  public void setFilter(FilterList filter) {
    this.filter = filter;
  }

  public byte[] getStartRow() {
    return startRow;
  }

  public void setStartRow(byte[] startRow) {
    this.startRow = startRow;
  }

  public byte[] getStopRow() {
    return stopRow;
  }

  public void setStopRow(byte[] stopRow) {
    this.stopRow = stopRow;
  }

  public int getMaxVersions() {
    return maxVersions;
  }

  public void setMaxVersions(int maxVersions) {
    this.maxVersions = maxVersions;
  }

  public int getBatch() {
    return batch;
  }

  public void setBatch(int batch) {
    this.batch = batch;
  }

  public boolean isAllowPartialResults() {
    return allowPartialResults;
  }

  public void setAllowPartialResults(boolean allowPartialResults) {
    this.allowPartialResults = allowPartialResults;
  }

  public long getMaxResultSize() {
    return maxResultSize;
  }

  public void setMaxResultSize(long maxResultSize) {
    this.maxResultSize = maxResultSize;
  }

  public boolean isCacheBlocks() {
    return cacheBlocks;
  }

  public void setCacheBlocks(boolean cacheBlocks) {
    this.cacheBlocks = cacheBlocks;
  }

  public boolean isReversed() {
    return reversed;
  }

  public void setReversed(boolean reversed) {
    this.reversed = reversed;
  }

  public int getCaching() {
    return caching;
  }

  public void setCaching(int caching) {
    this.caching = caching;
  }

  public int getLimit() {
    return limit;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public long getMinStamp() {
    return minStamp;
  }

  public void setMinStamp(long minStamp) {
    this.minStamp = minStamp;
  }

  public long getMaxStamp() {
    return maxStamp;
  }

  public void setMaxStamp(long maxStamp) {
    this.maxStamp = maxStamp;
  }

  public List<ColTupple> getColumnfamilies() {
    return columnfamilies;
  }

  public void setColumnfamilies(List<ColTupple> columnfamilies) {
    this.columnfamilies = columnfamilies;
  }

  public String getTableName() {
    return tableName;
  }

  public byte[] getPartitionKey() {
    return partitionKey;
  }

  public void setPartitionKey(byte[] partitionKey) {
    this.partitionKey = partitionKey;
  }
}
