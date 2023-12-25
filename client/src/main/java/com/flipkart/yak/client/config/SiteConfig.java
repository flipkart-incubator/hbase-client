package com.flipkart.yak.client.config;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.hadoop.hbase.client.Durability;

public class SiteConfig implements Serializable {

  private int poolSize = 50;
  private Map<String, String> hbaseConfig = new HashMap<>();
  private int indexPurgeQueueSize = 20;
  private Durability durabilityThreshold;
  private String storeName;
  private int maxBatchGetSize = 20;
  private int maxBatchDeleteSize = 20;
  private Optional<String> hadoopUserName = Optional.empty();

  public int getPoolSize() {
    return poolSize;
  }

  public SiteConfig withPoolSize(int poolSize) {
    this.poolSize = poolSize;
    return this;
  }

  public Map<String, String> getHbaseConfig() {
    return hbaseConfig;
  }

  public SiteConfig withHbaseConfig(Map<String, String> hbaseConfig) {
    this.hbaseConfig = hbaseConfig;
    return this;
  }

  public int getIndexPurgeQueueSize() {
    return indexPurgeQueueSize;
  }

  public SiteConfig withIndexPurgeQueueSize(int indexPurgeQueueSize) {
    this.indexPurgeQueueSize = indexPurgeQueueSize;
    return this;
  }

  public Durability getDurabilityThreshold() {
    return durabilityThreshold;
  }

  public SiteConfig withDurabilityThreshold(String durabilityThreshold) {
    this.durabilityThreshold = Durability.valueOf(durabilityThreshold);
    return this;
  }

  public SiteConfig withDurabilityThreshold(Durability durabilityThreshold) {
    this.durabilityThreshold = durabilityThreshold;
    return this;
  }

  public String getStoreName() {
    return storeName;
  }

  public SiteConfig withStoreName(String storeName) {
    this.storeName = storeName;
    return this;
  }

  public int getMaxBatchGetSize() {
    return maxBatchGetSize;
  }

  public SiteConfig withMaxBatchGetSize(int maxBatchGetSize) {
    this.maxBatchGetSize = maxBatchGetSize;
    return this;
  }

  public int getMaxBatchDeleteSize() {
    return maxBatchDeleteSize;
  }

  public SiteConfig withMaxBatchDeleteSize(int maxBatchDeleteSize) {
    this.maxBatchDeleteSize = maxBatchDeleteSize;
    return this;
  }

  public Optional<String> getHadoopUserName() {
    return hadoopUserName;
  }

  public SiteConfig withHadoopUserName(Optional<String> hadoopUserName) {
    this.hadoopUserName = hadoopUserName;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SiteConfig that = (SiteConfig) o;
    return poolSize == that.poolSize && indexPurgeQueueSize == that.indexPurgeQueueSize && maxBatchGetSize == that.maxBatchGetSize && maxBatchDeleteSize == that.maxBatchDeleteSize && Objects.equals(hbaseConfig, that.hbaseConfig) && durabilityThreshold == that.durabilityThreshold && Objects.equals(storeName, that.storeName) && Objects.equals(hadoopUserName, that.hadoopUserName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(poolSize, hbaseConfig, indexPurgeQueueSize, durabilityThreshold, storeName, maxBatchGetSize, maxBatchDeleteSize, hadoopUserName);
  }
}
