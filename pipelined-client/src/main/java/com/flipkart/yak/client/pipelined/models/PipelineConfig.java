package com.flipkart.yak.client.pipelined.models;

import com.flipkart.yak.client.pipelined.YakPipelinedStore;
import com.flipkart.yak.client.pipelined.config.MultiRegionStoreConfig;
import com.flipkart.yak.distributor.KeyDistributor;

import java.util.Map;
import java.util.Optional;

/**
 * Pipeline Config for initializing {@link YakPipelinedStore}
 */
public class PipelineConfig {
  private final MultiRegionStoreConfig multiRegionStoreConfig;
  private final Optional<Map<String, KeyDistributor>> keyDistributorMap;
  private final int siteBootstrapTimeoutInSeconds;
  private final int poolSize;
  private final String name;
  private int siteBootstrapRetryCount = 3;
  private long siteBootstrapRetryDelayInMillis = 3000;
  /** Optional bound for the thread-pool work queue; empty means unbounded. */
  private Optional<Integer> executorQueueCapacity = Optional.empty();

  public PipelineConfig(MultiRegionStoreConfig multiRegionStoreConfig, int poolSize, int siteBootstrapTimeoutInSeconds,
                        String name) {
    this(multiRegionStoreConfig, siteBootstrapTimeoutInSeconds, poolSize, name, Optional.empty());
  }

  public PipelineConfig(MultiRegionStoreConfig multiRegionStoreConfig, int siteBootstrapTimeoutInSeconds, int poolSize,
                        String name, Optional<Map<String, KeyDistributor>> keyDistributorMap) {
    this.multiRegionStoreConfig = multiRegionStoreConfig;
    this.siteBootstrapTimeoutInSeconds = siteBootstrapTimeoutInSeconds;
    this.poolSize = poolSize;
    this.name = name;
    this.keyDistributorMap = keyDistributorMap;
  }

  public PipelineConfig(MultiRegionStoreConfig multiRegionStoreConfig, int siteBootstrapTimeoutInSeconds, int poolSize,
                        String name, Optional<Map<String, KeyDistributor>> keyDistributorMap, int siteBootstrapRetryCount,
                        long siteBootstrapRetryDelayInMillis) {
    this.multiRegionStoreConfig = multiRegionStoreConfig;
    this.siteBootstrapTimeoutInSeconds = siteBootstrapTimeoutInSeconds;
    this.poolSize = poolSize;
    this.name = name;
    this.keyDistributorMap = keyDistributorMap;
    this.siteBootstrapRetryCount = siteBootstrapRetryCount;
    this.siteBootstrapRetryDelayInMillis = siteBootstrapRetryDelayInMillis;
  }

  public PipelineConfig(MultiRegionStoreConfig multiRegionStoreConfig, Optional<Map<String, KeyDistributor>> keyDistributorMap,
                        int siteBootstrapTimeoutInSeconds, int poolSize, String name,
                        int siteBootstrapRetryCount, long siteBootstrapRetryDelayInMillis, Integer executorQueueCapacity) {
    this.multiRegionStoreConfig = multiRegionStoreConfig;
    this.keyDistributorMap = keyDistributorMap;
    this.siteBootstrapTimeoutInSeconds = siteBootstrapTimeoutInSeconds;
    this.poolSize = poolSize;
    this.name = name;
    this.siteBootstrapRetryCount = siteBootstrapRetryCount;
    this.siteBootstrapRetryDelayInMillis = siteBootstrapRetryDelayInMillis;
    this.executorQueueCapacity = Optional.ofNullable(executorQueueCapacity);
  }


  public MultiRegionStoreConfig getMultiRegionStoreConfig() {
    return multiRegionStoreConfig;
  }

  public int getSiteBootstrapTimeoutInSeconds() {
    return siteBootstrapTimeoutInSeconds;
  }

  public int getPoolSize() {
    return poolSize;
  }

  public String getName() {
    return name;
  }

  public Optional<Map<String, KeyDistributor>> getKeyDistributorMap() {
    return keyDistributorMap;
  }

  public int getSiteBootstrapRetryCount() {
    return siteBootstrapRetryCount;
  }

  public long getSiteBootstrapRetryDelayInMillis() {
    return siteBootstrapRetryDelayInMillis;
  }

  public Optional<Integer> getExecutorQueueCapacity() {
    return executorQueueCapacity;
  }
}
