package com.flipkart.yak.client.pipelined.config;

import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.pipelined.models.Region;

import java.io.Serializable;
import java.util.Map;

public class MultiRegionStoreConfig implements Serializable {
  @SuppressWarnings("java:S1948")
  private Map<Region, RegionConfig> regions;
  private SiteConfig defaultConfig;

  public MultiRegionStoreConfig() {
    // Empty Constructor
  }

  public Map<Region, RegionConfig> getRegions() {
    return regions;
  }

  public void setRegions(Map<Region, RegionConfig> regions) {
    this.regions = regions;
  }

  public SiteConfig getDefaultConfig() {
    return defaultConfig;
  }

  public void setDefaultConfig(SiteConfig defaultConfig) {
    this.defaultConfig = defaultConfig;
  }
}
