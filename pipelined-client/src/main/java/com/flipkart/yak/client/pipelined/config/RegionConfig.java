package com.flipkart.yak.client.pipelined.config;

import com.flipkart.yak.client.config.SiteConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class RegionConfig {
  private Map<String, SiteConfig> sites = new HashMap<>();

  public RegionConfig() {
  }

  public RegionConfig(Map<String, SiteConfig> sites) {
    this.sites = sites;
  }

  public Map<String, SiteConfig> getSites() {
    return sites;
  }

  public Optional<SiteConfig> getSite(String siteName) {
    return Optional.of(sites.get(siteName));
  }

  public boolean addSite(String siteName, SiteConfig siteConfig) {
    if (sites.containsKey(siteName)) {
      return false;
    }
    sites.put(siteName, siteConfig);
    return true;
  }
}
