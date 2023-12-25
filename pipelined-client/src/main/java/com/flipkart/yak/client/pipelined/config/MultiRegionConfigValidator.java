package com.flipkart.yak.client.pipelined.config;

import com.flipkart.yak.client.config.ConfigValidator;
import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.pipelined.exceptions.ConfigValidationFailedException;
import com.flipkart.yak.client.pipelined.models.Region;
import com.google.common.base.Preconditions;
import java.util.HashSet;
import java.util.Set;

public class MultiRegionConfigValidator {
  private MultiRegionConfigValidator() {
    throw new IllegalStateException();
  }

  public static void validate(MultiRegionStoreConfig multiRegionStoreConfig) throws ConfigValidationFailedException {
    try {
      Set<String> storeNames = new HashSet<>();
      Preconditions.checkNotNull(multiRegionStoreConfig, "MultiRegionStoreConfig can't be null");
      Preconditions
          .checkNotNull(multiRegionStoreConfig.getDefaultConfig(), "MultiRegionStoreConfig default config can't be null");
      Preconditions.checkNotNull(multiRegionStoreConfig.getRegions(), "MultiRegionStoreConfig regions can't be null");
      Preconditions
          .checkArgument(!multiRegionStoreConfig.getRegions().isEmpty(), "MultiRegionStoreConfig regions can't be empty");

      for (Region regionKey : multiRegionStoreConfig.getRegions().keySet()) {
        RegionConfig regionConfig = multiRegionStoreConfig.getRegions().get(regionKey);

        Preconditions.checkNotNull(regionConfig, "MultiRegionStoreConfig regionConfig can't be null");
        Preconditions.checkNotNull(regionConfig.getSites(), "MultiRegionStoreConfig regionConfig sites can't be null");
        Preconditions
            .checkArgument(!regionConfig.getSites().isEmpty(), "MultiRegionStoreConfig regionConfig sites can't be empty");

        for (String siteName : regionConfig.getSites().keySet()) {
          SiteConfig site = regionConfig.getSites().get(siteName);
          ConfigValidator.validate(site);
          Preconditions.checkArgument(!storeNames.contains(site.getStoreName()), "No 2 sites can have same store name");
          storeNames.add(site.getStoreName());
        }
      }
    } catch (Exception ex) {
      throw new ConfigValidationFailedException(ex);
    }
  }
}
