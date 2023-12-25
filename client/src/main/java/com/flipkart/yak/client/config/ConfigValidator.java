package com.flipkart.yak.client.config;

import com.flipkart.yak.client.exceptions.StoreException;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.client.Durability;

public class ConfigValidator {

  private ConfigValidator() {
    throw new IllegalStateException();
  }

  public static void validate(SiteConfig config) throws StoreException {
    try {
      Preconditions.checkNotNull(config, "SiteConfig site can't be null");
      Preconditions.checkNotNull(config.getStoreName(), "site store name can't be null");
      Preconditions.checkArgument(config.getPoolSize() > 10, "site poolsize has to be greater than 10");
      Preconditions.checkArgument((
              config.getDurabilityThreshold() == null || !(
                  config.getDurabilityThreshold().equals(Durability.SKIP_WAL))),
          "Durability SKIP_WAL is not an acceptable option");
    } catch (Exception ex) {
      throw new StoreException(ex);
    }
  }
}