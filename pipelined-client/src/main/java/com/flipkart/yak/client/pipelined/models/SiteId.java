package com.flipkart.yak.client.pipelined.models;

import java.util.Objects;

/**
 * A SiteId is a single hbase cluster which holds a particular dataset.
 */
public class SiteId {
  public final String site;
  public final Region region;

  public SiteId(String site, Region region) {
    this.site = site;
    this.region = region;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SiteId siteId = (SiteId) o;
    return site.equals(siteId.site) && region == siteId.region;
  }

  @Override public String toString() {
    return "SiteId{" + "site='" + site + '\'' + ", region='" + region.getName() + '\'' + '}';
  }

  @Override public int hashCode() {
    return Objects.hash(site, region);
  }
}
