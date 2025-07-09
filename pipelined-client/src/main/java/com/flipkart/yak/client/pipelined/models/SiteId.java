package com.flipkart.yak.client.pipelined.models;

import java.util.Objects;

/**
 * A SiteId is a single hbase cluster which holds a particular dataset.
 */
public class SiteId {
  public final String site;
  public final DataCenter dataCenter;

  public SiteId(String site, DataCenter dataCenter) {
    this.site = site;
    this.dataCenter = dataCenter;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SiteId siteId = (SiteId) o;
    return site.equals(siteId.site) && dataCenter == siteId.dataCenter;
  }

  @Override public String toString() {
    return "SiteId{" + "site='" + site + '\'' + ", region='" + dataCenter.getName() + '\'' + '}';
  }

  @Override public int hashCode() {
    return Objects.hash(site, dataCenter);
  }
}
