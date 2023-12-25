package com.flipkart.yak.client.pipelined.models;

import java.util.HashSet;
import java.util.Set;

/**
 * A ReplicaSet is a group of hbase {@link SiteId} sites that hold the same dataset. While there is always one hot site
 * which takes the writes and all cold sites replicate from primary and can be used for reads or other use cases
 */
public class HotColdReplicaSet implements ReplicaSet {
  private SiteId hotSite;
  private Set<SiteId> coldSites = new HashSet<>();

  public HotColdReplicaSet(SiteId hotSite, Set<SiteId> coldSites) {
    this.hotSite = hotSite;
    this.coldSites = coldSites;
  }

  public SiteId getHotSite() {
    return hotSite;
  }

  public Set<SiteId> getColdSites() {
    return coldSites;
  }
}
