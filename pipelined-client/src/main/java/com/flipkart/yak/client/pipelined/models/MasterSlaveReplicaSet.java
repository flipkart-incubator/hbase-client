package com.flipkart.yak.client.pipelined.models;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A ReplicaSet is a group of hbase {@link SiteId} sites that hold the same dataset. While there is always one primary
 * site which takes the writes and all secondaries replicate from primary as a standby or as a fallback
 */
public class MasterSlaveReplicaSet implements ReplicaSet {
  private SiteId primarySite;
  private List<SiteId> secondarySites = new ArrayList<>();

  public MasterSlaveReplicaSet(SiteId primarySite, List<SiteId> secondarySites) {
    this.primarySite = primarySite;
    this.secondarySites = secondarySites;
  }

  public SiteId getPrimarySite() {
    return primarySite;
  }

  public List<SiteId> getSecondarySites() {
    return secondarySites;
  }

  @SuppressWarnings("java:S1125")
  public boolean isValid() {
    return (primarySite == null || secondarySites == null) ? false : true;
  }

  @Override public String toString() {
    return "MasterSlaveReplicaSet{" + "primarySite=" + primarySite + ", secondarySites=" + secondarySites + '}';
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MasterSlaveReplicaSet that = (MasterSlaveReplicaSet) o;
    return Objects.equals(primarySite, that.primarySite) && Objects.equals(secondarySites, that.secondarySites);
  }

  @Override public int hashCode() {
    return Objects.hash(primarySite, secondarySites);
  }
}
