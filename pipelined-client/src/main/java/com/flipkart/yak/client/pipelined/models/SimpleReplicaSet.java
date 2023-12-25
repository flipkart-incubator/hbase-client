package com.flipkart.yak.client.pipelined.models;

import java.util.ArrayList;

/**
 * A Specialised {@link MasterSlaveReplicaSet} which has zero secondary {@link SiteId} sites
 */
public class SimpleReplicaSet extends MasterSlaveReplicaSet {

  public SimpleReplicaSet(SiteId primarySite) {
    super(primarySite, new ArrayList<>());
  }
}
