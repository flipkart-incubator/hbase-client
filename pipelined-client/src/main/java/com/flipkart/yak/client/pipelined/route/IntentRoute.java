package com.flipkart.yak.client.pipelined.route;

import com.flipkart.yak.client.pipelined.models.DataCenter;
import com.flipkart.yak.client.pipelined.models.IntentConsistency;

@SuppressWarnings("java:S3740")
public class IntentRoute extends Route {
  private final IntentConsistency writeConsistency;

  public IntentRoute(DataCenter dataCenter, IntentConsistency writeConsistency, HotRouter hotRouter) {
    super(dataCenter, hotRouter);
    this.writeConsistency = writeConsistency;
  }

  public IntentConsistency getWriteConsistency() {
    return writeConsistency;
  }
}
