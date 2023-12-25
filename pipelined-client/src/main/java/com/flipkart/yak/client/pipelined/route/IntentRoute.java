package com.flipkart.yak.client.pipelined.route;

import com.flipkart.yak.client.pipelined.models.IntentConsistency;
import com.flipkart.yak.client.pipelined.models.Region;

@SuppressWarnings("java:S3740")
public class IntentRoute extends Route {
  private final IntentConsistency writeConsistency;

  public IntentRoute(Region region, IntentConsistency writeConsistency, HotRouter hotRouter) {
    super(region, hotRouter);
    this.writeConsistency = writeConsistency;
  }

  public IntentConsistency getWriteConsistency() {
    return writeConsistency;
  }
}
