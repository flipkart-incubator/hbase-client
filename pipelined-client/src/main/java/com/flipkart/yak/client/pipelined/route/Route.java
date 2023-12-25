package com.flipkart.yak.client.pipelined.route;

import com.flipkart.yak.client.pipelined.models.Region;
import com.flipkart.yak.client.pipelined.models.ReplicaSet;

@SuppressWarnings("java:S3740")
public abstract class Route<T extends ReplicaSet, U> {
  private final Region myCurrentRegion;

  private final HotRouter<T, U> hotRouter;

  public Route(Region myCurrentRegion, HotRouter hotRouter) {
    this.hotRouter = hotRouter;
    this.myCurrentRegion = myCurrentRegion;
  }

  public Region getMyCurrentRegion() {
    return myCurrentRegion;
  }

  public HotRouter<T, U> getHotRouter() {
    return hotRouter;
  }
}
