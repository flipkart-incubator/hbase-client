package com.flipkart.yak.client.pipelined.route;

import com.flipkart.yak.client.pipelined.models.DataCenter;
import com.flipkart.yak.client.pipelined.models.ReplicaSet;

@SuppressWarnings("java:S3740")
public abstract class Route<T extends ReplicaSet, U> {
  private final DataCenter myCurrentDataCenter;

  private final HotRouter<T, U> hotRouter;

  public Route(DataCenter myCurrentDataCenter, HotRouter hotRouter) {
    this.hotRouter = hotRouter;
    this.myCurrentDataCenter = myCurrentDataCenter;
  }

  public DataCenter getMyCurrentRegion() {
    return myCurrentDataCenter;
  }

  public HotRouter<T, U> getHotRouter() {
    return hotRouter;
  }
}
