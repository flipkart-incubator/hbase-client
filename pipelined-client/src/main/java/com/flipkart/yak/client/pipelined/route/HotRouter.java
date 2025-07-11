package com.flipkart.yak.client.pipelined.route;

import com.flipkart.yak.client.pipelined.exceptions.PipelinedStoreDataCorruptException;
import com.flipkart.yak.client.pipelined.models.ReplicaSet;
import java.util.Optional;

public interface HotRouter<T extends ReplicaSet, U> {

  T getReplicaSet(Optional<U> routeMeta) throws PipelinedStoreDataCorruptException;
}
