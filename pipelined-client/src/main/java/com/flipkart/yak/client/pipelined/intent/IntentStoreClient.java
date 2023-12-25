package com.flipkart.yak.client.pipelined.intent;

import com.flipkart.yak.client.pipelined.models.IntentReadRequest;
import com.flipkart.yak.client.pipelined.models.IntentWriteRequest;
import java.util.function.BiConsumer;

/**
 * This interface is to be implemented for using a particular store as intent store
 *
 * @param <T> Intent data which needs to be extended to implement this interface
 */
public interface IntentStoreClient<T extends IntentWriteRequest, U extends IntentReadRequest, V, W> {
  public void write(T request, BiConsumer<V, Throwable> handler);

  public void read(U request, BiConsumer<W, Throwable> handler);
}
