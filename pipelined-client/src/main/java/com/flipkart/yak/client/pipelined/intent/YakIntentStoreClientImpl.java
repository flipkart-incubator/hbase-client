package com.flipkart.yak.client.pipelined.intent;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.flipkart.yak.client.AsyncStoreClient;
import com.flipkart.yak.client.pipelined.MasterSlaveYakPipelinedStoreImpl;
import com.flipkart.yak.client.pipelined.YakPipelinedStore;
import com.flipkart.yak.client.pipelined.exceptions.ConfigValidationFailedException;
import com.flipkart.yak.client.pipelined.exceptions.PipelinedClientInitializationException;
import com.flipkart.yak.client.pipelined.metrics.PipelinedClientMetricsPublisher;
import com.flipkart.yak.client.pipelined.models.IntentConsistency;
import com.flipkart.yak.client.pipelined.models.MasterSlaveReplicaSet;
import com.flipkart.yak.client.pipelined.models.PipelineConfig;
import com.flipkart.yak.client.pipelined.models.PipelinedResponse;
import com.flipkart.yak.client.pipelined.models.SiteId;
import com.flipkart.yak.client.pipelined.models.StoreOperationResponse;
import com.flipkart.yak.client.pipelined.models.YakIntentReadRequest;
import com.flipkart.yak.client.pipelined.models.YakIntentWriteRequest;
import com.flipkart.yak.client.pipelined.route.IntentRoute;
import com.flipkart.yak.models.ResultMap;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hbase as intent store implementation of {@link IntentStoreClient} with parameter {@link YakIntentWriteRequest} provided
 * as data to store in a intent write call.
 * <br>
 * Expects a {@link MasterSlaveReplicaSet} and {@link IntentConsistency} to control which site all writes should land
 * on
 */
@SuppressWarnings("java:S3740")
public class YakIntentStoreClientImpl implements
    IntentStoreClient<YakIntentWriteRequest, YakIntentReadRequest, PipelinedResponse<StoreOperationResponse<Void>>, PipelinedResponse<StoreOperationResponse<ResultMap>>> {

  private final Logger logger = LoggerFactory.getLogger(YakIntentStoreClientImpl.class);

  private static final String METRIC_PREFIX_KEY = "com.flipkart.yak.client.async.pipelinedclient.intent.";

  private final YakPipelinedStore pipelinedStore;
  private final PipelinedClientMetricsPublisher publisher;

  public YakIntentStoreClientImpl(PipelineConfig pipelineConfig, IntentRoute route, MetricRegistry registry)
      throws ConfigValidationFailedException, PipelinedClientInitializationException {
    this.publisher = new PipelinedClientMetricsPublisher(registry, METRIC_PREFIX_KEY);
    this.pipelinedStore = new MasterSlaveYakPipelinedStoreImpl(pipelineConfig, route, registry);

    // Init Metrics
    publisher.getTimer(PipelinedClientMetricsPublisher.PUT_TIMER).close();
    publisher.incrementMetric(PipelinedClientMetricsPublisher.PUT_INIT, 0L);
    publisher.incrementMetric(PipelinedClientMetricsPublisher.PUT_COMPLETE, 0L);
  }

  @Override public void write(YakIntentWriteRequest request,
      BiConsumer<PipelinedResponse<StoreOperationResponse<Void>>, Throwable> handler) {
    Timer.Context timer = publisher.getTimer(PipelinedClientMetricsPublisher.PUT_TIMER);
    publisher.incrementMetric(PipelinedClientMetricsPublisher.PUT_INIT);

    pipelinedStore.put(request.getStoreData(), request.getRouteKeyOptional(), Optional.empty(), Optional.empty(),
            (BiConsumer<PipelinedResponse<StoreOperationResponse<Void>>, Throwable>) (response, error) -> {
          try {
            handler.accept(response, error);
            publisher.incrementMetric(PipelinedClientMetricsPublisher.PUT_COMPLETE);
            timer.close();
          } catch (Exception e) {
            logger.error("Failed to respond with intent write. Error " + e.getMessage(), e);
            handler.accept(null, error);
          }
        });
  }

  @Override public void read(YakIntentReadRequest request,
      BiConsumer<PipelinedResponse<StoreOperationResponse<ResultMap>>, Throwable> handler) {
    Timer.Context timer = publisher.getTimer(PipelinedClientMetricsPublisher.GET_TIMER);
    publisher.incrementMetric(PipelinedClientMetricsPublisher.GET_INIT);

    pipelinedStore.get(request.getRow(), request.getRouteKeyOptional(), Optional.empty(), Optional.empty(),
            (BiConsumer<PipelinedResponse<StoreOperationResponse<ResultMap>>, Throwable>) (response, error) -> {
          try {
            handler.accept(response, error);
            publisher.incrementMetric(PipelinedClientMetricsPublisher.GET_COMPLETE);
            timer.close();
          } catch (Exception e) {
            logger.error("Failed to respond with intent read. Error " + e.getMessage(), e);
            handler.accept(null, error);
          }
        });
  }

  public AsyncStoreClient getAsyncStoreClient(SiteId site) {
    return pipelinedStore.getAsyncStoreClient(site);
  }
}
