package com.flipkart.yak.client.pipelined;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.flipkart.yak.client.AsyncStoreClient;
import com.flipkart.yak.client.AsyncStoreClientImpl;
import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.exceptions.StoreDataNotFoundException;
import com.flipkart.yak.client.pipelined.config.MultiRegionConfigValidator;
import com.flipkart.yak.client.pipelined.config.MultiRegionStoreConfig;
import com.flipkart.yak.client.pipelined.config.RegionConfig;
import com.flipkart.yak.client.pipelined.exceptions.ConfigValidationFailedException;
import com.flipkart.yak.client.pipelined.exceptions.NoSiteAvailableToHandleException;
import com.flipkart.yak.client.pipelined.exceptions.PipelinedClientInitializationException;
import com.flipkart.yak.client.pipelined.exceptions.PipelinedStoreDataCorruptException;
import com.flipkart.yak.client.pipelined.exceptions.SiteNotAvailable;
import com.flipkart.yak.client.pipelined.metrics.PipelinedClientMetricsPublisher;
import com.flipkart.yak.client.pipelined.models.CircuitBreakerSettings;
import com.flipkart.yak.client.pipelined.models.IntentWriteRequest;
import com.flipkart.yak.client.pipelined.models.MasterSlaveReplicaSet;
import com.flipkart.yak.client.pipelined.models.PipelineConfig;
import com.flipkart.yak.client.pipelined.models.PipelinedResponse;
import com.flipkart.yak.client.pipelined.models.SiteId;
import com.flipkart.yak.client.pipelined.models.StoreOperationResponse;
import com.flipkart.yak.client.pipelined.models.DataCenter;
import com.flipkart.yak.client.pipelined.route.IntentRoute;
import com.flipkart.yak.client.pipelined.route.Route;
import com.flipkart.yak.distributor.KeyDistributor;
import com.flipkart.yak.models.*;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@inheritDoc}
 */
@SuppressWarnings({"java:S1168", "common-java:DuplicatedBlocks"})
public class MasterSlaveYakPipelinedStoreImpl<T, U extends IntentWriteRequest, V extends CircuitBreakerSettings>
    implements YakPipelinedStore<T, U, V> {

  @SuppressWarnings("java:S116")
  private Logger LOG = LoggerFactory.getLogger(MasterSlaveYakPipelinedStoreImpl.class);

  private static final String METRIC_PREFIX_KEY = "com.flipkart.yak.client.async.pipelinedclient.";

  private final MetricRegistry registry;
  private final ThreadPoolExecutor executor;
  private final Route<MasterSlaveReplicaSet, T> route;
  private final PipelineConfig pipelineConfig;
  private final PipelinedClientMetricsPublisher publisher;

  private Map<SiteId, AsyncStoreClient> clients = new HashMap<>();
  private HashMap<MasterSlaveReplicaSet, List<SiteId>> readReplicaSetCache = new HashMap<>();
  private HashMap<MasterSlaveReplicaSet, List<SiteId>> writeReplicaSetCache = new HashMap<>();

  public MasterSlaveYakPipelinedStoreImpl(PipelineConfig pipelineConfig, Route<MasterSlaveReplicaSet, T> route,
      MetricRegistry registry) throws ConfigValidationFailedException, PipelinedClientInitializationException {
    this.route = route;
    this.pipelineConfig = pipelineConfig;
    this.registry = registry;
    this.publisher = new PipelinedClientMetricsPublisher(registry, METRIC_PREFIX_KEY);
    this.publisher.init();
    this.executor =
        new ThreadPoolExecutor(pipelineConfig.getPoolSize(), pipelineConfig.getPoolSize(), 0L, TimeUnit.MILLISECONDS,
            getWorkQueue(pipelineConfig.getExecutorQueueCapacity()), new ThreadFactory() {
          private AtomicInteger counter = new AtomicInteger(0);

          @Override public Thread newThread(Runnable r) {
            return new Thread(r, pipelineConfig.getName() + "-threadpool-" + counter.getAndIncrement());
          }
        });

    MultiRegionConfigValidator.validate(pipelineConfig.getMultiRegionStoreConfig());
    _createClients(this.pipelineConfig);
  }

  /**
   * Returns a work queue for the thread pool: bounded to the given capacity when present,
   * otherwise unbounded.
   *
   * @param executorQueueCapacity optional queue capacity; empty for unbounded
   * @return a blocking queue for executor tasks
   */
  private static LinkedBlockingQueue<Runnable> getWorkQueue(Optional<Integer> executorQueueCapacity) {
    return executorQueueCapacity
            .map(capacity -> new LinkedBlockingQueue<Runnable>(capacity))
            .orElseGet(LinkedBlockingQueue::new);
  }

  private SiteConfig mergeDefaultConfigWithSiteConfig(SiteConfig siteConfig, SiteConfig defaultConfig) {
    if (defaultConfig == null) {
      return siteConfig;
    }
    int poolSize = (siteConfig.getPoolSize() > 0) ? siteConfig.getPoolSize() : defaultConfig.getPoolSize();
    int indexPurgeQueueSize = (siteConfig.getIndexPurgeQueueSize() > 0) ?
        siteConfig.getIndexPurgeQueueSize() :
        defaultConfig.getIndexPurgeQueueSize();
    int maxBatchGetSize =
        (siteConfig.getMaxBatchGetSize() > 0) ? siteConfig.getMaxBatchGetSize() : defaultConfig.getMaxBatchGetSize();
    int maxBatchDeleteSize = (siteConfig.getMaxBatchDeleteSize() > 0) ?
        siteConfig.getMaxBatchDeleteSize() :
        defaultConfig.getMaxBatchDeleteSize();
    String storeName = (siteConfig.getStoreName() != null) ? siteConfig.getStoreName() : defaultConfig.getStoreName();
    Map<String, String> hbaseConfig = new HashMap<>();
    if (siteConfig.getHbaseConfig() != null && !siteConfig.getHbaseConfig().isEmpty()) {
      hbaseConfig = siteConfig.getHbaseConfig();
    }
    if (defaultConfig.getHbaseConfig() != null && !defaultConfig.getHbaseConfig().isEmpty()) {
      for (String confName : defaultConfig.getHbaseConfig().keySet()) {
        hbaseConfig.putIfAbsent(confName, defaultConfig.getHbaseConfig().get(confName));
      }
    }

    SiteConfig config = new SiteConfig().withHbaseConfig(hbaseConfig).withPoolSize(poolSize)
        .withIndexPurgeQueueSize(indexPurgeQueueSize).withMaxBatchDeleteSize(maxBatchDeleteSize)
        .withMaxBatchGetSize(maxBatchGetSize).withDurabilityThreshold((siteConfig.getDurabilityThreshold() != null) ?
            siteConfig.getDurabilityThreshold() :
            defaultConfig.getDurabilityThreshold()).withStoreName(storeName).withHbaseConfig(hbaseConfig);

    if (siteConfig.getHadoopUserName().isPresent()) {
      config.withHadoopUserName(siteConfig.getHadoopUserName());
    }
    return config;
  }

  @SuppressWarnings({"java:S3776", "java:S100"})
  private void _createClients(PipelineConfig config) throws PipelinedClientInitializationException {
    MultiRegionStoreConfig multiRegionStoreConfig = config.getMultiRegionStoreConfig();
    Optional<Map<String, KeyDistributor>> keyDistributorMapOptional = config.getKeyDistributorMap();
    SiteConfig defaultConfig = multiRegionStoreConfig.getDefaultConfig();
    for (DataCenter dataCenterKey : multiRegionStoreConfig.getRegions().keySet()) {
      RegionConfig regionConfig = multiRegionStoreConfig.getRegions().get(dataCenterKey);

      for (Map.Entry<String, SiteConfig> entry : regionConfig.getSites().entrySet()) {
        String siteName = entry.getKey();
        SiteConfig siteConfig = mergeDefaultConfigWithSiteConfig(entry.getValue(), defaultConfig);
        SiteId siteId = new SiteId(siteName, dataCenterKey);
        if (!clients.containsKey(siteId)) {
          LOG.info("Setting up async store client with site: {}", siteId);
          int retriesLeft = config.getSiteBootstrapRetryCount();
          while (retriesLeft-- > 0) {
            try {
              if (retriesLeft < (config.getSiteBootstrapRetryCount() - 1)) {
                Thread.sleep(config.getSiteBootstrapRetryDelayInMillis());
              }
              AsyncStoreClient storeClient = new AsyncStoreClientImpl(siteConfig, keyDistributorMapOptional,
                      config.getSiteBootstrapTimeoutInSeconds(), this.registry);
              clients.put(siteId, storeClient);
              retriesLeft = 0;
              LOG.info("Successfully setup async store client with site: {}", siteId);
            } catch (Exception ex) {
              LOG.error("Failed to setup async client for SiteId: {}, Attempt: {}, Sleep Duration: {}, Error Msg: {}",
                      siteId, (config.getSiteBootstrapRetryCount() - retriesLeft),
                      config.getSiteBootstrapRetryDelayInMillis(), ex.getMessage(), ex);
            }
          }
        } else {
          LOG.error("Failed to setup async store client with site: {}, Error: Duplicate site configuration", siteId);
          throw new PipelinedClientInitializationException("Duplicate site configuration found");
        }
      }
    }

    if (clients.isEmpty()) {
      LOG.error("Failed to setup async client with any of the sites provided. Aborting Pipelined client..");
      throw new PipelinedClientInitializationException("No sites are accepting any connections");
    }
  }

  @SuppressWarnings("java:S1193")
  private List<SiteId> getSites(Optional<T> routeMeta, boolean isRead)
      throws PipelinedStoreDataCorruptException {
    try {
      List<SiteId> sites = new ArrayList<>();
      MasterSlaveReplicaSet replicaSet = route.getHotRouter().getReplicaSet(routeMeta);

      if (replicaSet != null && replicaSet.isValid()) {
        if (route instanceof IntentRoute) {
          readReplicaSetCache.putIfAbsent(replicaSet, PipelinedStoreUtils.getIntentSites(route, replicaSet));
          sites = readReplicaSetCache.get(replicaSet);
        } else if (isRead) {
          readReplicaSetCache.putIfAbsent(replicaSet, PipelinedStoreUtils.getSitesToRead(route, replicaSet));
          sites = readReplicaSetCache.get(replicaSet);
        } else {
          writeReplicaSetCache.putIfAbsent(replicaSet, PipelinedStoreUtils.getSitesToWrite(route, replicaSet));
          sites = writeReplicaSetCache.get(replicaSet);
        }
      }
      if (sites == null || sites.isEmpty()) {
        throw new NoSiteAvailableToHandleException();
      }
      return sites;
    } catch (Exception ex) {
      if (ex instanceof NoSiteAvailableToHandleException || ex instanceof PipelinedStoreDataCorruptException) {
        throw ex;
      } else {
        throw new NoSiteAvailableToHandleException(ex);
      }
    }
  }

  private <W, X> CompletableFuture<StoreOperationResponse<W>> runClientOperation(X data, SiteId site,
      StoreOperationResponse<W> response, String methodName, String metricName) {
    if (response != null && response.getError() == null) { //Already completed
      return CompletableFuture.completedFuture(response);
    } else {
      publisher.incrementMetric(metricName, (response != null));
      if (!clients.containsKey(site)) {
        LOG.error("Site is not available for queries, Falling back. site: {}", site);
        return CompletableFuture.completedFuture(new StoreOperationResponse<>(null, new SiteNotAvailable(), site));
      }
      try {
        Class<?> clazz = (methodName.equals(GET_METHOD_NAME) && !data.getClass().equals(GetRow.class)) ?
            GetRow.class :
            data.getClass();
        Method method = AsyncStoreClient.class.getMethod(methodName, clazz);
        CompletableFuture<W> output = (CompletableFuture<W>) method.invoke(clients.get(site), data);
        return output.handleAsync((value, error) -> {
          if (error != null && !(error instanceof StoreDataNotFoundException)) {
            LOG.error("Failed to {}, site: {}, error: {}", methodName, site, error.getMessage());
          } else if (error == null) {
            LOG.debug("Successful {}, site: {}", methodName, site);
          }
          return new StoreOperationResponse<>(value, error, site);
        }, executor);
      } catch (Exception ex) {
        // Method Invocation failures
        return CompletableFuture.completedFuture(new StoreOperationResponse<>(null, ex, site));
      }
    }
  }

  @SuppressWarnings("java:S1612")
  private <W, X> CompletableFuture<List<StoreOperationResponse<W>>> runBatchClientOperation(List<X> data, SiteId site,
      List<StoreOperationResponse<W>> responses, String method, String metricName) {

    if ((responses != null) && !(
        responses.stream().filter(response -> (response.getError() == null)).collect(Collectors.toList())).isEmpty()) {
      return CompletableFuture.completedFuture(responses);
    } else {
      publisher.incrementMetric(metricName, (responses != null));
      if (!clients.containsKey(site)) {
        LOG.error("Site is not available for queries, Falling back. site: {}", site);
        return CompletableFuture.completedFuture(
            data.stream().map(d -> (new StoreOperationResponse<W>(null, new SiteNotAvailable(), site)))
                .collect(Collectors.toList()));
      }
      try {
        List<CompletableFuture<StoreOperationResponse<W>>> futures = (
            (List<CompletableFuture<W>>) AsyncStoreClient.class.getMethod(method, List.class)
                .invoke(clients.get(site), data)).stream().map(future -> future.handleAsync((value, error) -> {
            if (error != null && !(error instanceof StoreDataNotFoundException)) {
              LOG.error("Failed to {}, site: {}, error: {}", method, site, error.getMessage());
            } else if (error == null) {
              LOG.debug("Successful {}, site: {}", method, site);
            }
            return new StoreOperationResponse<>(value, error, site);
          }, executor)).collect(Collectors.toList());
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]))
                .thenApplyAsync(v -> futures.stream().map(future -> future.join()).collect(Collectors.toList()));
      } catch (Exception ex) {
        // Method Invocation failures
        return CompletableFuture.completedFuture(
            data.stream().map(d -> new StoreOperationResponse<W>(null, ex, site)).collect(Collectors.toList()));
      }
    }
  }

  /**
   * @return connected clients with siteid as key and storeclient instance as value
   */
  public Map<SiteId, AsyncStoreClient> getConnectedClients() {
    return clients;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void increment(IncrementData incrementData, Optional<T> routeMeta, Optional<U> intentData,
                        Optional<V> circuitBreakerSettings,
                        BiConsumer<PipelinedResponse<StoreOperationResponse<ResultMap>>, Throwable> handler) {

    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(PipelinedClientMetricsPublisher.INCREMENT_TIMER);
    publisher.incrementMetric(PipelinedClientMetricsPublisher.INCREMENT_INIT);
    try {
      List<SiteId> sites = getSites(routeMeta, false);
      CompletableFuture<StoreOperationResponse<ResultMap>> future = CompletableFuture.completedFuture(null);
      for (SiteId site : sites) {
        future = future.thenComposeAsync(value -> runClientOperation(incrementData, site, value, INCREMENT_METHOD_NAME,
          PipelinedClientMetricsPublisher.INCREMENT_FALLBACK), executor);
      }

      future.whenCompleteAsync((result, error) -> {
        handler.accept(
          new PipelinedResponse<>(result, !(sites.get(0).equals(result.getSite()))),
          error);
        publisher.incrementMetric(PipelinedClientMetricsPublisher.INCREMENT_COMPLETE);
        timer.close();
      }, executor);
    } catch (NoSiteAvailableToHandleException | PipelinedStoreDataCorruptException ex) {
      handler.accept(null, ex);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.INCREMENT_NO_SITES);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.INCREMENT_COMPLETE);
      timer.close();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override public void put(StoreData data, Optional<T> routeMeta, Optional<U> intentData,
                            Optional<V> circuitBreaketrSettings,
                            BiConsumer<PipelinedResponse<StoreOperationResponse<Void>>, Throwable> handler) {

    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(PipelinedClientMetricsPublisher.PUT_TIMER);
    publisher.incrementMetric(PipelinedClientMetricsPublisher.PUT_INIT);
    try {
      List<SiteId> sites = getSites(routeMeta, false);
      CompletableFuture<StoreOperationResponse<Void>> future = CompletableFuture.completedFuture(null);
      for (SiteId site : sites) {
        future = future.thenComposeAsync(value -> runClientOperation(data, site, value, PUT_METHOD_NAME,
            PipelinedClientMetricsPublisher.PUT_FALLBACK), executor);
      }

      future.whenCompleteAsync((result, error) -> {
        handler.accept(
                new PipelinedResponse<>(result, !(sites.get(0).equals(result.getSite()))),
            error);
        publisher.incrementMetric(PipelinedClientMetricsPublisher.PUT_COMPLETE);
        timer.close();
      }, executor);
    } catch (NoSiteAvailableToHandleException | PipelinedStoreDataCorruptException ex) {
      handler.accept(null, ex);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.PUT_NO_SITES);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.PUT_COMPLETE);
      timer.close();
    }
  }

  @Override
  public void batch(BatchData data, Optional<T> routeMeta, Optional<U> intentData,
      Optional<V> circuitBreakerSettings,
      BiConsumer<PipelinedResponse<StoreOperationResponse<Void>>, Throwable> handler) {
    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(PipelinedClientMetricsPublisher.BATCH_TIMER);
    publisher.incrementMetric(PipelinedClientMetricsPublisher.BATCH_INIT);
    try {
      List<SiteId> sites = getSites(routeMeta, false);
      CompletableFuture<StoreOperationResponse<Void>> future = CompletableFuture.completedFuture(null);
      for (SiteId site : sites) {
        future = future.thenComposeAsync(
            value -> runClientOperation(data, site, value, BATCH_METHOD_NAME,
                PipelinedClientMetricsPublisher.BATCH_FALLBACK), executor);
      }

      future.whenCompleteAsync((result, error) -> {
        handler.accept(
            new PipelinedResponse<>(result, !(sites.get(0).equals(result.getSite()))),
            error);
        publisher.incrementMetric(PipelinedClientMetricsPublisher.BATCH_COMPLETE);
        timer.close();
      }, executor);
    } catch (NoSiteAvailableToHandleException | PipelinedStoreDataCorruptException ex) {
      handler.accept(null, ex);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.BATCH_NO_SITES);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.BATCH_COMPLETE);
      timer.close();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override public void put(List<StoreData> dataList, Optional<T> routeMeta, Optional<U> intentData,
                            Optional<V> circuitBreaketrSettings,
                            BiConsumer<PipelinedResponse<List<StoreOperationResponse<Void>>>, Throwable> handler) {

    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(PipelinedClientMetricsPublisher.BATCH_PUT_TIMER);
    publisher.incrementMetric(PipelinedClientMetricsPublisher.BATCH_PUT_INIT);
    try {
      List<SiteId> sites = getSites(routeMeta, false);
      CompletableFuture<List<StoreOperationResponse<Void>>> future = CompletableFuture.completedFuture(null);
      for (SiteId site : sites) {
        future = future.thenComposeAsync(value -> runBatchClientOperation(dataList, site, value, PUT_METHOD_NAME,
            PipelinedClientMetricsPublisher.BATCH_PUT_FALLBACK), executor);
      }

      future.whenCompleteAsync((results, error) -> {
        boolean isStale =
            !results.stream().filter(result -> !(sites.get(0).equals(result.getSite()))).collect(Collectors.toList())
                .isEmpty();
        handler.accept(new PipelinedResponse<>(results, isStale), error);
        publisher.incrementMetric(PipelinedClientMetricsPublisher.BATCH_PUT_COMPLETE);
        timer.close();
      }, executor);
    } catch (NoSiteAvailableToHandleException | PipelinedStoreDataCorruptException ex) {
      handler.accept(null, ex);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.BATCH_PUT_NO_SITES);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.BATCH_PUT_COMPLETE);
      timer.close();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override public void checkAndPut(CheckAndStoreData data, Optional<T> routeMeta, Optional<U> intentData,
                                    Optional<V> circuitBreakerSettings,
                                    BiConsumer<PipelinedResponse<StoreOperationResponse<Boolean>>, Throwable> handler) {

    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(PipelinedClientMetricsPublisher.CAS_TIMER);
    publisher.incrementMetric(PipelinedClientMetricsPublisher.CAS_INIT);
    try {
      List<SiteId> sites = getSites(routeMeta, false);
      CompletableFuture<StoreOperationResponse<Boolean>> future = CompletableFuture.completedFuture(null);
      for (SiteId site : sites) {
        future = future.thenComposeAsync(value -> runClientOperation(data, site, value, CHECK_PUT_METHOD_NAME,
            PipelinedClientMetricsPublisher.CAS_FALLBACK), executor);
      }

      future.whenCompleteAsync((result, error) -> {
        handler.accept(
                new PipelinedResponse<>(result, !(sites.get(0).equals(result.getSite()))),
            error);
        publisher.incrementMetric(PipelinedClientMetricsPublisher.CAS_COMPLETE);
        timer.close();
      }, executor);
    } catch (NoSiteAvailableToHandleException | PipelinedStoreDataCorruptException ex) {
      handler.accept(null, ex);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.CAS_NO_SITES);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.CAS_COMPLETE);
      timer.close();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override public void append(StoreData data, Optional<T> routeMeta, Optional<U> intentData,
                               Optional<V> circuitBreakerSettings,
                               BiConsumer<PipelinedResponse<StoreOperationResponse<ResultMap>>, Throwable> handler) {

    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(PipelinedClientMetricsPublisher.APPEND_TIMER);
    publisher.incrementMetric(PipelinedClientMetricsPublisher.APPEND_INIT);
    try {
      List<SiteId> sites = getSites(routeMeta, false);
      CompletableFuture<StoreOperationResponse<ResultMap>> future = CompletableFuture.completedFuture(null);
      for (SiteId site : sites) {
        future = future.thenComposeAsync(value -> runClientOperation(data, site, value, APPEND_METHOD_NAME,
            PipelinedClientMetricsPublisher.APPEND_FALLBACK), executor);
      }

      future.whenCompleteAsync((result, error) -> {
        handler.accept(
                new PipelinedResponse<>(result, !(sites.get(0).equals(result.getSite()))),
            error);
        publisher.incrementMetric(PipelinedClientMetricsPublisher.APPEND_COMPLETE);
        timer.close();
      }, executor);
    } catch (NoSiteAvailableToHandleException | PipelinedStoreDataCorruptException ex) {
      handler.accept(null, ex);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.APPEND_NO_SITES);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.APPEND_COMPLETE);
      timer.close();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override public void delete(List<DeleteData> data, Optional<T> routeMeta, Optional<U> intentData,
                               Optional<V> circuitBreakerSettings,
                               BiConsumer<PipelinedResponse<List<StoreOperationResponse<Void>>>, Throwable> handler) {

    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(PipelinedClientMetricsPublisher.BATCH_DELETE_TIMER);
    publisher.incrementMetric(PipelinedClientMetricsPublisher.BATCH_DELETE_INIT);
    try {
      List<SiteId> sites = getSites(routeMeta, false);
      CompletableFuture<List<StoreOperationResponse<Void>>> future = CompletableFuture.completedFuture(null);
      for (SiteId site : sites) {
        future = future.thenComposeAsync(value -> runBatchClientOperation(data, site, value, DELETE_METHOD_NAME,
            PipelinedClientMetricsPublisher.BATCH_DELETE_FALLBACK), executor);
      }

      future.whenCompleteAsync((results, error) -> {
        boolean isStale =
            !results.stream().filter(result -> !(sites.get(0).equals(result.getSite()))).collect(Collectors.toList())
                .isEmpty();
        handler.accept(new PipelinedResponse<>(results, isStale), error);
        publisher.incrementMetric(PipelinedClientMetricsPublisher.BATCH_DELETE_COMPLETE);
        timer.close();
      }, executor);
    } catch (NoSiteAvailableToHandleException | PipelinedStoreDataCorruptException ex) {
      handler.accept(null, ex);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.BATCH_DELETE_NO_SITES);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.BATCH_DELETE_COMPLETE);
      timer.close();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override public void checkAndDelete(CheckAndDeleteData data, Optional<T> routeMeta, Optional<U> intentData,
                                       Optional<V> circuitBreakerSettings,
                                       BiConsumer<PipelinedResponse<StoreOperationResponse<Boolean>>, Throwable> handler) {

    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(PipelinedClientMetricsPublisher.CHECK_DELETE_TIMER);
    publisher.incrementMetric(PipelinedClientMetricsPublisher.CHECK_DELETE_INIT);
    try {
      List<SiteId> sites = getSites(routeMeta, false);
      CompletableFuture<StoreOperationResponse<Boolean>> future = CompletableFuture.completedFuture(null);
      for (SiteId site : sites) {
        future = future.thenComposeAsync(value -> runClientOperation(data, site, value, CHECK_DELETE_METHOD_NAME,
            PipelinedClientMetricsPublisher.CHECK_DELETE_FALLBACK), executor);
      }
      future.whenCompleteAsync((result, error) -> {
        handler.accept(
                new PipelinedResponse<>(result, !(sites.get(0).equals(result.getSite()))),
            error);
        publisher.incrementMetric(PipelinedClientMetricsPublisher.CHECK_DELETE_COMPLETE);
        timer.close();
      }, executor);
    } catch (NoSiteAvailableToHandleException | PipelinedStoreDataCorruptException ex) {
      handler.accept(null, ex);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.CHECK_DELETE_NO_SITES);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.CHECK_DELETE_COMPLETE);
      timer.close();
    }
  }

  /**
   * {@inheritDoc}
   */
  public void scan(ScanData data, Optional<T> routeMeta, Optional<U> intentData, Optional<V> circuitBreakerSettings,
                   BiConsumer<PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>>, Throwable> handler) {
    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(PipelinedClientMetricsPublisher.SCAN_TIMER);
    publisher.incrementMetric(PipelinedClientMetricsPublisher.SCAN_INIT);
    try {
      List<SiteId> sites = getSites(routeMeta, true);
      CompletableFuture<StoreOperationResponse<Map<String, ResultMap>>> future = CompletableFuture.completedFuture(null);
      for (SiteId site : sites) {
        future = future.thenComposeAsync(value -> runClientOperation(data, site, value, SCAN_METHOD_NAME,
            PipelinedClientMetricsPublisher.SCAN_FALLBACK), executor);
      }

      future.whenCompleteAsync((result, error) -> {
        handler.accept(new PipelinedResponse<>(result,
                !(sites.get(0).equals(result.getSite()))), error);
        publisher.incrementMetric(PipelinedClientMetricsPublisher.SCAN_COMPLETE);
        timer.close();
      }, executor);
    } catch (NoSiteAvailableToHandleException | PipelinedStoreDataCorruptException ex) {
      handler.accept(null, ex);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.SCAN_NO_SITES);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.SCAN_COMPLETE);
      timer.close();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override public <X extends GetRow> void get(X gets, Optional<T> routeMeta, Optional<U> intentData,
                                               Optional<V> circuitBreakerSettings,
                                               BiConsumer<PipelinedResponse<StoreOperationResponse<ResultMap>>, Throwable> handler) {

    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(PipelinedClientMetricsPublisher.GET_TIMER);
    publisher.incrementMetric(PipelinedClientMetricsPublisher.GET_INIT);
    try {
      List<SiteId> sites = getSites(routeMeta, true);
      CompletableFuture<StoreOperationResponse<ResultMap>> future = CompletableFuture.completedFuture(null);
      for (SiteId site : sites) {
        future = future.thenComposeAsync(value -> runClientOperation(gets, site, value, GET_METHOD_NAME,
            PipelinedClientMetricsPublisher.GET_FALLBACK), executor);
      }

      future.whenCompleteAsync((result, error) -> {
        handler.accept(
                new PipelinedResponse<>(result, !(sites.get(0).equals(result.getSite()))),
            error);
        publisher.incrementMetric(PipelinedClientMetricsPublisher.GET_COMPLETE);
        timer.close();
      }, executor);
    } catch (NoSiteAvailableToHandleException | PipelinedStoreDataCorruptException ex) {
      handler.accept(null, ex);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.GET_NO_SITES);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.GET_COMPLETE);
      timer.close();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override public void get(List<? extends GetRow> rows, Optional<T> routeMeta, Optional<U> intentData,
                            Optional<V> circuitBreakerSettings,
                            BiConsumer<PipelinedResponse<List<StoreOperationResponse<ResultMap>>>, Throwable> handler) {

    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(PipelinedClientMetricsPublisher.GET_TIMER);
    publisher.incrementMetric(PipelinedClientMetricsPublisher.BATCH_GET_INIT);
    try {
      List<SiteId> sites = getSites(routeMeta, true);
      CompletableFuture<List<StoreOperationResponse<ResultMap>>> future = CompletableFuture.completedFuture(null);
      for (SiteId site : sites) {
        future = future.thenComposeAsync(value -> runBatchClientOperation(rows, site, value, GET_METHOD_NAME,
            PipelinedClientMetricsPublisher.BATCH_GET_FALLBACK), executor);
      }

      future.whenCompleteAsync((results, error) -> {
        boolean isStale =
            !results.stream().filter(result -> !(sites.get(0).equals(result.getSite()))).collect(Collectors.toList())
                .isEmpty();
        handler.accept(new PipelinedResponse<>(results, isStale), error);
        publisher.incrementMetric(PipelinedClientMetricsPublisher.BATCH_GET_COMPLETE);
        timer.close();
      }, executor);
    } catch (NoSiteAvailableToHandleException | PipelinedStoreDataCorruptException ex) {
      handler.accept(null, ex);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.BATCH_GET_NO_SITES);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.BATCH_GET_COMPLETE);
      timer.close();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override public void getByIndex(GetColumnsMapByIndex get, Optional<T> routeMeta, Optional<U> intentData,
                                   Optional<V> circuitBreakerSettings,
                                   BiConsumer<PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>>, Throwable> handler) {

    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(PipelinedClientMetricsPublisher.INDEX_GET_TIMER);
    publisher.incrementMetric(PipelinedClientMetricsPublisher.INDEX_GET_INIT);
    try {
      List<SiteId> sites = getSites(routeMeta, true);
      CompletableFuture<StoreOperationResponse<List<ColumnsMap>>> future = CompletableFuture.completedFuture(null);
      for (SiteId site : sites) {
        future = future.thenComposeAsync(value -> runClientOperation(get, site, value, GET_INDEX_METHOD_NAME,
            PipelinedClientMetricsPublisher.INDEX_GET_FALLBACK), executor);
      }

      future.whenCompleteAsync((result, error) -> {
        handler.accept(new PipelinedResponse<>(result,
                !(sites.get(0).equals(result.getSite()))), error);
        publisher.incrementMetric(PipelinedClientMetricsPublisher.INDEX_GET_COMPLETE);
        timer.close();
      }, executor);
    } catch (NoSiteAvailableToHandleException | PipelinedStoreDataCorruptException ex) {
      handler.accept(null, ex);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.INDEX_GET_NO_SITES);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.INDEX_GET_COMPLETE);
      timer.close();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override public void getByIndex(GetCellByIndex get, Optional<T> routeMeta, Optional<U> intentData,
                                   Optional<V> circuitBreakerSettings,
                                   BiConsumer<PipelinedResponse<StoreOperationResponse<List<Cell>>>, Throwable> handler) {

    publisher.updateThreadCounter(executor.getActiveCount(), executor.getQueue().size(), executor.getPoolSize());
    Timer.Context timer = publisher.getTimer(PipelinedClientMetricsPublisher.INDEX_GET_TIMER);
    publisher.incrementMetric(PipelinedClientMetricsPublisher.INDEX_GET_INIT);
    try {
      List<SiteId> sites = getSites(routeMeta, true);
      CompletableFuture<StoreOperationResponse<List<Cell>>> future = CompletableFuture.completedFuture(null);
      for (SiteId site : sites) {
        future = future.thenComposeAsync(value -> runClientOperation(get, site, value, GET_INDEX_METHOD_NAME,
            PipelinedClientMetricsPublisher.INDEX_GET_FALLBACK), executor);
      }

      future.whenCompleteAsync((result, error) -> {
        handler.accept(
                new PipelinedResponse<>(result, !(sites.get(0).equals(result.getSite()))),
            error);
        publisher.incrementMetric(PipelinedClientMetricsPublisher.INDEX_GET_COMPLETE);
        timer.close();
      }, executor);
    } catch (NoSiteAvailableToHandleException | PipelinedStoreDataCorruptException ex) {
      handler.accept(null, ex);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.INDEX_GET_NO_SITES);
      publisher.incrementMetric(PipelinedClientMetricsPublisher.INDEX_GET_COMPLETE);
      timer.close();
    }
  }

  @Override public List<AsyncStoreClient> getAsyncStoreClient(Optional<T> routeMeta)
      throws PipelinedStoreDataCorruptException {
    List<SiteId> sites = getSites(routeMeta, true);

    List<AsyncStoreClient> connections = new ArrayList<>();
    for (SiteId siteId : sites) {
      connections.add(clients.get(siteId));
    }
    return connections;
  }

  @Override public AsyncStoreClient getAsyncStoreClient(SiteId siteId) {
    AsyncStoreClient asyncStoreClient = clients.get(siteId);
    if (asyncStoreClient == null) {
      throw new NoSiteAvailableToHandleException();
    }
    return asyncStoreClient;
  }

  @Override public void shutdown() throws IOException, InterruptedException {
    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.MINUTES);
    for (Map.Entry<SiteId, AsyncStoreClient> entry : clients.entrySet()) {
      AsyncStoreClient client = clients.get(entry.getKey());
      client.shutdown();
    }
  }
}
