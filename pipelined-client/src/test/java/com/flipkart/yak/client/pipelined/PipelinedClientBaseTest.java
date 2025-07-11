package com.flipkart.yak.client.pipelined;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.yak.client.AsyncStoreClientImpl;
import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.pipelined.config.MultiRegionStoreConfig;
import com.flipkart.yak.client.pipelined.config.RegionConfig;
import com.flipkart.yak.client.pipelined.decorator.circuitbreakers.HystrixCircuitBreakerDecorator;
import com.flipkart.yak.client.pipelined.decorator.intent.YakIntentStoreDecorator;
import com.flipkart.yak.client.pipelined.exceptions.NoSiteAvailableToHandleException;
import com.flipkart.yak.client.pipelined.intent.IntentStoreClient;
import com.flipkart.yak.client.pipelined.intent.YakIntentStoreClientImpl;
import com.flipkart.yak.client.pipelined.models.*;
import com.flipkart.yak.client.pipelined.models.DataCenter;
import com.flipkart.yak.client.pipelined.route.HotRouter;
import com.flipkart.yak.client.pipelined.route.IntentRoute;
import com.flipkart.yak.client.pipelined.route.StoreRoute;
import com.flipkart.yak.distributor.KeyDistributor;
import com.flipkart.yak.distributor.MurmusHashDistribution;
import com.flipkart.yak.models.Cell;
import com.flipkart.yak.models.ColumnsMap;
import com.flipkart.yak.models.ResultMap;
import com.flipkart.yak.models.StoreData;
import com.flipkart.yak.models.StoreDataBuilder;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixObservableCommand;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hbase.client.Durability;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Matchers;
import static org.mockito.Matchers.eq;
import org.mockito.Mock;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

@RunWith(PowerMockRunner.class) @PowerMockRunnerDelegate(Parameterized.class)
@PrepareForTest({ MasterSlaveYakPipelinedStoreImpl.class, YakIntentStoreClientImpl.class })
public abstract class PipelinedClientBaseTest {

  protected final String FAILED_TO_ROUTE_MESSAGE = "Failed to route";
  protected final String STORE_NAME = "pipelined-client";
  protected final String SITEA = "Site-A";
  protected final String SITEB = "Site-B";
  protected SiteId Region1SiteA = new SiteId(SITEA, Region.REGION_1);
  protected SiteId Region1SiteB = new SiteId(SITEB, Region.REGION_1);
  protected SiteId Region2SiteA = new SiteId(SITEA, Region.REGION_2);
  protected static Map<String, KeyDistributor> keyDistributorMap = new HashMap<>();
  protected MetricRegistry registry = new MetricRegistry();
  protected YakPipelinedStore store;
  protected SyncYakPipelinedStore syncStore;
  protected PipelineConfig storeConfig;
  protected StoreRoute storeRoute;
  protected IntentStoreClient intentStore;
  protected PipelineConfig intentConfig;
  protected IntentRoute intentRoute;
  protected int poolSize = 10;
  protected int timeout = 30;
  protected int siteBootstrapRetryCount = 3;
  protected int siteBootstrapRetryDelayInMillis = 300;

  protected boolean runWithHystrix = false;
  protected boolean runWithIntent = false;

  protected final static String TABLE_NAME = "RandomTableName";
  protected final static String NAMESPACE_NAME = "RandomNamespaceName";
  protected final static String FULL_TABLE_NAME = NAMESPACE_NAME + ":" + TABLE_NAME;
  protected final static String FULL_TABLE_NAME_INDEX = FULL_TABLE_NAME + "_index";

  protected Optional<HystrixSettings> hystrixSettings;
  protected Optional<YakIntentWriteRequest> intentData;
  protected StoreData intentStoreData;

  protected String iRowKey = "irk1-" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
  protected String rowKey1 = "rk1-" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
  protected String rowKey2 = "rk2-" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
  protected String indexKey1 = "ik1-" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
  protected String indexKey2 = "ik2-" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
  protected String routeMetaCh = "CH-" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
  protected String routeMetaHyd = "HYD-" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
  protected Optional<String> routeMetaChOptional = Optional.of(routeMetaCh);
  protected Optional<String> routeMetaHydOptional = Optional.of(routeMetaHyd);
  protected String cf1 = "cf1-" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
  protected String cf2 = "cf2-" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
  protected String cq1 = "cq1-" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
  protected String cq2 = "cq2-" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
  protected String qv1 = "qv1-" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
  protected String qv2 = "qv2-" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();

  @Mock AsyncStoreClientImpl region1SiteAClient;
  @Mock AsyncStoreClientImpl region1SiteBClient;
  @Mock AsyncStoreClientImpl region2SiteAClient;

  @Before public void setupAll() throws Exception {
    keyDistributorMap.put(FULL_TABLE_NAME, new MurmusHashDistribution(128));
    MockitoAnnotations.initMocks(this);
    PowerMockito.mockStatic(Executors.class);

    /************************************************* Mock Objects ************************************************/
    PowerMockito.whenNew(AsyncStoreClientImpl.class)
        .withArguments(eq(getStoreConfigRegion1()), eq(Optional.of(keyDistributorMap)), Matchers.anyInt(), eq(registry))
        .thenReturn(region1SiteAClient);

    PowerMockito.whenNew(AsyncStoreClientImpl.class)
        .withArguments(eq(getStoreConfigRegion2()), eq(Optional.of(keyDistributorMap)), Matchers.anyInt(),
            eq(registry)).thenReturn(region2SiteAClient);

    PowerMockito.whenNew(AsyncStoreClientImpl.class)
        .withArguments(eq(getStoreConfigRegion3()), eq(Optional.of(keyDistributorMap)), Matchers.anyInt(),
            eq(registry)).thenReturn(region1SiteBClient);

    /************************************************* Build Stores ************************************************/
    storeConfig = new PipelineConfig(buildConfig(), timeout, poolSize, STORE_NAME, Optional.of(keyDistributorMap),
            siteBootstrapRetryCount, siteBootstrapRetryDelayInMillis);
    intentConfig = new PipelineConfig(buildConfig(), timeout, poolSize, STORE_NAME, Optional.of(keyDistributorMap));
    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_MANDATORY, WriteConsistency.PRIMARY_MANDATORY,
            router);
    intentRoute = new IntentRoute(Region.REGION_1, IntentConsistency.PRIMARY_MANDATORY, router);

    intentStore = new YakIntentStoreClientImpl(intentConfig, intentRoute, registry);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);
    syncStore = new SyncYakPipelinedStoreImpl(store);

    /************************************** Hystrix settings and Intent Data ***************************************/
    hystrixSettings = Optional.empty();
    intentData = Optional.empty();
    if (runWithHystrix) {
      HystrixObservableCommand.Setter storeSettings =
          HystrixObservableCommand.Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("store-settings"));
      storeSettings
          .andCommandPropertiesDefaults(HystrixCommandProperties.Setter().withExecutionTimeoutInMilliseconds(10000));
      HystrixSettings settings = new HystrixSettings(storeSettings);
      hystrixSettings = Optional.of(new HystrixSettings(storeSettings));
    }
    if (runWithIntent) {
      HystrixObservableCommand.Setter storeSettings =
          HystrixObservableCommand.Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("intent-settings"));
      storeSettings
          .andCommandPropertiesDefaults(HystrixCommandProperties.Setter().withExecutionTimeoutInMilliseconds(20000));
      HystrixSettings settings = new HystrixSettings(storeSettings);
      intentStoreData =
          new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(iRowKey.getBytes()).addColumn(cf1, cq1, qv1.getBytes())
              .build();
      when(region2SiteAClient.put(eq(intentStoreData))).thenReturn(CompletableFuture.completedFuture(null));
      intentData = Optional.of(new YakIntentWriteRequest<String, HystrixSettings>(intentStoreData, routeMetaHydOptional,
          Optional.of(new HystrixSettings(storeSettings))));
    }
  }

  private SiteConfig getDefaultConfig() {
    Map<String, String> hbaseConfig = new HashMap<>();
    hbaseConfig = new HashMap<String, String>();
    hbaseConfig.put("hbase.client.retries.number", "3");
    hbaseConfig.put("hbase.client.max.perserver.tasks", "20");
    hbaseConfig.put("hbase.client.max.perregion.tasks", "2");
    hbaseConfig.put("hbase.client.ipc.pool.type", "RoundRobinPool");
    hbaseConfig.put("hbase.client.ipc.pool.size", "10");
    hbaseConfig.put("hbase.client.operation.timeout", "5000");
    hbaseConfig.put("hbase.client.meta.operation.timeout", "2000");
    hbaseConfig.put("hbase.rpc.timeout", "1000");
    hbaseConfig.put("hbase.rpc.shortoperation.timeout", "500");

    return new SiteConfig().withDurabilityThreshold(Durability.SYNC_WAL.name()).withHbaseConfig(hbaseConfig);
  }

  protected SiteConfig getStoreConfigRegion1() {
    SiteConfig siteConfig = getDefaultConfig();
    siteConfig.withStoreName(Region.REGION_1.getName() + "-" + SITEA);
    siteConfig.getHbaseConfig()
        .put("hbase.zookeeper.quorum", "preprod-yak-z1-zk-1,preprod-yak-z1-zk-2,preprod-yak-z1-zk-3");
    return siteConfig;
  }

  protected SiteConfig getStoreConfigRegion2() {
    SiteConfig siteConfig = getDefaultConfig();
    siteConfig.withStoreName(Region.REGION_2.getName() + "-" + SITEA);
    siteConfig.getHbaseConfig()
        .put("hbase.zookeeper.quorum", "preprod-yak-z2-zk-1,preprod-yak-z2-zk-2,preprod-yak-z2-zk-3");
    return siteConfig;
  }

  protected SiteConfig getStoreConfigRegion3() {
    SiteConfig siteConfig = getDefaultConfig();
    siteConfig.withStoreName(Region.REGION_1.getName() + "-" + SITEB);
    siteConfig.getHbaseConfig()
        .put("hbase.zookeeper.quorum", "preprod-yak-z3-zk-1,preprod-yak-z3-zk-2,preprod-yak-z3-zk-3");
    return siteConfig;
  }

  protected MultiRegionStoreConfig buildConfig() {
    MultiRegionStoreConfig multiRegionStoreConfig = new MultiRegionStoreConfig();
    multiRegionStoreConfig.setDefaultConfig(new SiteConfig());

    RegionConfig region1Config = new RegionConfig(new HashMap<>());
    region1Config.addSite(SITEA, getStoreConfigRegion1());
    region1Config.addSite(SITEB, getStoreConfigRegion3());

    RegionConfig region2Config = new RegionConfig(new HashMap<>());
    region2Config.addSite(SITEA, getStoreConfigRegion2());

    Map<DataCenter, RegionConfig> regions = new HashMap<>();
    regions.put(Region.REGION_1, region1Config);
    regions.put(Region.REGION_2, region2Config);
    multiRegionStoreConfig.setRegions(regions);
    return multiRegionStoreConfig;
  }

  protected HotRouter nullRoute = new HotRouter<MasterSlaveReplicaSet, String>() {
    @Override public MasterSlaveReplicaSet getReplicaSet(Optional<String> routeMeta)
        throws NoSiteAvailableToHandleException {
      return null;
    }
  };

  protected HotRouter nullValuesRoute = new HotRouter<MasterSlaveReplicaSet, String>() {
    @Override public MasterSlaveReplicaSet getReplicaSet(Optional<String> routeMeta)
        throws NoSiteAvailableToHandleException {
      return new MasterSlaveReplicaSet(null, null);
    }
  };

  protected HotRouter localPreferredRouter = new HotRouter<MasterSlaveReplicaSet, String>() {
    @Override public MasterSlaveReplicaSet getReplicaSet(Optional<String> routeMeta)
        throws NoSiteAvailableToHandleException {
      return new MasterSlaveReplicaSet(Region1SiteA, Arrays.asList(Region2SiteA, Region1SiteB));
    }
  };

  protected HotRouter router = new HotRouter<MasterSlaveReplicaSet, String>() {
    @Override public MasterSlaveReplicaSet getReplicaSet(Optional<String> routeMeta)
        throws NoSiteAvailableToHandleException {
      MasterSlaveReplicaSet replicaSet;
      if (routeMeta.isPresent()) {
        String key = routeMeta.get();
        if (key.startsWith("CH")) {
          replicaSet = new MasterSlaveReplicaSet(Region1SiteA, Arrays.asList(Region1SiteB, Region2SiteA));
        } else if (key.startsWith("HYD")) {
          replicaSet = new MasterSlaveReplicaSet(Region2SiteA, Arrays.asList(Region1SiteA, Region1SiteB));
        } else {
          throw new NoSiteAvailableToHandleException(FAILED_TO_ROUTE_MESSAGE);
        }
      } else {
        throw new NoSiteAvailableToHandleException(FAILED_TO_ROUTE_MESSAGE);
      }
      return replicaSet;
    }
  };

  protected YakPipelinedStore generatePipelinedStore(PipelineConfig config, StoreRoute storeRoute,
      MetricRegistry registry, IntentStoreClient intentStoreClient) throws Exception {
    if (runWithIntent && runWithHystrix) {
      return new HystrixCircuitBreakerDecorator(new YakIntentStoreDecorator(
          new HystrixCircuitBreakerDecorator(new MasterSlaveYakPipelinedStoreImpl(config, storeRoute, registry)),
          intentStoreClient));
    } else if (runWithHystrix) {
      return new HystrixCircuitBreakerDecorator(new MasterSlaveYakPipelinedStoreImpl(config, storeRoute, registry));
    } else if (runWithIntent) {
      return new YakIntentStoreDecorator(new MasterSlaveYakPipelinedStoreImpl(config, storeRoute, registry),
          intentStoreClient);
    } else {
      return new MasterSlaveYakPipelinedStoreImpl(config, storeRoute, registry);
    }
  }

  protected <T> BiConsumer<PipelinedResponse<StoreOperationResponse<T>>, Throwable> responseHandler(
      CompletableFuture<PipelinedResponse<StoreOperationResponse<T>>> future) {
    return (response, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
      } else {
        future.complete(response);
      }
    };
  }

  protected <T> BiConsumer<PipelinedResponse<List<StoreOperationResponse<T>>>, Throwable> responsesHandler(
      CompletableFuture<PipelinedResponse<List<StoreOperationResponse<T>>>> future) {
    return (response, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
      } else {
        future.complete(response);
      }
    };
  }

  protected ResultMap buildResultMap(String rowkey, String cf, String cq, String qv) {
    ResultMap resultMap = new ResultMap();
    ColumnsMap columnsMap = new ColumnsMap();
    columnsMap.put(cq1, new Cell(qv1.getBytes()));
    resultMap.put(cf1, columnsMap);
    return resultMap;
  }
}
