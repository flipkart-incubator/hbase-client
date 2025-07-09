package com.flipkart.yak.client.pipelined;

import com.flipkart.yak.client.AsyncStoreClientImpl;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.client.pipelined.exceptions.NoSiteAvailableToHandleException;
import com.flipkart.yak.client.pipelined.exceptions.PipelinedClientInitializationException;
import com.flipkart.yak.client.pipelined.models.*;
import com.flipkart.yak.client.pipelined.models.DataCenter;
import com.flipkart.yak.client.pipelined.route.StoreRoute;
import com.flipkart.yak.models.GetDataBuilder;
import com.flipkart.yak.models.GetRow;
import com.flipkart.yak.models.ResultMap;
import com.flipkart.yak.models.StoreData;
import com.flipkart.yak.models.StoreDataBuilder;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixObservableCommand;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import static junit.framework.Assert.assertNotNull;
import org.junit.After;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.mockito.Matchers;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.powermock.api.mockito.PowerMockito;

public class PipelinedClientOtherTest extends PipelinedClientBaseTest {
  private StoreData data;

  @Parameterized.Parameters public static Collection parameters() {
    return Arrays.asList(new Object[][] { { true, true }, { false, false }, { true, false }, { false, true } });
  }

  public PipelinedClientOtherTest(boolean runWithHystrix, boolean runWithIntent) {
    this.runWithHystrix = runWithHystrix;
    this.runWithIntent = false;
  }

  @Before public void setup() throws Exception {
    data = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).addColumn(cf1, cq1, qv1.getBytes())
        .build();
  }

  @After public void tearDown() throws IOException, InterruptedException {
    store.shutdown();
  }

  @Test public void testPutToWithAllSitesInitializeFailures() throws Exception {
    String errorMessage = "No sites are accepting any connections";
    PowerMockito.whenNew(AsyncStoreClientImpl.class)
        .withArguments(eq(getStoreConfigRegion1()), eq(Optional.of(keyDistributorMap)), Matchers.anyInt(), eq(registry))
        .thenThrow(new StoreException(errorMessage));

    PowerMockito.whenNew(AsyncStoreClientImpl.class)
        .withArguments(eq(getStoreConfigRegion2()), eq(Optional.of(keyDistributorMap)), Matchers.anyInt(),
            eq(registry)).thenThrow(new StoreException(errorMessage));

    PowerMockito.whenNew(AsyncStoreClientImpl.class)
        .withArguments(eq(getStoreConfigRegion3()), eq(Optional.of(keyDistributorMap)), Matchers.anyInt(),
            eq(registry)).thenThrow(new StoreException(errorMessage));

    try {
      store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);
    } catch (Exception ex) {
      assertTrue("Expect StoreException to be thrown", ex instanceof PipelinedClientInitializationException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getMessage().equals(errorMessage));
    }
    verify(region2SiteAClient, times(0)).put(intentStoreData);
    PowerMockito.verifyNew(AsyncStoreClientImpl.class, times(5))
            .withArguments(eq(getStoreConfigRegion1()), eq(Optional.of(keyDistributorMap)), Matchers.anyInt(), eq(registry));

    PowerMockito.verifyNew(AsyncStoreClientImpl.class, times(5))
            .withArguments(eq(getStoreConfigRegion2()), eq(Optional.of(keyDistributorMap)), Matchers.anyInt(), eq(registry));

    PowerMockito.verifyNew(AsyncStoreClientImpl.class, times(5))
            .withArguments(eq(getStoreConfigRegion3()), eq(Optional.of(keyDistributorMap)), Matchers.anyInt(), eq(registry));
  }

  @Test public void testPutToWithSitesInitializeFailuresRetried() throws Exception {
    String errorMessage = "No sites are accepting any connections";
    PowerMockito.whenNew(AsyncStoreClientImpl.class)
            .withArguments(eq(getStoreConfigRegion1()), eq(Optional.of(keyDistributorMap)), Matchers.anyInt(), eq(registry))
            .thenThrow(new StoreException(errorMessage));

    try {
      store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);
      assertTrue("Exception should not be thrown", true);
    } catch (Exception ex) {
      assertFalse("Exception should not be thrown", true);
    }
    PowerMockito.verifyNew(AsyncStoreClientImpl.class, times(5))
      .withArguments(eq(getStoreConfigRegion1()), eq(Optional.of(keyDistributorMap)), Matchers.anyInt(), eq(registry));

    PowerMockito.verifyNew(AsyncStoreClientImpl.class, times(3))
            .withArguments(eq(getStoreConfigRegion2()), eq(Optional.of(keyDistributorMap)), Matchers.anyInt(), eq(registry));

    PowerMockito.verifyNew(AsyncStoreClientImpl.class, times(3))
            .withArguments(eq(getStoreConfigRegion3()), eq(Optional.of(keyDistributorMap)), Matchers.anyInt(), eq(registry));
  }



  @Test public void testPutToHystrixTimeout() throws Exception {
    HystrixObservableCommand.Setter storeSettings =
        HystrixObservableCommand.Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("store-settings"));
    storeSettings
        .andCommandPropertiesDefaults(HystrixCommandProperties.Setter().withExecutionTimeoutInMilliseconds(100));
    HystrixSettings settings = new HystrixSettings(storeSettings);
    Optional<HystrixSettings> hystrixSettings = Optional.of(new HystrixSettings(storeSettings));

    CompletableFuture<PipelinedResponse<StoreOperationResponse<Void>>> future = new CompletableFuture<>();
    CompletableFuture<Void> responseFuture = CompletableFuture.supplyAsync(() -> {
      try {
        Thread.sleep(500);
      } catch (Exception e) {
        return null;
      }
      return null;
    });
    when(region1SiteAClient.put(eq(data))).thenReturn(responseFuture);
    store.put(data, routeKeyChOptional, intentData, hystrixSettings, responseHandler(future));
    try {
      future.get();
      assertTrue("Should have thrown exception", true);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof TimeoutException);
    }
  }

  @Test public void testGetFromNullValueRoute() throws Exception {
    String FAILED_TO_ROUTE_MESSAGE = "No site is available to route the traffic";
    storeRoute = new StoreRoute(Region.REGION_1, ReadConsistency.LOCAL_PREFERRED, WriteConsistency.PRIMARY_MANDATORY,
        nullValuesRoute);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    ResultMap resultMap = buildResultMap(rowKey1, cf1, cq1, qv1);
    GetRow row = new GetDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).build();
    CompletableFuture<PipelinedResponse<StoreOperationResponse<ResultMap>>> future = new CompletableFuture<>();
    when(region1SiteAClient.get(eq(row))).thenReturn(CompletableFuture.completedFuture(resultMap));
    store.get(row, Optional.of("Dummy"), Optional.empty(), hystrixSettings, responseHandler(future));
    try {
      future.get();
      assertTrue("should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof NoSiteAvailableToHandleException);
      assertTrue("Incorrect exception message", (ex.getCause().getMessage().equals(FAILED_TO_ROUTE_MESSAGE)));
    }
    verify(region1SiteAClient, times(0)).get(row);
  }

  @Test public void testGetFromNullRoute() throws Exception {
    String FAILED_TO_ROUTE_MESSAGE = "No site is available to route the traffic";
    storeRoute = new StoreRoute(Region.REGION_1, ReadConsistency.LOCAL_PREFERRED, WriteConsistency.PRIMARY_MANDATORY,
        nullRoute);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    ResultMap resultMap = buildResultMap(rowKey1, cf1, cq1, qv1);
    GetRow row = new GetDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).build();
    CompletableFuture<PipelinedResponse<StoreOperationResponse<ResultMap>>> future = new CompletableFuture<>();
    when(region1SiteAClient.get(eq(row))).thenReturn(CompletableFuture.completedFuture(resultMap));
    store.get(row, Optional.of("Dummy"), Optional.empty(), hystrixSettings, responseHandler(future));
    try {
      future.get();
      assertTrue("should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof NoSiteAvailableToHandleException);
      assertTrue("Incorrect exception message", (ex.getCause().getMessage().equals(FAILED_TO_ROUTE_MESSAGE)));
    }
    verify(region1SiteAClient, times(0)).get(row);
  }
}
