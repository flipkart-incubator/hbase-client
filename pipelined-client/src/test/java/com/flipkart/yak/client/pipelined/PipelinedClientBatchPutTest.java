package com.flipkart.yak.client.pipelined;

import com.flipkart.yak.client.AsyncStoreClientImpl;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.client.pipelined.exceptions.NoSiteAvailableToHandleException;
import com.flipkart.yak.client.pipelined.exceptions.PipelinedStoreException;
import com.flipkart.yak.client.pipelined.models.*;
import com.flipkart.yak.client.pipelined.models.Region;
import com.flipkart.yak.client.pipelined.route.StoreRoute;
import com.flipkart.yak.models.StoreData;
import com.flipkart.yak.models.StoreDataBuilder;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static junit.framework.Assert.assertNotNull;
import org.junit.After;
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

public class PipelinedClientBatchPutTest extends PipelinedClientBaseTest {

  private List<StoreData> dataList;

  @Parameterized.Parameters public static Collection parameters() {
    return Arrays.asList(new Object[][] { { true, true }, { false, false }, { true, false }, { false, true } });
  }

  public PipelinedClientBatchPutTest(boolean runWithHystrix, boolean runWithIntent) {
    this.runWithHystrix = runWithHystrix;
    this.runWithIntent = runWithIntent;
  }

  @Before public void setup() throws Exception {
    StoreData data1 =
        new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).addColumn(cf1, cq1, qv1.getBytes())
            .build();
    StoreData data2 =
        new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey2.getBytes()).addColumn(cf2, cq2, qv2.getBytes())
            .build();
    dataList = Arrays.asList(data1, data2);
  }

  @After public void tearDown() throws IOException, InterruptedException {
    store.shutdown();
  }

  @Test public void testBatchPutToRegion1SiteA() throws ExecutionException, InterruptedException {
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<Void>>>> future = new CompletableFuture<>();
    CompletableFuture<Void> respFuture = CompletableFuture.completedFuture(null);
    when(region1SiteAClient.put(eq(dataList))).thenReturn(Arrays.asList(respFuture, respFuture));
    store.put(dataList, routeKeyChOptional, intentData, hystrixSettings, responsesHandler(future));

    PipelinedResponse<List<StoreOperationResponse<Void>>> response = future.get();
    response.getOperationResponse().stream().forEachOrdered(resp -> {
      assertTrue("Should have null response", (resp.getValue() == null));
      assertTrue("Should not throw exception", (resp.getError() == null));
      assertTrue("Should match with region1 siteA", (resp.getSite().equals(Region1SiteA)));
      assertTrue("Should not be stale", (!response.isStale()));
    });
    verify(region1SiteAClient, times(1)).put(dataList);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testBatchPutToRegion1SiteASync()
          throws ExecutionException, InterruptedException, TimeoutException {
    CompletableFuture<Void> respFuture = CompletableFuture.completedFuture(null);
    when(region1SiteAClient.put(eq(dataList))).thenReturn(Arrays.asList(respFuture, respFuture));

    PipelinedResponse<List<StoreOperationResponse<Void>>> response =
        syncStore.put(dataList, routeKeyChOptional, intentData, hystrixSettings);
    response.getOperationResponse().stream().forEachOrdered(resp -> {
      assertTrue("Should have null response", (resp.getValue() == null));
      assertTrue("Should not throw exception", (resp.getError() == null));
      assertTrue("Should match with region1 siteA", (resp.getSite().equals(Region1SiteA)));
      assertTrue("Should not be stale", (!response.isStale()));
    });
    verify(region1SiteAClient, times(1)).put(dataList);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testBatchPutToRegion1SiteAWithNoIntent() throws ExecutionException, InterruptedException {
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<Void>>>> future = new CompletableFuture<>();
    CompletableFuture<Void> respFuture = CompletableFuture.completedFuture(null);
    when(region1SiteAClient.put(eq(dataList))).thenReturn(Arrays.asList(respFuture, respFuture));
    store.put(dataList, routeKeyChOptional, Optional.empty(), hystrixSettings, responsesHandler(future));

    PipelinedResponse<List<StoreOperationResponse<Void>>> response = future.get();
    response.getOperationResponse().stream().forEachOrdered(resp -> {
      assertTrue("Should have null response", (resp.getValue() == null));
      assertTrue("Should not throw exception", (resp.getError() == null));
      assertTrue("Should match with region1 siteA", (resp.getSite().equals(Region1SiteA)));
      assertTrue("Should not be stale", (!response.isStale()));
    });
    verify(region1SiteAClient, times(1)).put(dataList);
    verify(region2SiteAClient, times(0)).put(intentStoreData);
  }

  @Test public void testBatchPutToRegion1SiteAWithIntentFailure() throws InterruptedException {
    String errorMessage = "Failed";
    CompletableFuture respFailureFuture = new CompletableFuture();
    respFailureFuture.completeExceptionally(new PipelinedStoreException(errorMessage));
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<Void>>>> future = new CompletableFuture<>();
    CompletableFuture<Void> respFuture = CompletableFuture.completedFuture(null);
    when(region1SiteAClient.put(eq(dataList))).thenReturn(Arrays.asList(respFuture, respFuture));
    when(region2SiteAClient.put(eq(intentStoreData))).thenReturn(respFailureFuture);
    store.put(dataList, routeKeyChOptional, intentData, hystrixSettings, responsesHandler(future));

    try {
      future.get();
      assertTrue("Should have thrown exception", false || (!runWithIntent));
      verify(region1SiteAClient, times(1)).put(dataList);
    } catch (ExecutionException ex) {
      assertTrue("Expect PipelinedException to be thrown", ex.getCause() != null);
      assertTrue("Expect PipelinedException to be thrown", ex.getCause() instanceof PipelinedStoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
      verify(region1SiteAClient, times(0)).put(dataList);
    }
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testBatchPutToRegion2SiteA() throws ExecutionException, InterruptedException {
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<Void>>>> future = new CompletableFuture<>();
    CompletableFuture<Void> respFuture = CompletableFuture.completedFuture(null);
    when(region2SiteAClient.put(eq(dataList))).thenReturn(Arrays.asList(respFuture, respFuture));
    store.put(dataList, routeKeyHydOptional, intentData, hystrixSettings, responsesHandler(future));

    PipelinedResponse<List<StoreOperationResponse<Void>>> response = future.get();
    response.getOperationResponse().stream().forEachOrdered(resp -> {
      assertTrue("Should have null response", (resp.getValue() == null));
      assertTrue("Should not throw exception", (resp.getError() == null));
      assertTrue("Should match with region1 siteA", (resp.getSite().equals(Region2SiteA)));
      assertTrue("Should not be stale", (!response.isStale()));
    });
    verify(region1SiteAClient, times(0)).put(dataList);
    verify(region2SiteAClient, times(1)).put(dataList);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testBatchPutToRegion1SiteAWithException() throws ExecutionException, InterruptedException {
    String errorMessage = "Failed";
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<Void>>>> future = new CompletableFuture<>();
    CompletableFuture respFailureFuture = new CompletableFuture();
    respFailureFuture.completeExceptionally(new PipelinedStoreException(errorMessage));
    when(region1SiteAClient.put(eq(dataList))).thenReturn(Arrays.asList(respFailureFuture, respFailureFuture));
    store.put(dataList, routeKeyChOptional, intentData, hystrixSettings, responsesHandler(future));

    PipelinedResponse<List<StoreOperationResponse<Void>>> response = future.get();
    response.getOperationResponse().stream().forEachOrdered(resp -> {
      assertTrue("Should have null response", (resp.getValue() == null));
      assertTrue("Should throw exception", (resp.getError() != null));
      assertTrue("Should throw exception", (resp.getError() instanceof PipelinedStoreException));
      assertTrue("Should throw exception", (resp.getError().getMessage().equals(errorMessage)));
    });
    verify(region1SiteAClient, times(1)).put(dataList);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testBatchPutToRegion1SiteAWithExceptionSync()
          throws ExecutionException, InterruptedException, TimeoutException {
    String errorMessage = "Failed";
    CompletableFuture respFailureFuture = new CompletableFuture();
    respFailureFuture.completeExceptionally(new PipelinedStoreException(errorMessage));
    when(region1SiteAClient.put(eq(dataList))).thenReturn(Arrays.asList(respFailureFuture, respFailureFuture));

    PipelinedResponse<List<StoreOperationResponse<Void>>> response =
        syncStore.put(dataList, routeKeyChOptional, intentData, hystrixSettings);
    response.getOperationResponse().stream().forEachOrdered(resp -> {
      assertTrue("Should have null response", (resp.getValue() == null));
      assertTrue("Should throw exception", (resp.getError() != null));
      assertTrue("Should throw exception", (resp.getError() instanceof PipelinedStoreException));
      assertTrue("Should throw exception", (resp.getError().getMessage().equals(errorMessage)));
    });
    verify(region1SiteAClient, times(1)).put(dataList);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testBatchPutToRegion1SiteAWithNoSitesException() throws InterruptedException {
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<Void>>>> future = new CompletableFuture<>();
    CompletableFuture<Void> respFuture = CompletableFuture.completedFuture(null);
    when(region1SiteAClient.put(eq(dataList))).thenReturn(Arrays.asList(respFuture, respFuture));
    store.put(dataList, Optional.of("Dummy"), intentData, hystrixSettings, responsesHandler(future));

    try {
      future.get();
      assertTrue("should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof NoSiteAvailableToHandleException);
      assertTrue("Incorrect exception message", (ex.getCause().getMessage().equals(FAILED_TO_ROUTE_MESSAGE)));
    }
    verify(region1SiteAClient, times(0)).put(dataList);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);

  }

  @Test public void testBatchPutToRegion1SiteAWithSomeSitesInitializeFail() throws Exception {
    PowerMockito.whenNew(AsyncStoreClientImpl.class)
        .withArguments(eq(getStoreConfigRegion1()), eq(Optional.of(keyDistributorMap)), Matchers.anyInt(), eq(registry))
        .thenThrow(new StoreException("No sites are accepting any connections"));
    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_MANDATORY, WriteConsistency.PRIMARY_PREFERRED,
            router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<Void>>>> future = new CompletableFuture<>();
    CompletableFuture<Void> respFuture = CompletableFuture.completedFuture(null);
    when(region1SiteBClient.put(eq(dataList))).thenReturn(Arrays.asList(respFuture, respFuture));
    store.put(dataList, routeKeyChOptional, intentData, hystrixSettings, responsesHandler(future));

    PipelinedResponse<List<StoreOperationResponse<Void>>> response = future.get();
    response.getOperationResponse().stream().forEachOrdered(resp -> {
      assertTrue("Should have null response", (resp.getValue() == null));
      assertTrue("Should not throw exception", (resp.getError() == null));
      assertTrue("Should match with region1 siteA", (resp.getSite().equals(Region1SiteB)));
      assertTrue("Should be stale", (response.isStale()));
    });
    verify(region1SiteAClient, times(0)).put(dataList);
    verify(region1SiteBClient, times(1)).put(dataList);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testBatchPutToRegion1SiteBAsFallback() throws Exception {
    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_MANDATORY, WriteConsistency.PRIMARY_PREFERRED,
            router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<Void>>>> future = new CompletableFuture<>();
    CompletableFuture respFailureFuture = new CompletableFuture();
    respFailureFuture.completeExceptionally(new PipelinedStoreException("Failed"));
    when(region1SiteAClient.put(eq(dataList))).thenReturn(Arrays.asList(respFailureFuture, respFailureFuture));
    CompletableFuture<Void> respFuture = CompletableFuture.completedFuture(null);
    when(region1SiteBClient.put(eq(dataList))).thenReturn(Arrays.asList(respFuture, respFuture));
    store.put(dataList, routeKeyChOptional, intentData, hystrixSettings, responsesHandler(future));

    PipelinedResponse<List<StoreOperationResponse<Void>>> response = future.get();
    response.getOperationResponse().stream().forEachOrdered(resp -> {
      assertTrue("Should have null response", (resp.getValue() == null));
      assertTrue("Should not throw exception", (resp.getError() == null));
      assertTrue("Should match with region1 siteA", (resp.getSite().equals(Region1SiteB)));
      assertTrue("Should be stale", (response.isStale()));
    });
    verify(region1SiteAClient, times(1)).put(dataList);
    verify(region1SiteBClient, times(1)).put(dataList);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testBatchPutToRegion1SiteBAsDoubleFallback() throws Exception {
    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_MANDATORY, WriteConsistency.PRIMARY_PREFERRED,
            router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<Void>>>> future = new CompletableFuture<>();
    CompletableFuture respFailureFuture = new CompletableFuture();
    respFailureFuture.completeExceptionally(new PipelinedStoreException("Failed"));
    when(region1SiteAClient.put(eq(dataList))).thenReturn(Arrays.asList(respFailureFuture, respFailureFuture));
    when(region1SiteBClient.put(eq(dataList))).thenReturn(Arrays.asList(respFailureFuture, respFailureFuture));
    CompletableFuture<Void> respFuture = CompletableFuture.completedFuture(null);
    when(region2SiteAClient.put(eq(dataList))).thenReturn(Arrays.asList(respFuture, respFuture));
    store.put(dataList, routeKeyChOptional, intentData, hystrixSettings, responsesHandler(future));

    PipelinedResponse<List<StoreOperationResponse<Void>>> response = future.get();
    response.getOperationResponse().stream().forEachOrdered(resp -> {
      assertTrue("Should have null response", (resp.getValue() == null));
      assertTrue("Should not throw exception", (resp.getError() == null));
      assertTrue("Should match with region1 siteA", (resp.getSite().equals(Region2SiteA)));
      assertTrue("Should be stale", (response.isStale()));
    });
    verify(region1SiteAClient, times(1)).put(dataList);
    verify(region1SiteBClient, times(1)).put(dataList);
    verify(region2SiteAClient, times(1)).put(dataList);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }
}
