package com.flipkart.yak.client.pipelined;

import com.flipkart.yak.client.AsyncStoreClientImpl;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.client.pipelined.models.*;
import com.flipkart.yak.models.StoreData;
import com.flipkart.yak.models.StoreDataBuilder;
import com.flipkart.yak.client.pipelined.exceptions.NoSiteAvailableToHandleException;
import com.flipkart.yak.client.pipelined.exceptions.PipelinedStoreException;
import com.flipkart.yak.client.pipelined.route.StoreRoute;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
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

public class PipelinedClientPutTest extends PipelinedClientBaseTest {
  private StoreData data;

  @Parameterized.Parameters public static Collection parameters() {
    return Arrays.asList(new Object[][] { { true, true }, { false, false }, { true, false }, { false, true } });
  }

  public PipelinedClientPutTest(boolean runWithHystrix, boolean runWithIntent) {
    this.runWithHystrix = runWithHystrix;
    this.runWithIntent = runWithIntent;
  }

  @Before public void setup() throws Exception {
    data = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).addColumn(cf1, cq1, qv1.getBytes())
        .build();
  }

  @After public void tearDown() throws IOException, InterruptedException {
    store.shutdown();
  }

  @Test public void testPutToRegion1SiteA() throws ExecutionException, InterruptedException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Void>>> future = new CompletableFuture<>();
    when(region1SiteAClient.put(eq(data))).thenReturn(CompletableFuture.completedFuture(null));
    store.put(data, routeMetaChOptional, intentData, hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Void>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == null));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteA)));
    assertTrue("Should not be stale", (!response.isStale()));
    verify(region1SiteAClient, times(1)).put(data);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testPutToRegion1SiteASync()
          throws ExecutionException, InterruptedException, TimeoutException {
    when(region1SiteAClient.put(eq(data))).thenReturn(CompletableFuture.completedFuture(null));
    PipelinedResponse<StoreOperationResponse<Void>> response =
        syncStore.put(data, routeMetaChOptional, intentData, hystrixSettings);
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == null));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteA)));
    assertTrue("Should not be stale", (!response.isStale()));
    verify(region1SiteAClient, times(1)).put(data);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testPutToRegion1SiteAWithNoIntent() throws ExecutionException, InterruptedException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Void>>> future = new CompletableFuture<>();
    when(region1SiteAClient.put(eq(data))).thenReturn(CompletableFuture.completedFuture(null));
    store.put(data, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Void>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == null));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteA)));
    assertTrue("Should not be stale", (!response.isStale()));
    verify(region1SiteAClient, times(1)).put(data);
    verify(region2SiteAClient, times(0)).put(intentStoreData);
  }

  @Test public void testPutToRegion1SiteAWithIntentFailure() throws InterruptedException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Void>>> future = new CompletableFuture<>();
    when(region1SiteAClient.put(eq(data))).thenReturn(CompletableFuture.completedFuture(null));
    String errorMessage = "Failed";
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException(errorMessage));
    when(region2SiteAClient.put(eq(intentStoreData))).thenReturn(respFuture);
    store.put(data, routeMetaChOptional, intentData, hystrixSettings, responseHandler(future));
    try {
      future.get();
      assertTrue("Should have thrown exception", false || (!runWithIntent));
      verify(region1SiteAClient, times(1)).put(data);
    } catch (ExecutionException ex) {
      assertTrue("Expect PipelinedException to be thrown", ex.getCause() != null);
      assertTrue("Expect PipelinedException to be thrown", ex.getCause() instanceof PipelinedStoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
      verify(region1SiteAClient, times(0)).put(data);
    }
  }

  @Test public void testPutToRegion2SiteA() throws ExecutionException, InterruptedException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Void>>> future = new CompletableFuture<>();
    when(region2SiteAClient.put(eq(data))).thenReturn(CompletableFuture.completedFuture(null));
    store.put(data, routeMetaHydOptional, intentData, hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Void>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == null));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region2 siteA", (response.getOperationResponse().getSite().equals(Region2SiteA)));
    assertTrue("Should not be stale", (!response.isStale()));
    verify(region2SiteAClient, times(1)).put(data);
    verify(region1SiteAClient, times(0)).put(data);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testPutToRegion1SiteAWithException() throws ExecutionException, InterruptedException {
    String errorMessage = "Failed";
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Void>>> future = new CompletableFuture<>();
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException(errorMessage));
    when(region1SiteAClient.put(eq(data))).thenReturn(respFuture);
    store.put(data, routeMetaChOptional, intentData, hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Void>> response = future.get();
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == null));
    assertTrue("Should throw exception", (response.getOperationResponse().getError() != null));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError() instanceof PipelinedStoreException));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError().getMessage().equals(errorMessage)));
    verify(region1SiteAClient, times(1)).put(data);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testPutToRegion1SiteAWithExceptionSync()
          throws ExecutionException, InterruptedException, TimeoutException {
    String errorMessage = "Failed";
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException(errorMessage));
    when(region1SiteAClient.put(eq(data))).thenReturn(respFuture);
    PipelinedResponse<StoreOperationResponse<Void>> response =
        syncStore.put(data, routeMetaChOptional, intentData, hystrixSettings);
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == null));
    assertTrue("Should throw exception", (response.getOperationResponse().getError() != null));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError() instanceof PipelinedStoreException));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError().getMessage().equals(errorMessage)));
    verify(region1SiteAClient, times(1)).put(data);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testPutToRegion1SiteAWithNoSitesException() throws InterruptedException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Void>>> future = new CompletableFuture<>();
    store.put(data, Optional.of("Dummy"), intentData, hystrixSettings, responseHandler(future));
    try {
      future.get();
      assertTrue("should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof NoSiteAvailableToHandleException);
      assertTrue("Incorrect exception message", (ex.getCause().getMessage().equals(FAILED_TO_ROUTE_MESSAGE)));
    }
    verify(region1SiteAClient, times(0)).put(data);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testPutToRegion1SiteAWithSomeSitesInitializeFail() throws Exception {
    PowerMockito.whenNew(AsyncStoreClientImpl.class)
        .withArguments(eq(getStoreConfigRegion1()), eq(Optional.of(keyDistributorMap)), Matchers.anyInt(), eq(registry))
        .thenThrow(new StoreException("No sites are accepting any connections"));

    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_MANDATORY, WriteConsistency.PRIMARY_PREFERRED,
            router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Void>>> future = new CompletableFuture<>();
    when(region1SiteBClient.put(eq(data))).thenReturn(CompletableFuture.completedFuture(null));
    store.put(data, routeMetaChOptional, intentData, hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Void>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == null));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteB", (response.getOperationResponse().getSite().equals(Region1SiteB)));
    assertTrue("Should be stale", (response.isStale()));
    verify(region1SiteAClient, times(0)).put(data);
    verify(region1SiteBClient, times(1)).put(data);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testPutToRegion1SiteBAsFallback() throws Exception {
    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_MANDATORY, WriteConsistency.PRIMARY_PREFERRED,
            router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    CompletableFuture<PipelinedResponse<StoreOperationResponse<Void>>> future = new CompletableFuture<>();
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException("Failed"));
    when(region1SiteAClient.put(eq(data))).thenReturn(respFuture);
    when(region1SiteBClient.put(eq(data))).thenReturn(CompletableFuture.completedFuture(null));
    store.put(data, routeMetaChOptional, intentData, hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Void>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == null));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteB", (response.getOperationResponse().getSite().equals(Region1SiteB)));
    assertTrue("Should not be stale", (response.isStale()));

    verify(region1SiteAClient, times(1)).put(data);
    verify(region1SiteBClient, times(1)).put(data);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testPutToRegion1SiteBAsDoubleFallback() throws Exception {
    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_MANDATORY, WriteConsistency.PRIMARY_PREFERRED,
            router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    CompletableFuture<PipelinedResponse<StoreOperationResponse<Void>>> future = new CompletableFuture<>();
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException("Failed"));
    when(region1SiteAClient.put(eq(data))).thenReturn(respFuture);
    when(region1SiteBClient.put(eq(data))).thenReturn(respFuture);
    when(region2SiteAClient.put(eq(data))).thenReturn(CompletableFuture.completedFuture(null));
    store.put(data, routeMetaChOptional, intentData, hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Void>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == null));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region2 siteA", (response.getOperationResponse().getSite().equals(Region2SiteA)));
    assertTrue("Should not be stale", (response.isStale()));

    verify(region1SiteAClient, times(1)).put(data);
    verify(region1SiteBClient, times(1)).put(data);
    verify(region2SiteAClient, times(1)).put(data);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testPutToRegion1SiteAWithLocalPreferred() throws Exception {
    storeRoute = new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_MANDATORY, WriteConsistency.LOCAL_PREFERRED,
        localPreferredRouter);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    CompletableFuture<PipelinedResponse<StoreOperationResponse<Void>>> future = new CompletableFuture<>();
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException("Exception"));
    when(region1SiteAClient.put(eq(data))).thenReturn(respFuture);
    when(region1SiteBClient.put(eq(data))).thenReturn(CompletableFuture.completedFuture(null));
    store.put(data, routeMetaChOptional, intentData, hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Void>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == null));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteB)));
    assertTrue("Should be stale", (response.isStale()));
    verify(region1SiteAClient, times(1)).put(data);
    verify(region1SiteBClient, times(1)).put(data);
    verify(region2SiteAClient, times(0)).put(data);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testPutToRegion1SiteAWithLocalPreferredOtherDC() throws Exception {
    storeRoute =
        new StoreRoute(Region.REGION_2, ReadConsistency.PRIMARY_MANDATORY, WriteConsistency.LOCAL_PREFERRED,
            localPreferredRouter);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    CompletableFuture<PipelinedResponse<StoreOperationResponse<Void>>> future = new CompletableFuture<>();
    when(region2SiteAClient.put(eq(data))).thenReturn(CompletableFuture.completedFuture(null));
    store.put(data, routeMetaChOptional, intentData, hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Void>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == null));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with hyd siteA", (response.getOperationResponse().getSite().equals(Region2SiteA)));
    assertTrue("Should not be stale", (!response.isStale())); //Local preferred should result in local write.
    verify(region1SiteAClient, times(0)).put(data);
    verify(region1SiteBClient, times(0)).put(data);
    verify(region2SiteAClient, times(1)).put(data);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }
}
