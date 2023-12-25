package com.flipkart.yak.client.pipelined;

import com.flipkart.yak.client.AsyncStoreClientImpl;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.client.pipelined.exceptions.NoSiteAvailableToHandleException;
import com.flipkart.yak.client.pipelined.exceptions.PipelinedStoreException;
import com.flipkart.yak.client.pipelined.models.*;
import com.flipkart.yak.client.pipelined.models.Region;
import com.flipkart.yak.client.pipelined.route.StoreRoute;
import com.flipkart.yak.models.CheckAndStoreData;
import com.flipkart.yak.models.StoreDataBuilder;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static junit.framework.Assert.assertNotNull;
import org.apache.hadoop.hbase.CompareOperator;
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

public class PipelinedClientCheckPutTest extends PipelinedClientBaseTest {

  private CheckAndStoreData checkAndStoreData;

  @Parameterized.Parameters public static Collection parameters() {
    return Arrays.asList(new Object[][] { { true, true }, { false, false }, { true, false }, { false, true } });
  }

  public PipelinedClientCheckPutTest(boolean runWithHystrix, boolean runWithIntent) {
    this.runWithHystrix = runWithHystrix;
    this.runWithIntent = runWithIntent;
  }

  @Before public void setup() throws Exception {
    checkAndStoreData =
        new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).addColumn(cf1, cq1, "2".getBytes())
            .buildWithCheckAndVerifyColumn(cf1, cq1, qv1.getBytes(), CompareOperator.EQUAL.name());
  }

  @After public void tearDown() throws IOException, InterruptedException {
    store.shutdown();
  }

  @Test public void testCheckPutToRegion1SiteA() throws ExecutionException, InterruptedException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> future = new CompletableFuture<>();
    when(region1SiteAClient.checkAndPut(eq(checkAndStoreData))).thenReturn(CompletableFuture.completedFuture(true));
    store.checkAndPut(checkAndStoreData, routeKeyChOptional, intentData, hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Boolean>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == true));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteA)));
    assertTrue("Should not be stale", (!response.isStale()));
    verify(region1SiteAClient, times(1)).checkAndPut(checkAndStoreData);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testCheckPutToRegion1SiteASync()
          throws ExecutionException, InterruptedException, TimeoutException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> future = new CompletableFuture<>();
    when(region1SiteAClient.checkAndPut(eq(checkAndStoreData))).thenReturn(CompletableFuture.completedFuture(true));
    PipelinedResponse<StoreOperationResponse<Boolean>> response =
        syncStore.checkAndPut(checkAndStoreData, routeKeyChOptional, intentData, hystrixSettings);
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == true));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteA)));
    assertTrue("Should not be stale", (!response.isStale()));
    verify(region1SiteAClient, times(1)).checkAndPut(checkAndStoreData);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testCheckPutToRegion1SiteAWithNoIntent() throws ExecutionException, InterruptedException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> future = new CompletableFuture<>();
    when(region1SiteAClient.checkAndPut(eq(checkAndStoreData))).thenReturn(CompletableFuture.completedFuture(true));
    store
        .checkAndPut(checkAndStoreData, routeKeyChOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Boolean>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == true));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteA)));
    assertTrue("Should not be stale", (!response.isStale()));
    verify(region1SiteAClient, times(1)).checkAndPut(checkAndStoreData);
    verify(region2SiteAClient, times(0)).put(intentStoreData);
  }

  @Test public void testCheckPutToRegion1SiteAWithIntentFailure() throws InterruptedException {
    String errorMessage = "Failed";
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> future = new CompletableFuture<>();
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException(errorMessage));
    when(region2SiteAClient.put(eq(intentStoreData))).thenReturn(respFuture);
    when(region1SiteAClient.checkAndPut(eq(checkAndStoreData))).thenReturn(CompletableFuture.completedFuture(true));
    store.checkAndPut(checkAndStoreData, routeKeyChOptional, intentData, hystrixSettings, responseHandler(future));
    try {
      future.get();
      assertTrue("Should have thrown exception", false || (!runWithIntent));
      verify(region1SiteAClient, times(1)).checkAndPut(checkAndStoreData);
    } catch (ExecutionException ex) {
      assertTrue("Expect PipelinedException to be thrown", ex.getCause() != null);
      assertTrue("Expect PipelinedException to be thrown", ex.getCause() instanceof PipelinedStoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
      verify(region1SiteAClient, times(0)).checkAndPut(checkAndStoreData);
    }
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testCheckPutToRegion2SiteA() throws ExecutionException, InterruptedException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> future = new CompletableFuture<>();
    when(region2SiteAClient.checkAndPut(eq(checkAndStoreData))).thenReturn(CompletableFuture.completedFuture(true));
    store.checkAndPut(checkAndStoreData, routeKeyHydOptional, intentData, hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Boolean>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == true));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region2SiteA)));
    assertTrue("Should not be stale", (!response.isStale()));
    verify(region1SiteAClient, times(0)).checkAndPut(checkAndStoreData);
    verify(region2SiteAClient, times(1)).checkAndPut(checkAndStoreData);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testCheckPutToRegion1SiteAWithException() throws ExecutionException, InterruptedException {
    String errorMessage = "Failed";
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> future = new CompletableFuture<>();
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException(errorMessage));
    when(region1SiteAClient.checkAndPut(eq(checkAndStoreData))).thenReturn(respFuture);
    store.checkAndPut(checkAndStoreData, routeKeyChOptional, intentData, hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Boolean>> response = future.get();

    assertTrue("Should have null response", (response.getOperationResponse().getValue() == null));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() != null));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError() instanceof PipelinedStoreException));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError().getMessage().equals(errorMessage)));
    verify(region1SiteAClient, times(1)).checkAndPut(checkAndStoreData);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testCheckPutToRegion1SiteAWithExceptionSync()
          throws ExecutionException, InterruptedException, TimeoutException {
    String errorMessage = "Failed";
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException(errorMessage));
    when(region1SiteAClient.checkAndPut(eq(checkAndStoreData))).thenReturn(respFuture);
    PipelinedResponse<StoreOperationResponse<Boolean>> response =
        syncStore.checkAndPut(checkAndStoreData, routeKeyChOptional, intentData, hystrixSettings);

    assertTrue("Should have null response", (response.getOperationResponse().getValue() == null));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() != null));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError() instanceof PipelinedStoreException));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError().getMessage().equals(errorMessage)));
    verify(region1SiteAClient, times(1)).checkAndPut(checkAndStoreData);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testCheckPutToRegion1SiteAWithNoSitesException() throws InterruptedException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> future = new CompletableFuture<>();
    when(region1SiteAClient.checkAndPut(eq(checkAndStoreData))).thenReturn(CompletableFuture.completedFuture(true));
    store.checkAndPut(checkAndStoreData, Optional.of("Dummy"), intentData, hystrixSettings, responseHandler(future));
    try {
      future.get();
      assertTrue("should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof NoSiteAvailableToHandleException);
      assertTrue("Incorrect exception message", (ex.getCause().getMessage().equals(FAILED_TO_ROUTE_MESSAGE)));
    }
    verify(region1SiteAClient, times(0)).checkAndPut(checkAndStoreData);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testCheckPutToRegion1SiteAWithSomeSitesInitializeFail() throws Exception {
    PowerMockito.whenNew(AsyncStoreClientImpl.class)
        .withArguments(eq(getStoreConfigRegion1()), eq(Optional.of(keyDistributorMap)), Matchers.anyInt(), eq(registry))
        .thenThrow(new StoreException("No sites are accepting any connections"));

    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_MANDATORY, WriteConsistency.PRIMARY_PREFERRED,
            router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> future = new CompletableFuture<>();
    when(region1SiteBClient.checkAndPut(eq(checkAndStoreData))).thenReturn(CompletableFuture.completedFuture(true));
    store.checkAndPut(checkAndStoreData, routeKeyChOptional, intentData, hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Boolean>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == true));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteB)));
    assertTrue("Should be stale", (response.isStale()));
    verify(region1SiteAClient, times(0)).checkAndPut(checkAndStoreData);
    verify(region1SiteBClient, times(1)).checkAndPut(checkAndStoreData);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);

  }

  @Test public void testCheckPutToRegion1SiteBAsFallback() throws Exception {
    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_MANDATORY, WriteConsistency.PRIMARY_PREFERRED,
            router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> future = new CompletableFuture<>();
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException("Failed"));
    when(region1SiteAClient.checkAndPut(eq(checkAndStoreData))).thenReturn(respFuture);
    when(region1SiteBClient.checkAndPut(eq(checkAndStoreData))).thenReturn(CompletableFuture.completedFuture(true));
    store.checkAndPut(checkAndStoreData, routeKeyChOptional, intentData, hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Boolean>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == true));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteB", (response.getOperationResponse().getSite().equals(Region1SiteB)));
    assertTrue("Should not be stale", (response.isStale()));
    verify(region1SiteAClient, times(1)).checkAndPut(checkAndStoreData);
    verify(region1SiteBClient, times(1)).checkAndPut(checkAndStoreData);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testCheckPutToRegion1SiteBAsDoubleFallback() throws Exception {
    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_MANDATORY, WriteConsistency.PRIMARY_PREFERRED,
            router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> future = new CompletableFuture<>();
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException("Failed"));
    when(region1SiteAClient.checkAndPut(eq(checkAndStoreData))).thenReturn(respFuture);
    when(region1SiteBClient.checkAndPut(eq(checkAndStoreData))).thenReturn(respFuture);
    when(region2SiteAClient.checkAndPut(eq(checkAndStoreData))).thenReturn(CompletableFuture.completedFuture(true));
    store.checkAndPut(checkAndStoreData, routeKeyChOptional, intentData, hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Boolean>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == true));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteB", (response.getOperationResponse().getSite().equals(Region2SiteA)));
    assertTrue("Should not be stale", (response.isStale()));
    verify(region1SiteAClient, times(1)).checkAndPut(checkAndStoreData);
    verify(region1SiteBClient, times(1)).checkAndPut(checkAndStoreData);
    verify(region2SiteAClient, times(1)).checkAndPut(checkAndStoreData);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }
}
