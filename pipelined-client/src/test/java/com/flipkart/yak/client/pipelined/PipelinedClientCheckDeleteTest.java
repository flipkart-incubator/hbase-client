package com.flipkart.yak.client.pipelined;

import com.flipkart.yak.client.AsyncStoreClientImpl;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.client.pipelined.exceptions.NoSiteAvailableToHandleException;
import com.flipkart.yak.client.pipelined.exceptions.PipelinedStoreException;
import com.flipkart.yak.client.pipelined.models.*;
import com.flipkart.yak.client.pipelined.models.DataCenter;
import com.flipkart.yak.client.pipelined.route.StoreRoute;
import com.flipkart.yak.models.CheckAndDeleteData;
import com.flipkart.yak.models.DeleteDataBuilder;
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

public class PipelinedClientCheckDeleteTest extends PipelinedClientBaseTest {

  private CheckAndDeleteData deleteData;

  @Parameterized.Parameters public static Collection parameters() {
    return Arrays.asList(new Object[][] { { true, true }, { false, false }, { true, false }, { false, true } });
  }

  public PipelinedClientCheckDeleteTest(boolean runWithHystrix, boolean runWithIntent) {
    this.runWithHystrix = runWithHystrix;
    this.runWithIntent = runWithIntent;
  }

  @Before public void setup() throws Exception {
    deleteData = new DeleteDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes())
        .buildWithCheckAndVerifyColumn(cf1, cq1, qv1.getBytes(), CompareOperator.EQUAL.name());
  }

  @After public void tearDown() throws IOException, InterruptedException {
    store.shutdown();
  }

  @Test public void testCheckDeleteToRegion1SiteA() throws ExecutionException, InterruptedException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> future = new CompletableFuture<>();
    when(region1SiteAClient.checkAndDelete(eq(deleteData))).thenReturn(CompletableFuture.completedFuture(true));
    store.checkAndDelete(deleteData, routeKeyChOptional, intentData, hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Boolean>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == true));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteA)));
    assertTrue("Should not be stale", (!response.isStale()));
    verify(region1SiteAClient, times(1)).checkAndDelete(deleteData);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testCheckDeleteToRegion1SiteASync()
          throws ExecutionException, InterruptedException, TimeoutException {
    when(region1SiteAClient.checkAndDelete(eq(deleteData))).thenReturn(CompletableFuture.completedFuture(true));
    PipelinedResponse<StoreOperationResponse<Boolean>> response =
        syncStore.checkAndDelete(deleteData, routeKeyChOptional, intentData, hystrixSettings);
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == true));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteA)));
    assertTrue("Should not be stale", (!response.isStale()));
    verify(region1SiteAClient, times(1)).checkAndDelete(deleteData);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testCheckDeleteToRegion1SiteAWithNoIntent() throws ExecutionException, InterruptedException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> future = new CompletableFuture<>();
    when(region1SiteAClient.checkAndDelete(eq(deleteData))).thenReturn(CompletableFuture.completedFuture(true));
    store.checkAndDelete(deleteData, routeKeyChOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Boolean>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == true));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteA)));
    assertTrue("Should not be stale", (!response.isStale()));
    verify(region1SiteAClient, times(1)).checkAndDelete(deleteData);
    verify(region2SiteAClient, times(0)).put(intentStoreData);
  }

  @Test public void testCheckDeleteToRegion1SiteAWithIntentFailure() throws InterruptedException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> future = new CompletableFuture<>();
    when(region1SiteAClient.checkAndDelete(eq(deleteData))).thenReturn(CompletableFuture.completedFuture(true));
    String errorMessage = "Failed";
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException(errorMessage));
    when(region2SiteAClient.put(eq(intentStoreData))).thenReturn(respFuture);
    store.checkAndDelete(deleteData, routeKeyChOptional, intentData, hystrixSettings, responseHandler(future));
    try {
      future.get();
      assertTrue("Should have thrown exception", false || (!runWithIntent));
      verify(region1SiteAClient, times(1)).checkAndDelete(deleteData);
    } catch (ExecutionException ex) {
      assertTrue("Expect PipelinedException to be thrown", ex.getCause() != null);
      assertTrue("Expect PipelinedException to be thrown", ex.getCause() instanceof PipelinedStoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
      verify(region1SiteAClient, times(0)).checkAndDelete(deleteData);
    }
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testCheckDeleteToRegion2SiteA() throws ExecutionException, InterruptedException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> future = new CompletableFuture<>();
    when(region2SiteAClient.checkAndDelete(eq(deleteData))).thenReturn(CompletableFuture.completedFuture(true));
    store.checkAndDelete(deleteData, routeKeyHydOptional, intentData, hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Boolean>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == true));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region2SiteA)));
    assertTrue("Should not be stale", (!response.isStale()));
    verify(region1SiteAClient, times(0)).checkAndDelete(deleteData);
    verify(region2SiteAClient, times(1)).checkAndDelete(deleteData);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testCheckDeleteToRegion1SiteAWithException() throws ExecutionException, InterruptedException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> future = new CompletableFuture<>();
    String errorMessage = "Failed";
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException(errorMessage));
    when(region1SiteAClient.checkAndDelete(eq(deleteData))).thenReturn(respFuture);
    store.checkAndDelete(deleteData, routeKeyChOptional, intentData, hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Boolean>> response = future.get();
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == null));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() != null));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError() instanceof PipelinedStoreException));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError().getMessage().equals(errorMessage)));
    verify(region1SiteAClient, times(1)).checkAndDelete(deleteData);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testCheckDeleteToRegion1SiteAWithExceptionSync()
          throws ExecutionException, InterruptedException, TimeoutException {
    String errorMessage = "Failed";
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException(errorMessage));
    when(region1SiteAClient.checkAndDelete(eq(deleteData))).thenReturn(respFuture);
    PipelinedResponse<StoreOperationResponse<Boolean>> response =
        syncStore.checkAndDelete(deleteData, routeKeyChOptional, intentData, hystrixSettings);
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == null));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() != null));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError() instanceof PipelinedStoreException));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError().getMessage().equals(errorMessage)));
    verify(region1SiteAClient, times(1)).checkAndDelete(deleteData);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testCheckDeleteToRegion1SiteAWithNoSitesException() throws InterruptedException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> future = new CompletableFuture<>();
    store.checkAndDelete(deleteData, Optional.of("Dummy"), intentData, hystrixSettings, responseHandler(future));
    try {
      future.get();
      assertTrue("should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof NoSiteAvailableToHandleException);
      assertTrue("Incorrect exception message", (ex.getCause().getMessage().equals(FAILED_TO_ROUTE_MESSAGE)));
    }
    verify(region1SiteAClient, times(0)).checkAndDelete(deleteData);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testCheckDeleteToRegion1SiteAWithSomeSitesInitializeFail() throws Exception {
    PowerMockito.whenNew(AsyncStoreClientImpl.class)
        .withArguments(eq(getStoreConfigRegion1()), eq(Optional.of(keyDistributorMap)), Matchers.anyInt(), eq(registry))
        .thenThrow(new StoreException("No sites are accepting any connections"));

    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_MANDATORY, WriteConsistency.PRIMARY_PREFERRED,
            router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> future = new CompletableFuture<>();
    when(region1SiteBClient.checkAndDelete(eq(deleteData))).thenReturn(CompletableFuture.completedFuture(true));
    store.checkAndDelete(deleteData, routeKeyChOptional, intentData, hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Boolean>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == true));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteB)));
    assertTrue("Should be stale", (response.isStale()));
    verify(region1SiteAClient, times(0)).checkAndDelete(deleteData);
    verify(region1SiteBClient, times(1)).checkAndDelete(deleteData);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testCheckDeleteToRegion1SiteBAsFallback() throws Exception {
    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_MANDATORY, WriteConsistency.PRIMARY_PREFERRED,
            router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> future = new CompletableFuture<>();

    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException("Failed"));
    when(region1SiteAClient.checkAndDelete(eq(deleteData))).thenReturn(respFuture);
    when(region1SiteBClient.checkAndDelete(eq(deleteData))).thenReturn(CompletableFuture.completedFuture(true));
    store.checkAndDelete(deleteData, routeKeyChOptional, intentData, hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Boolean>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == true));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteB)));
    assertTrue("Should be stale", (response.isStale()));
    verify(region1SiteAClient, times(1)).checkAndDelete(deleteData);
    verify(region1SiteBClient, times(1)).checkAndDelete(deleteData);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testCheckDeleteToRegion1SiteBAsDoubleFallback() throws Exception {
    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_MANDATORY, WriteConsistency.PRIMARY_PREFERRED,
            router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> future = new CompletableFuture<>();

    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException("Failed"));
    when(region1SiteAClient.checkAndDelete(eq(deleteData))).thenReturn(respFuture);
    when(region1SiteBClient.checkAndDelete(eq(deleteData))).thenReturn(respFuture);
    when(region2SiteAClient.checkAndDelete(eq(deleteData))).thenReturn(CompletableFuture.completedFuture(true));
    store.checkAndDelete(deleteData, routeKeyChOptional, intentData, hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Boolean>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == true));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region2SiteA)));
    assertTrue("Should be stale", (response.isStale()));
    verify(region1SiteAClient, times(1)).checkAndDelete(deleteData);
    verify(region1SiteBClient, times(1)).checkAndDelete(deleteData);
    verify(region2SiteAClient, times(1)).checkAndDelete(deleteData);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }
}
