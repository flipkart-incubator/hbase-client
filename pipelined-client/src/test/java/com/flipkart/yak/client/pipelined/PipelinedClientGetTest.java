package com.flipkart.yak.client.pipelined;

import com.flipkart.yak.client.AsyncStoreClientImpl;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.client.pipelined.exceptions.NoSiteAvailableToHandleException;
import com.flipkart.yak.client.pipelined.exceptions.PipelinedStoreException;
import com.flipkart.yak.client.pipelined.models.*;
import com.flipkart.yak.client.pipelined.models.DataCenter;
import com.flipkart.yak.client.pipelined.route.StoreRoute;
import com.flipkart.yak.models.GetColumnMap;
import com.flipkart.yak.models.GetDataBuilder;
import com.flipkart.yak.models.GetRow;
import com.flipkart.yak.models.ResultMap;
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

public class PipelinedClientGetTest extends PipelinedClientBaseTest {
  ResultMap resultMap;
  GetRow row;

  @Parameterized.Parameters public static Collection parameters() {
    return Arrays.asList(new Object[][] { { true }, { false } });
  }

  public PipelinedClientGetTest(boolean runWithHystrix) {
    this.runWithHystrix = runWithHystrix;
  }

  @Before public void setup() throws Exception {
    resultMap = buildResultMap(rowKey1, cf1, cq1, qv1);
    row = new GetDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).build();
  }

  @After public void tearDown() throws IOException, InterruptedException {
    store.shutdown();
  }

  @Test public void testGetFromRegion1SiteA()
      throws ExecutionException, InterruptedException, StoreException, IOException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<ResultMap>>> future = new CompletableFuture<>();
    when(region1SiteAClient.get(eq(row))).thenReturn(CompletableFuture.completedFuture(resultMap));

    store.get(row, routeKeyChOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<ResultMap>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue().equals(resultMap)));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteA)));
    assertTrue("Should not be stale", (!response.isStale()));
    verify(region1SiteAClient, times(1)).get(row);
  }

  @Test public void testGetColumnsMapFromRegion1SiteA()
      throws ExecutionException, InterruptedException, StoreException, IOException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<ResultMap>>> future = new CompletableFuture<>();
    GetColumnMap row = new GetDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).buildForColFamily(cf1);
    when(region1SiteAClient.get(eq(row))).thenReturn(CompletableFuture.completedFuture(resultMap));
    store.get(row, routeKeyChOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<ResultMap>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue().equals(resultMap)));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteA)));
    assertTrue("Should not be stale", (!response.isStale()));
    verify(region1SiteAClient, times(1)).get(row);
  }

  @Test public void testGetFromRegion1SiteASync()
      throws ExecutionException, InterruptedException, TimeoutException {
    when(region1SiteAClient.get(eq(row))).thenReturn(CompletableFuture.completedFuture(resultMap));

    PipelinedResponse<StoreOperationResponse<ResultMap>> response =
        syncStore.get(row, routeKeyChOptional, Optional.empty(), hystrixSettings);
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue().equals(resultMap)));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteA)));
    assertTrue("Should not be stale", (!response.isStale()));
    verify(region1SiteAClient, times(1)).get(row);
  }

  @Test public void testGetFromRegion2SiteA() throws ExecutionException, InterruptedException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<ResultMap>>> future = new CompletableFuture<>();
    when(region2SiteAClient.get(eq(row))).thenReturn(CompletableFuture.completedFuture(resultMap));

    store.get(row, routeKeyHydOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<ResultMap>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue().equals(resultMap)));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region2SiteA)));
    assertTrue("Should not be stale", (!response.isStale()));
    verify(region1SiteAClient, times(0)).get(row);
    verify(region2SiteAClient, times(1)).get(row);
  }

  @Test public void testGetFromRegion1SiteAWithException() throws ExecutionException, InterruptedException {
    String errorMessage = "Failed";
    CompletableFuture<PipelinedResponse<StoreOperationResponse<ResultMap>>> future = new CompletableFuture<>();
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException(errorMessage));
    when(region1SiteAClient.get(eq(row))).thenReturn(respFuture);

    store.get(row, routeKeyChOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<ResultMap>> response = future.get();
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == null));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() != null));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError() instanceof PipelinedStoreException));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError().getMessage().equals(errorMessage)));
    verify(region1SiteAClient, times(1)).get(row);
  }

  @Test public void testGetFromRegion1SiteAWithExceptionSync()
          throws ExecutionException, InterruptedException, TimeoutException {
    String errorMessage = "Failed";
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException(errorMessage));
    when(region1SiteAClient.get(eq(row))).thenReturn(respFuture);

    PipelinedResponse<StoreOperationResponse<ResultMap>> response =
        syncStore.get(row, routeKeyChOptional, Optional.empty(), hystrixSettings);
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == null));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() != null));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError() instanceof PipelinedStoreException));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError().getMessage().equals(errorMessage)));
    verify(region1SiteAClient, times(1)).get(row);
  }

  @Test public void testGetFromRegion1SiteAWithNoSitesException() throws InterruptedException {
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

  @Test public void testGetFromRegion1SiteAWithSomeSitesInitializeFail() throws Exception {
    PowerMockito.whenNew(AsyncStoreClientImpl.class)
        .withArguments(eq(getStoreConfigRegion1()), eq(Optional.of(keyDistributorMap)), Matchers.anyInt(), eq(registry))
        .thenThrow(new StoreException("No sites are accepting any connections"));

    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_PREFERRED, WriteConsistency.PRIMARY_MANDATORY,
            router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    CompletableFuture<PipelinedResponse<StoreOperationResponse<ResultMap>>> future = new CompletableFuture<>();
    when(region1SiteBClient.get(eq(row))).thenReturn(CompletableFuture.completedFuture(resultMap));

    store.get(row, routeKeyChOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<ResultMap>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue().equals(resultMap)));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteB)));
    assertTrue("Should be stale", (response.isStale()));
    verify(region1SiteAClient, times(0)).get(row);
    verify(region1SiteBClient, times(1)).get(row);
  }

  @Test public void testGetFromRegion1SiteBAsFallback() throws Exception {
    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_PREFERRED, WriteConsistency.PRIMARY_MANDATORY,
            router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    CompletableFuture<PipelinedResponse<StoreOperationResponse<ResultMap>>> future = new CompletableFuture<>();
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException("Failed"));
    when(region1SiteAClient.get(eq(row))).thenReturn(respFuture);
    when(region1SiteBClient.get(eq(row))).thenReturn(CompletableFuture.completedFuture(resultMap));

    store.get(row, routeKeyChOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<ResultMap>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue().equals(resultMap)));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteB)));
    assertTrue("Should not be stale", (response.isStale()));
    verify(region1SiteAClient, times(1)).get(row);
    verify(region1SiteBClient, times(1)).get(row);
  }

  @Test public void testGetFromRegion1SiteBAsDoubleFallback() throws Exception {
    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_PREFERRED, WriteConsistency.PRIMARY_MANDATORY,
            router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    CompletableFuture<PipelinedResponse<StoreOperationResponse<ResultMap>>> future = new CompletableFuture<>();
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException("Failed"));
    when(region1SiteAClient.get(eq(row))).thenReturn(respFuture);
    when(region1SiteBClient.get(eq(row))).thenReturn(respFuture);
    when(region2SiteAClient.get(eq(row))).thenReturn(CompletableFuture.completedFuture(resultMap));

    store.get(row, routeKeyChOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<ResultMap>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue().equals(resultMap)));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region2SiteA)));
    assertTrue("Should not be stale", (response.isStale()));
    verify(region1SiteAClient, times(1)).get(row);
    verify(region1SiteBClient, times(1)).get(row);
    verify(region2SiteAClient, times(1)).get(row);
  }

  @Test public void testGetFromRegion1SiteAWithLocalPreferred() throws Exception {
    storeRoute = new StoreRoute(Region.REGION_1, ReadConsistency.LOCAL_PREFERRED, WriteConsistency.PRIMARY_MANDATORY,
        localPreferredRouter);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    ResultMap resultMap = buildResultMap(rowKey1, cf1, cq1, qv1);
    GetRow row = new GetDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).build();
    CompletableFuture<PipelinedResponse<StoreOperationResponse<ResultMap>>> future = new CompletableFuture<>();
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException("Exception"));
    when(region1SiteAClient.get(eq(row))).thenReturn(respFuture);
    when(region1SiteBClient.get(eq(row))).thenReturn(CompletableFuture.completedFuture(resultMap));

    store.get(row, routeKeyChOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<ResultMap>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue().equals(resultMap)));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteB)));
    assertTrue("Should not be stale", (response.isStale()));
    verify(region1SiteAClient, times(1)).get(row);
    verify(region1SiteBClient, times(1)).get(row);
  }

  @Test public void testGetFromRegion1SiteAWithLocalPreferredOtherDC() throws Exception {
    storeRoute =
        new StoreRoute(Region.REGION_2, ReadConsistency.LOCAL_PREFERRED, WriteConsistency.PRIMARY_MANDATORY,
            localPreferredRouter);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    ResultMap resultMap = buildResultMap(rowKey1, cf1, cq1, qv1);
    GetRow row = new GetDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).build();
    CompletableFuture<PipelinedResponse<StoreOperationResponse<ResultMap>>> future = new CompletableFuture<>();
    when(region2SiteAClient.get(eq(row))).thenReturn(CompletableFuture.completedFuture(resultMap));

    store.get(row, routeKeyChOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<ResultMap>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue().equals(resultMap)));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region2SiteA)));
    assertTrue("Should not be stale", (!response.isStale()));
    verify(region1SiteAClient, times(0)).get(row);
    verify(region1SiteBClient, times(0)).get(row);
    verify(region2SiteAClient, times(1)).get(row);
  }
}
