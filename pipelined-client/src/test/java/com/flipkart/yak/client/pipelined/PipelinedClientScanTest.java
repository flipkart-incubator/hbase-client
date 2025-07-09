package com.flipkart.yak.client.pipelined;

import com.flipkart.yak.client.AsyncStoreClientImpl;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.client.pipelined.exceptions.NoSiteAvailableToHandleException;
import com.flipkart.yak.client.pipelined.exceptions.PipelinedStoreException;
import com.flipkart.yak.client.pipelined.models.*;
import com.flipkart.yak.client.pipelined.models.DataCenter;
import com.flipkart.yak.client.pipelined.route.StoreRoute;
import com.flipkart.yak.models.ResultMap;
import com.flipkart.yak.models.ScanData;
import com.flipkart.yak.models.ScanDataBuilder;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static junit.framework.Assert.assertNotNull;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.FilterList;
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

public class PipelinedClientScanTest extends PipelinedClientBaseTest {
  Map<String, ResultMap> resultMaps = new HashMap<>();
  ScanData scanData;

  @Parameterized.Parameters public static Collection parameters() {
    return Arrays.asList(new Object[][] { { true }, { false } });
  }

  public PipelinedClientScanTest(boolean runWithHystrix) {
    this.runWithHystrix = runWithHystrix;
  }

  @Before public void setup() throws Exception {
    resultMaps.put(rowKey1, buildResultMap(rowKey1, cf1, cq1, qv1));
    resultMaps.put(rowKey1 + "-another", buildResultMap(rowKey1 + "-another", cf1, cq1, qv1));

    FilterList filterList = new FilterList();
    filterList.addFilter(new ColumnPrefixFilter(rowKey1.getBytes()));
    scanData = (new ScanDataBuilder(FULL_TABLE_NAME)).withFilterList(filterList).withLimit(1).build();
  }

  @After public void tearDown() throws IOException, InterruptedException {
    store.shutdown();
  }

  @Test public void testScanFromRegion1SiteA()
      throws ExecutionException, InterruptedException, StoreException, IOException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>>> future = new CompletableFuture<>();
    when(region1SiteAClient.scan(scanData)).thenReturn(CompletableFuture.completedFuture(resultMaps));

    store.scan(scanData, routeKeyChOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }

    assertTrue("Should not have null response", (response.getOperationResponse().getValue().equals(resultMaps)));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteA)));
    assertTrue("Should not be stale", (!response.isStale()));
    verify(region1SiteAClient, times(1)).scan(scanData);
  }

  @Test public void testScanFromRegion1SiteASync()
      throws ExecutionException, InterruptedException, TimeoutException, StoreException, IOException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>>> future = new CompletableFuture<>();
    when(region1SiteAClient.scan(scanData)).thenReturn(CompletableFuture.completedFuture(resultMaps));

    PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>> response =
        syncStore.scan(scanData, routeKeyChOptional, Optional.empty(), hystrixSettings);
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }

    assertTrue("Should not have null response", (response.getOperationResponse().getValue().equals(resultMaps)));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteA)));
    assertTrue("Should not be stale", (!response.isStale()));
    verify(region1SiteAClient, times(1)).scan(scanData);
  }

  @Test public void testScanFromRegion2SiteA() throws ExecutionException, InterruptedException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>>> future = new CompletableFuture<>();
    when(region2SiteAClient.scan(scanData)).thenReturn(CompletableFuture.completedFuture(resultMaps));

    store.scan(scanData, routeKeyHydOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }

    assertTrue("Should not have null response", (response.getOperationResponse().getValue().equals(resultMaps)));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region2SiteA)));
    assertTrue("Should not be stale", (!response.isStale()));
    verify(region1SiteAClient, times(0)).scan(scanData);
    verify(region2SiteAClient, times(1)).scan(scanData);
  }

  @Test public void testScanFromRegion1SiteAWithException() throws ExecutionException, InterruptedException {
    String errorMessage = "Failed";
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>>> future = new CompletableFuture<>();
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException(errorMessage));
    when(region1SiteAClient.scan(scanData)).thenReturn(respFuture);

    store.scan(scanData, routeKeyChOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>> response = future.get();

    assertTrue("Should have null response", (response.getOperationResponse().getValue() == null));
    assertTrue("Should throw exception", (response.getOperationResponse().getError() != null));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError() instanceof PipelinedStoreException));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError().getMessage().equals(errorMessage)));
    verify(region1SiteAClient, times(1)).scan(scanData);
  }

  @Test public void testScanFromRegion1SiteAWithExceptionSync()
    throws ExecutionException, InterruptedException, TimeoutException {
    String errorMessage = "Failed";
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>>> future = new CompletableFuture<>();
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException(errorMessage));
    when(region1SiteAClient.scan(scanData)).thenReturn(respFuture);

    PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>> response =
        syncStore.scan(scanData, routeKeyChOptional, Optional.empty(), hystrixSettings);

    assertTrue("Should have null response", (response.getOperationResponse().getValue() == null));
    assertTrue("Should throw exception", (response.getOperationResponse().getError() != null));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError() instanceof PipelinedStoreException));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError().getMessage().equals(errorMessage)));
    verify(region1SiteAClient, times(1)).scan(scanData);
  }

  @Test public void testScanFromRegion1SiteAWithNoSitesException() throws InterruptedException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>>> future = new CompletableFuture<>();
    when(region1SiteAClient.scan(scanData)).thenReturn(CompletableFuture.completedFuture(resultMaps));

    store.scan(scanData, Optional.of("Dummy"), Optional.empty(), hystrixSettings, responseHandler(future));
    try {
      future.get();
      assertTrue("should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof NoSiteAvailableToHandleException);
      assertTrue("Incorrect exception message", (ex.getCause().getMessage().equals(FAILED_TO_ROUTE_MESSAGE)));
    }
    verify(region1SiteAClient, times(0)).scan(scanData);
  }

  @Test public void testScanFromRegion1SiteAWithSomeSitesInitializeFail() throws Exception {
    PowerMockito.whenNew(AsyncStoreClientImpl.class)
        .withArguments(eq(getStoreConfigRegion1()), eq(Optional.of(keyDistributorMap)), Matchers.anyInt(), eq(registry))
        .thenThrow(new StoreException("No sites are accepting any connections"));

    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_PREFERRED, WriteConsistency.PRIMARY_MANDATORY,
            router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    CompletableFuture<PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>>> future = new CompletableFuture<>();
    when(region1SiteBClient.scan(scanData)).thenReturn(CompletableFuture.completedFuture(resultMaps));

    store.scan(scanData, routeKeyChOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }

    assertTrue("Should not have null response", (response.getOperationResponse().getValue().equals(resultMaps)));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteB)));
    assertTrue("Should be stale", (response.isStale()));
    verify(region1SiteAClient, times(0)).scan(scanData);
    verify(region1SiteBClient, times(1)).scan(scanData);
  }

  @Test public void testScanFromRegion1SiteBAsFallback() throws Exception {
    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_PREFERRED, WriteConsistency.PRIMARY_MANDATORY,
            router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    CompletableFuture<PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>>> future = new CompletableFuture<>();
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException("Failed"));
    when(region1SiteAClient.scan(scanData)).thenReturn(respFuture);
    when(region1SiteBClient.scan(scanData)).thenReturn(CompletableFuture.completedFuture(resultMaps));

    store.scan(scanData, routeKeyChOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }

    assertTrue("Should not have null response", (response.getOperationResponse().getValue().equals(resultMaps)));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteB)));
    assertTrue("Should be stale", (response.isStale()));
    verify(region1SiteAClient, times(1)).scan(scanData);
    verify(region1SiteBClient, times(1)).scan(scanData);
  }

  @Test public void testScanFromRegion1SiteBAsDoubleFallback() throws Exception {
    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_PREFERRED, WriteConsistency.PRIMARY_MANDATORY,
            router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    CompletableFuture<PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>>> future = new CompletableFuture<>();
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException("Failed"));
    when(region1SiteAClient.scan(scanData)).thenReturn(respFuture);
    when(region1SiteBClient.scan(scanData)).thenReturn(respFuture);
    when(region2SiteAClient.scan(scanData)).thenReturn(CompletableFuture.completedFuture(resultMaps));

    store.scan(scanData, routeKeyChOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }

    assertTrue("Should not have null response", (response.getOperationResponse().getValue().equals(resultMaps)));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region2SiteA)));
    assertTrue("Should be stale", (response.isStale()));
    verify(region1SiteAClient, times(1)).scan(scanData);
    verify(region1SiteBClient, times(1)).scan(scanData);
    verify(region2SiteAClient, times(1)).scan(scanData);
  }

  @Test public void testScanFromRegion1SiteAWithLocalPreferred() throws Exception {
    storeRoute = new StoreRoute(Region.REGION_1, ReadConsistency.LOCAL_PREFERRED, WriteConsistency.PRIMARY_MANDATORY,
        localPreferredRouter);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    CompletableFuture<PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>>> future = new CompletableFuture<>();
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException("Failed"));
    when(region1SiteAClient.scan(scanData)).thenReturn(respFuture);
    when(region1SiteBClient.scan(scanData)).thenReturn(CompletableFuture.completedFuture(resultMaps));

    store.scan(scanData, routeKeyChOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }

    assertTrue("Should not have null response", (response.getOperationResponse().getValue().equals(resultMaps)));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteB)));
    assertTrue("Should be stale", (response.isStale()));
    verify(region1SiteAClient, times(1)).scan(scanData);
    verify(region1SiteBClient, times(1)).scan(scanData);
  }

  @Test public void testScanFromRegion1SiteAWithLocalPreferredOtherDC() throws Exception {
    storeRoute =
        new StoreRoute(Region.REGION_2, ReadConsistency.LOCAL_PREFERRED, WriteConsistency.PRIMARY_MANDATORY,
            localPreferredRouter);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    CompletableFuture<PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>>> future = new CompletableFuture<>();
    when(region2SiteAClient.scan(scanData)).thenReturn(CompletableFuture.completedFuture(resultMaps));

    store.scan(scanData, routeKeyChOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }

    assertTrue("Should not have null response", (response.getOperationResponse().getValue().equals(resultMaps)));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region2SiteA)));
    assertTrue("Should not be stale", (!response.isStale()));
    verify(region1SiteAClient, times(0)).scan(scanData);
    verify(region1SiteBClient, times(0)).scan(scanData);
    verify(region2SiteAClient, times(1)).scan(scanData);
  }
}
