package com.flipkart.yak.client.pipelined;

import com.flipkart.yak.client.AsyncStoreClientImpl;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.client.pipelined.exceptions.NoSiteAvailableToHandleException;
import com.flipkart.yak.client.pipelined.exceptions.PipelinedStoreException;
import com.flipkart.yak.client.pipelined.models.*;
import com.flipkart.yak.client.pipelined.models.DataCenter;
import com.flipkart.yak.client.pipelined.route.StoreRoute;
import com.flipkart.yak.models.Cell;
import com.flipkart.yak.models.ColumnsMap;
import com.flipkart.yak.models.GetDataBuilder;
import com.flipkart.yak.models.GetRow;
import com.flipkart.yak.models.ResultMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
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

public class PipelinedClientBatchGetTest extends PipelinedClientBaseTest {

  List<ResultMap> resultsMap;
  List<GetRow> rows;

  @Parameterized.Parameters public static Collection parameters() {
    return Arrays.asList(new Object[][] { { true }, { false } });
  }

  public PipelinedClientBatchGetTest(boolean runWithHystrix) {
    this.runWithHystrix = runWithHystrix;
  }

  @Before public void setup() throws Exception {
    resultsMap = buildResultMap(Arrays.asList(cf1, cf1), Arrays.asList(cq1, cq2), Arrays.asList(qv1, qv2));
    GetRow row1 = new GetDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).build();
    GetRow row2 = new GetDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).build();
    rows = Arrays.asList(row1, row2);
  }

  private List<ResultMap> buildResultMap(List<String> cfs, List<String> cqs, List<String> qvs) {
    List<ResultMap> resultMaps = new ArrayList<>();
    for (int index = 0; index < cfs.size(); index += 1) {
      ResultMap resultMap = new ResultMap();
      ColumnsMap columnsMap = new ColumnsMap();
      columnsMap.put(cqs.get(index), new Cell(qvs.get(index).getBytes()));
      resultMap.put(cfs.get(index), columnsMap);
      resultMaps.add(resultMap);
    }
    return resultMaps;
  }

  @After public void tearDown() throws IOException, InterruptedException {
    store.shutdown();
  }

  @Test public void testBatchGetFromRegion1SiteA()
      throws ExecutionException, InterruptedException, StoreException, IOException {
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<ResultMap>>>> future = new CompletableFuture<>();
    when(region1SiteAClient.get(eq(rows))).thenReturn(
        resultsMap.stream().map(resultMap -> CompletableFuture.completedFuture(resultMap))
            .collect(Collectors.toList()));

    store.get(rows, routeKeyChOptional, Optional.empty(), hystrixSettings, responsesHandler(future));
    PipelinedResponse<List<StoreOperationResponse<ResultMap>>> response = future.get();
    response.getOperationResponse().stream().forEachOrdered(resp -> {
      assertTrue("Should have null response", (resultsMap.contains(resp.getValue())));
      assertTrue("Should not throw exception", (resp.getError() == null));
      assertTrue("Should match with region1 siteA", (resp.getSite().equals(Region1SiteA)));
      assertTrue("Should not be stale", (!response.isStale()));
    });
    verify(region1SiteAClient, times(1)).get(rows);
  }

  @Test public void testBatchGetFromRegion1SiteASync()
      throws ExecutionException, InterruptedException, TimeoutException {
    when(region1SiteAClient.get(eq(rows))).thenReturn(
        resultsMap.stream().map(resultMap -> CompletableFuture.completedFuture(resultMap))
            .collect(Collectors.toList()));

    PipelinedResponse<List<StoreOperationResponse<ResultMap>>> response =
        syncStore.get(rows, routeKeyChOptional, Optional.empty(), hystrixSettings);
    response.getOperationResponse().stream().forEachOrdered(resp -> {
      assertTrue("Should have null response", (resultsMap.contains(resp.getValue())));
      assertTrue("Should not throw exception", (resp.getError() == null));
      assertTrue("Should match with region1 siteA", (resp.getSite().equals(Region1SiteA)));
      assertTrue("Should not be stale", (!response.isStale()));
    });
    verify(region1SiteAClient, times(1)).get(rows);
  }

  @Test public void testBatchGetFromRegion2SiteA() throws ExecutionException, InterruptedException {
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<ResultMap>>>> future = new CompletableFuture<>();
    when(region2SiteAClient.get(eq(rows))).thenReturn(
        resultsMap.stream().map(resultMap -> CompletableFuture.completedFuture(resultMap))
            .collect(Collectors.toList()));

    store.get(rows, routeKeyHydOptional, Optional.empty(), hystrixSettings, responsesHandler(future));
    PipelinedResponse<List<StoreOperationResponse<ResultMap>>> response = future.get();
    response.getOperationResponse().stream().forEachOrdered(resp -> {
      assertTrue("Should not have response", (resultsMap.contains(resp.getValue())));
      assertTrue("Should not throw exception", (resp.getError() == null));
      assertTrue("Should match with region1 siteA", (resp.getSite().equals(Region2SiteA)));
      assertTrue("Should not be stale", (!response.isStale()));
    });
    verify(region1SiteAClient, times(0)).get(rows);
    verify(region2SiteAClient, times(1)).get(rows);
  }

  @Test public void testBatchGetFromRegion1SiteAWithException() throws ExecutionException, InterruptedException {
    String errorMessage = "Failed";
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<ResultMap>>>> future = new CompletableFuture<>();
    CompletableFuture<ResultMap> respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException(errorMessage));

    when(region1SiteAClient.get(eq(rows)))
        .thenReturn(resultsMap.stream().map(resultMap -> respFuture).collect(Collectors.toList()));
    store.get(rows, routeKeyChOptional, Optional.empty(), hystrixSettings, responsesHandler(future));
    PipelinedResponse<List<StoreOperationResponse<ResultMap>>> response = future.get();
    response.getOperationResponse().stream().forEachOrdered(resp -> {
      assertTrue("Should have null response", (resp.getValue() == null));
      assertTrue("Should not throw exception", (resp.getError() != null));
      assertTrue("Should throw exception", (resp.getError() instanceof PipelinedStoreException));
      assertTrue("Should throw exception", (resp.getError().getMessage().equals(errorMessage)));
    });
    verify(region1SiteAClient, times(1)).get(rows);
  }

  @Test public void testBatchGetFromRegion1SiteAWithExceptionSync()
          throws ExecutionException, InterruptedException, TimeoutException {
    String errorMessage = "Failed";
    CompletableFuture<ResultMap> respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException(errorMessage));

    when(region1SiteAClient.get(eq(rows)))
        .thenReturn(resultsMap.stream().map(resultMap -> respFuture).collect(Collectors.toList()));
    PipelinedResponse<List<StoreOperationResponse<ResultMap>>> response =
        syncStore.get(rows, routeKeyChOptional, Optional.empty(), hystrixSettings);
    response.getOperationResponse().stream().forEachOrdered(resp -> {
      assertTrue("Should have null response", (resp.getValue() == null));
      assertTrue("Should not throw exception", (resp.getError() != null));
      assertTrue("Should throw exception", (resp.getError() instanceof PipelinedStoreException));
      assertTrue("Should throw exception", (resp.getError().getMessage().equals(errorMessage)));
    });
    verify(region1SiteAClient, times(1)).get(rows);
  }

  @Test public void testBatchGetFromRegion1SiteAWithNoSitesException() throws InterruptedException {
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<ResultMap>>>> future = new CompletableFuture<>();
    when(region1SiteAClient.get(eq(rows))).thenReturn(
        resultsMap.stream().map(resultMap -> CompletableFuture.completedFuture(resultMap))
            .collect(Collectors.toList()));

    store.get(rows, Optional.of("Dummy"), Optional.empty(), hystrixSettings, responsesHandler(future));
    PipelinedResponse<List<StoreOperationResponse<ResultMap>>> response = null;
    try {
      future.get();
      assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertTrue("Should not throw exception", (ex.getCause() != null));
      assertTrue("Should throw exception", (ex.getCause() instanceof NoSiteAvailableToHandleException));
      assertTrue("Incorrect exception message", (ex.getCause().getMessage().equals(FAILED_TO_ROUTE_MESSAGE)));
    }
    verify(region1SiteAClient, times(0)).get(rows);
  }

  @Test public void testBatchGetFromRegion1SiteAWithSomeSitesInitializeFail() throws Exception {
    PowerMockito.whenNew(AsyncStoreClientImpl.class)
        .withArguments(eq(getStoreConfigRegion1()), eq(Optional.of(keyDistributorMap)), Matchers.anyInt(), eq(registry))
        .thenThrow(new StoreException("No sites are accepting any connections"));

    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_PREFERRED, WriteConsistency.PRIMARY_MANDATORY,
            router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<ResultMap>>>> future = new CompletableFuture<>();
    when(region1SiteBClient.get(eq(rows))).thenReturn(
        resultsMap.stream().map(resultMap -> CompletableFuture.completedFuture(resultMap))
            .collect(Collectors.toList()));

    store.get(rows, routeKeyChOptional, Optional.empty(), hystrixSettings, responsesHandler(future));
    PipelinedResponse<List<StoreOperationResponse<ResultMap>>> response = future.get();
    response.getOperationResponse().stream().forEachOrdered(resp -> {
      assertTrue("Should have null response", (resultsMap.contains(resp.getValue())));
      assertTrue("Should not throw exception", (resp.getError() == null));
      assertTrue("Should match with region1 siteA", (resp.getSite().equals(Region1SiteB)));
      assertTrue("Should be stale", (response.isStale()));
    });
    verify(region1SiteAClient, times(0)).get(rows);
    verify(region1SiteBClient, times(1)).get(rows);

  }

  @Test public void testBatchGetFromRegion1SiteBAsFallback() throws Exception {
    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_PREFERRED, WriteConsistency.PRIMARY_MANDATORY,
            router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<ResultMap>>>> future = new CompletableFuture<>();
    CompletableFuture<ResultMap> respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException("Failed"));
    when(region1SiteAClient.get(eq(rows)))
        .thenReturn(resultsMap.stream().map(s -> respFuture).collect(Collectors.toList()));
    when(region1SiteBClient.get(eq(rows))).thenReturn(
        resultsMap.stream().map(resultMap -> CompletableFuture.completedFuture(resultMap))
            .collect(Collectors.toList()));

    store.get(rows, routeKeyChOptional, Optional.empty(), hystrixSettings, responsesHandler(future));
    PipelinedResponse<List<StoreOperationResponse<ResultMap>>> response = future.get();
    response.getOperationResponse().stream().forEachOrdered(resp -> {
      assertTrue("Should have null response", (resultsMap.contains(resp.getValue())));
      assertTrue("Should not throw exception", (resp.getError() == null));
      assertTrue("Should match with region1 siteA", (resp.getSite().equals(Region1SiteB)));
      assertTrue("Should be stale", (response.isStale()));
    });
    verify(region1SiteAClient, times(1)).get(rows);
    verify(region1SiteBClient, times(1)).get(rows);
  }

  @Test public void testBatchGetFromRegion1SiteBAsDoubleFallback() throws Exception {
    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_PREFERRED, WriteConsistency.PRIMARY_MANDATORY,
            router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<ResultMap>>>> future = new CompletableFuture<>();
    CompletableFuture<ResultMap> respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException("Failed"));
    when(region1SiteAClient.get(eq(rows)))
        .thenReturn(resultsMap.stream().map(s -> respFuture).collect(Collectors.toList()));
    when(region1SiteBClient.get(eq(rows)))
        .thenReturn(resultsMap.stream().map(s -> respFuture).collect(Collectors.toList()));
    when(region2SiteAClient.get(eq(rows))).thenReturn(
        resultsMap.stream().map(resultMap -> CompletableFuture.completedFuture(resultMap))
            .collect(Collectors.toList()));

    store.get(rows, routeKeyChOptional, Optional.empty(), hystrixSettings, responsesHandler(future));
    PipelinedResponse<List<StoreOperationResponse<ResultMap>>> response = future.get();
    response.getOperationResponse().stream().forEachOrdered(resp -> {
      assertTrue("Should have null response", (resultsMap.contains(resp.getValue())));
      assertTrue("Should not throw exception", (resp.getError() == null));
      assertTrue("Should match with region1 siteA", (resp.getSite().equals(Region2SiteA)));
      assertTrue("Should be stale", (response.isStale()));
    });
    verify(region1SiteAClient, times(1)).get(rows);
    verify(region1SiteBClient, times(1)).get(rows);
    verify(region2SiteAClient, times(1)).get(rows);
  }
}
