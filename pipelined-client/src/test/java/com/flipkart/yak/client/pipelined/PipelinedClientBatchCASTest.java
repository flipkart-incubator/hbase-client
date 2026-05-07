package com.flipkart.yak.client.pipelined;

import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.client.pipelined.exceptions.NoSiteAvailableToHandleException;
import com.flipkart.yak.client.pipelined.exceptions.PipelinedStoreException;
import com.flipkart.yak.client.pipelined.models.*;
import com.flipkart.yak.client.pipelined.route.StoreRoute;
import com.flipkart.yak.models.CheckAndStoreData;
import com.flipkart.yak.models.StoreDataBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.CompareOperator;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.powermock.api.mockito.PowerMockito;

public class PipelinedClientBatchCASTest extends PipelinedClientBaseTest {

  private List<CheckAndStoreData> dataList;

  @Parameterized.Parameters public static Collection parameters() {
    return Arrays.asList(new Object[][] { { true, true }, { false, false }, { true, false }, { false, true } });
  }

  public PipelinedClientBatchCASTest(boolean runWithHystrix, boolean runWithIntent) {
    this.runWithHystrix = runWithHystrix;
    this.runWithIntent = runWithIntent;
  }

  @Before public void setup() throws Exception {
    CheckAndStoreData data1 = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes())
        .addColumn(cf1, cq1, "newVal1".getBytes())
        .buildWithCheckAndVerifyColumn(cf1, cq1, qv1.getBytes(), CompareOperator.EQUAL);
    CheckAndStoreData data2 = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey2.getBytes())
        .addColumn(cf2, cq2, "newVal2".getBytes())
        .buildWithCheckAndVerifyColumn(cf2, cq2, qv2.getBytes(), CompareOperator.EQUAL);
    dataList = Arrays.asList(data1, data2);
  }

  @After public void tearDown() throws IOException, InterruptedException {
    store.shutdown();
  }

  @Test public void testBatchCASAllSucceed() throws ExecutionException, InterruptedException {
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<Boolean>>>> future = new CompletableFuture<>();
    CompletableFuture<Boolean> successFuture = CompletableFuture.completedFuture(Boolean.TRUE);
    when(region1SiteAClient.batchCheckAndPutForPipelined(eq(dataList)))
        .thenReturn(Arrays.asList(successFuture, successFuture));

    store.checkAndPut(dataList, routeMetaChOptional, intentData, hystrixSettings, responsesHandler(future));

    PipelinedResponse<List<StoreOperationResponse<Boolean>>> response = future.get();
    response.getOperationResponse().stream().forEachOrdered(resp -> {
      assertTrue("CAS should have succeeded", Boolean.TRUE.equals(resp.getValue()));
      assertTrue("Should not throw exception", resp.getError() == null);
      assertTrue("Should match region1 siteA", resp.getSite().equals(Region1SiteA));
      assertTrue("Should not be stale", !response.isStale());
    });
    verify(region1SiteAClient, times(1)).batchCheckAndPutForPipelined(dataList);
    verify(region2SiteAClient, times((runWithIntent) ? 1 : 0)).put(intentStoreData);
  }

  @Test public void testBatchCASAllSucceedSync()
      throws ExecutionException, InterruptedException, TimeoutException {
    CompletableFuture<Boolean> successFuture = CompletableFuture.completedFuture(Boolean.TRUE);
    when(region1SiteAClient.batchCheckAndPutForPipelined(eq(dataList)))
        .thenReturn(Arrays.asList(successFuture, successFuture));

    PipelinedResponse<List<StoreOperationResponse<Boolean>>> response =
        syncStore.checkAndPut(dataList, routeMetaChOptional, intentData, hystrixSettings);

    response.getOperationResponse().stream().forEachOrdered(resp -> {
      assertTrue("CAS should have succeeded", Boolean.TRUE.equals(resp.getValue()));
      assertTrue("Should not throw exception", resp.getError() == null);
      assertTrue("Should match region1 siteA", resp.getSite().equals(Region1SiteA));
    });
    verify(region1SiteAClient, times(1)).batchCheckAndPutForPipelined(dataList);
  }

  @Test public void testBatchCASPartialFalse() throws ExecutionException, InterruptedException {
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<Boolean>>>> future = new CompletableFuture<>();
    CompletableFuture<Boolean> successFuture = CompletableFuture.completedFuture(Boolean.TRUE);
    CompletableFuture<Boolean> failFuture = CompletableFuture.completedFuture(Boolean.FALSE);
    when(region1SiteAClient.batchCheckAndPutForPipelined(eq(dataList)))
        .thenReturn(Arrays.asList(successFuture, failFuture));

    store.checkAndPut(dataList, routeMetaChOptional, intentData, hystrixSettings, responsesHandler(future));

    PipelinedResponse<List<StoreOperationResponse<Boolean>>> response = future.get();
    List<StoreOperationResponse<Boolean>> results = response.getOperationResponse();
    assertTrue("First CAS should succeed", Boolean.TRUE.equals(results.get(0).getValue()));
    assertTrue("Second CAS should return false", Boolean.FALSE.equals(results.get(1).getValue()));
    assertTrue("Should not throw exception on row 0", results.get(0).getError() == null);
    assertTrue("Should not throw exception on row 1", results.get(1).getError() == null);
    verify(region1SiteAClient, times(1)).batchCheckAndPutForPipelined(dataList);
  }

  @Test public void testBatchCASAllFalse() throws ExecutionException, InterruptedException {
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<Boolean>>>> future = new CompletableFuture<>();
    CompletableFuture<Boolean> failFuture = CompletableFuture.completedFuture(Boolean.FALSE);
    when(region1SiteAClient.batchCheckAndPutForPipelined(eq(dataList)))
        .thenReturn(Arrays.asList(failFuture, failFuture));

    store.checkAndPut(dataList, routeMetaChOptional, intentData, hystrixSettings, responsesHandler(future));

    PipelinedResponse<List<StoreOperationResponse<Boolean>>> response = future.get();
    response.getOperationResponse().stream().forEachOrdered(resp -> {
      assertTrue("CAS should return false", Boolean.FALSE.equals(resp.getValue()));
      assertTrue("Should not throw exception", resp.getError() == null);
    });
    verify(region1SiteAClient, times(1)).batchCheckAndPutForPipelined(dataList);
  }

  @Test public void testBatchCASWithInfraException() throws ExecutionException, InterruptedException {
    String errorMessage = "HBase infra failure";
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<Boolean>>>> future = new CompletableFuture<>();
    CompletableFuture<Boolean> errorFuture = new CompletableFuture<>();
    errorFuture.completeExceptionally(new PipelinedStoreException(errorMessage));
    when(region1SiteAClient.batchCheckAndPutForPipelined(eq(dataList)))
        .thenReturn(Arrays.asList(errorFuture, errorFuture));

    store.checkAndPut(dataList, routeMetaChOptional, intentData, hystrixSettings, responsesHandler(future));

    PipelinedResponse<List<StoreOperationResponse<Boolean>>> response = future.get();
    response.getOperationResponse().stream().forEachOrdered(resp -> {
      assertTrue("Should have null value on error", resp.getValue() == null);
      assertTrue("Should have error", resp.getError() != null);
      assertTrue("Should be PipelinedStoreException", resp.getError() instanceof PipelinedStoreException);
      assertTrue("Error message should match", resp.getError().getMessage().equals(errorMessage));
    });
    verify(region1SiteAClient, times(1)).batchCheckAndPutForPipelined(dataList);
  }

  @Test public void testBatchCASEmptyList() throws ExecutionException, InterruptedException {
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<Boolean>>>> future = new CompletableFuture<>();
    when(region1SiteAClient.batchCheckAndPutForPipelined(eq(Collections.emptyList())))
        .thenReturn(Collections.emptyList());

    store.checkAndPut(Collections.emptyList(), routeMetaChOptional, intentData, hystrixSettings,
        responsesHandler(future));

    PipelinedResponse<List<StoreOperationResponse<Boolean>>> response = future.get();
    assertTrue("Response list should be empty", response.getOperationResponse().isEmpty());
    verify(region1SiteAClient, times(1)).batchCheckAndPutForPipelined(Collections.emptyList());
  }

  @Test public void testBatchCASNoSiteAvailable() throws InterruptedException {
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<Boolean>>>> future = new CompletableFuture<>();
    store.checkAndPut(dataList, Optional.of("UNKNOWN_ROUTE"), intentData, hystrixSettings, responsesHandler(future));

    try {
      future.get();
      assertTrue("Should have thrown NoSiteAvailableToHandleException", false);
    } catch (ExecutionException ex) {
      assertNotNull("Cause should not be null", ex.getCause());
      assertTrue("Should be NoSiteAvailableToHandleException",
          ex.getCause() instanceof NoSiteAvailableToHandleException);
      assertTrue("Message should match", ex.getCause().getMessage().equals(FAILED_TO_ROUTE_MESSAGE));
    }
    verify(region1SiteAClient, times(0)).batchCheckAndPutForPipelined(dataList);
  }

  @Test public void testBatchCASFallbackToSiteB() throws Exception {
    PowerMockito.whenNew(com.flipkart.yak.client.AsyncStoreClientImpl.class)
        .withArguments(eq(getStoreConfigRegion1()), eq(Optional.of(keyDistributorMap)),
            org.mockito.Matchers.anyInt(), eq(registry))
        .thenThrow(new StoreException("Region1 SiteA unavailable"));
    storeRoute = new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_MANDATORY,
        WriteConsistency.PRIMARY_PREFERRED, router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);
    syncStore = new SyncYakPipelinedStoreImpl(store);

    CompletableFuture<Boolean> successFuture = CompletableFuture.completedFuture(Boolean.TRUE);
    when(region1SiteBClient.batchCheckAndPutForPipelined(eq(dataList)))
        .thenReturn(Arrays.asList(successFuture, successFuture));

    PipelinedResponse<List<StoreOperationResponse<Boolean>>> response =
        syncStore.checkAndPut(dataList, routeMetaChOptional, intentData, hystrixSettings);

    response.getOperationResponse().stream().forEachOrdered(resp -> {
      assertTrue("CAS should succeed on fallback site", Boolean.TRUE.equals(resp.getValue()));
      assertTrue("Should not have error", resp.getError() == null);
      assertTrue("Should match Region1 SiteB", resp.getSite().equals(Region1SiteB));
      assertTrue("Should be stale since not primary site", response.isStale());
    });
    verify(region1SiteAClient, times(0)).batchCheckAndPutForPipelined(dataList);
    verify(region1SiteBClient, times(1)).batchCheckAndPutForPipelined(dataList);
  }
}
