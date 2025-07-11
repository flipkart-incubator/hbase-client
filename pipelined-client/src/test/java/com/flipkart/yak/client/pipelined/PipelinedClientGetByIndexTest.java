package com.flipkart.yak.client.pipelined;

import com.flipkart.yak.client.AsyncStoreClientImpl;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.client.pipelined.exceptions.NoSiteAvailableToHandleException;
import com.flipkart.yak.client.pipelined.exceptions.PipelinedStoreException;
import com.flipkart.yak.client.pipelined.models.*;
import com.flipkart.yak.client.pipelined.route.StoreRoute;
import com.flipkart.yak.models.Cell;
import com.flipkart.yak.models.ColumnsMap;
import com.flipkart.yak.models.GetByIndexBuilder;
import com.flipkart.yak.models.GetCellByIndex;
import com.flipkart.yak.models.GetColumnsMapByIndex;
import com.flipkart.yak.models.SimpleIndexAttributes;
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

public class PipelinedClientGetByIndexTest extends PipelinedClientBaseTest {

  List<ColumnsMap> columns;
  GetColumnsMapByIndex columnsMapByIndex;
  List<Cell> cells;
  GetCellByIndex cellByIndex;

  @Parameterized.Parameters public static Collection parameters() {
    return Arrays.asList(new Object[][] { { true }, { false } });
  }

  public PipelinedClientGetByIndexTest(boolean runWithHystrix) {
    this.runWithHystrix = runWithHystrix;
  }

  @Before public void setup() throws Exception {
    ColumnsMap map1 = new ColumnsMap();
    map1.put(cq1, new Cell(qv1.getBytes()));
    map1.put(indexKey1, new Cell(rowKey1.getBytes()));
    ColumnsMap map2 = new ColumnsMap();
    map2.put(cq2, new Cell(qv2.getBytes()));
    map2.put(indexKey2, new Cell(rowKey2.getBytes()));
    columns = Arrays.asList(map1, map2);
    columnsMapByIndex =
        new GetByIndexBuilder(FULL_TABLE_NAME, cf1).withIndexKey(indexKey1, new SimpleIndexAttributes()).build();

    cells = Arrays.asList(new Cell(qv1.getBytes()), new Cell(qv2.getBytes()));
    cellByIndex = new GetByIndexBuilder(FULL_TABLE_NAME, cf1).withIndexKey(indexKey1, new SimpleIndexAttributes())
        .buildForCloumn(cq1);
  }

  @After public void tearDown() throws IOException, InterruptedException {
    store.shutdown();
  }

  @Test public void testGetByIndexFromRegion1SiteA()
      throws ExecutionException, InterruptedException, StoreException, IOException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>>> future = new CompletableFuture<>();
    when(region1SiteAClient.getByIndex(eq(columnsMapByIndex))).thenReturn(CompletableFuture.completedFuture(columns));
    store.getByIndex(columnsMapByIndex, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue().equals(columns)));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteA)));
    assertTrue("Should not be stale", (!response.isStale()));
    verify(region1SiteAClient, times(1)).getByIndex(columnsMapByIndex);

    CompletableFuture<PipelinedResponse<StoreOperationResponse<List<Cell>>>> futureCell = new CompletableFuture<>();
    when(region1SiteAClient.getByIndex(eq(cellByIndex))).thenReturn(CompletableFuture.completedFuture(cells));
    store.getByIndex(cellByIndex, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(futureCell));
    PipelinedResponse<StoreOperationResponse<List<Cell>>> responseCell = futureCell.get();
    if (responseCell.getOperationResponse().getError() != null) {
      throw new ExecutionException(responseCell.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (responseCell.getOperationResponse().getValue().equals(cells)));
    assertTrue("Should not throw exception", (responseCell.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (responseCell.getOperationResponse().getSite().equals(Region1SiteA)));
    assertTrue("Should not be stale", (!responseCell.isStale()));
    verify(region1SiteAClient, times(1)).getByIndex(cellByIndex);
  }

  @Test public void testGetByIndexFromRegion1SiteASync()
      throws ExecutionException, InterruptedException, TimeoutException {
    when(region1SiteAClient.getByIndex(eq(columnsMapByIndex))).thenReturn(CompletableFuture.completedFuture(columns));
    PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>> response =
        syncStore.getByIndex(columnsMapByIndex, routeMetaChOptional, Optional.empty(), hystrixSettings);
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue().equals(columns)));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteA)));
    assertTrue("Should not be stale", (!response.isStale()));
    verify(region1SiteAClient, times(1)).getByIndex(columnsMapByIndex);

    when(region1SiteAClient.getByIndex(eq(cellByIndex))).thenReturn(CompletableFuture.completedFuture(cells));
    PipelinedResponse<StoreOperationResponse<List<Cell>>> responseCell =
        syncStore.getByIndex(cellByIndex, routeMetaChOptional, Optional.empty(), hystrixSettings);
    if (responseCell.getOperationResponse().getError() != null) {
      throw new ExecutionException(responseCell.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (responseCell.getOperationResponse().getValue().equals(cells)));
    assertTrue("Should not throw exception", (responseCell.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (responseCell.getOperationResponse().getSite().equals(Region1SiteA)));
    assertTrue("Should not be stale", (!responseCell.isStale()));
    verify(region1SiteAClient, times(1)).getByIndex(cellByIndex);
  }

  @Test public void testGetByIndexFromRegion2SiteA() throws ExecutionException, InterruptedException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>>> future = new CompletableFuture<>();
    when(region2SiteAClient.getByIndex(eq(columnsMapByIndex))).thenReturn(CompletableFuture.completedFuture(columns));
    store
        .getByIndex(columnsMapByIndex, routeMetaHydOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue().equals(columns)));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region2 siteA", (response.getOperationResponse().getSite().equals(Region2SiteA)));
    assertTrue("Should not be stale", (!response.isStale()));
    verify(region1SiteAClient, times(0)).getByIndex(columnsMapByIndex);
    verify(region2SiteAClient, times(1)).getByIndex(columnsMapByIndex);

    CompletableFuture<PipelinedResponse<StoreOperationResponse<List<Cell>>>> futureCell = new CompletableFuture<>();
    when(region2SiteAClient.getByIndex(eq(cellByIndex))).thenReturn(CompletableFuture.completedFuture(cells));
    store.getByIndex(cellByIndex, routeMetaHydOptional, Optional.empty(), hystrixSettings, responseHandler(futureCell));
    PipelinedResponse<StoreOperationResponse<List<Cell>>> responseCell = futureCell.get();
    if (responseCell.getOperationResponse().getError() != null) {
      throw new ExecutionException(responseCell.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (responseCell.getOperationResponse().getValue().equals(cells)));
    assertTrue("Should not throw exception", (responseCell.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA",
        (responseCell.getOperationResponse().getSite().equals(Region2SiteA)));
    assertTrue("Should not be stale", (!responseCell.isStale()));
    verify(region1SiteAClient, times(0)).getByIndex(cellByIndex);
    verify(region2SiteAClient, times(1)).getByIndex(cellByIndex);
  }

  @Test public void testGetByIndexFromRegion1SiteAWithException() throws ExecutionException, InterruptedException {
    String errorMessage = "Failed";
    CompletableFuture<PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>>> future = new CompletableFuture<>();
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException(errorMessage));
    when(region1SiteAClient.getByIndex(eq(columnsMapByIndex))).thenReturn(respFuture);
    store.getByIndex(columnsMapByIndex, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>> response = future.get();
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == null));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() != null));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError() instanceof PipelinedStoreException));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError().getMessage().equals(errorMessage)));
    verify(region1SiteAClient, times(1)).getByIndex(columnsMapByIndex);

    CompletableFuture<PipelinedResponse<StoreOperationResponse<List<Cell>>>> futureCell = new CompletableFuture<>();
    when(region1SiteAClient.getByIndex(eq(cellByIndex))).thenReturn(respFuture);
    store.getByIndex(cellByIndex, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(futureCell));
    PipelinedResponse<StoreOperationResponse<List<Cell>>> responseCell = futureCell.get();
    assertTrue("Should have null response", (responseCell.getOperationResponse().getValue() == null));
    assertTrue("Should not throw exception", (responseCell.getOperationResponse().getError() != null));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError() instanceof PipelinedStoreException));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError().getMessage().equals(errorMessage)));
    verify(region1SiteAClient, times(1)).getByIndex(cellByIndex);
  }

  @Test public void testGetByIndexFromRegion1SiteAWithExceptionSync()
          throws ExecutionException, InterruptedException, TimeoutException {
    String errorMessage = "Failed";
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException(errorMessage));
    when(region1SiteAClient.getByIndex(eq(columnsMapByIndex))).thenReturn(respFuture);
    PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>> response =
        syncStore.getByIndex(columnsMapByIndex, routeMetaChOptional, Optional.empty(), hystrixSettings);
    assertTrue("Should have null response", (response.getOperationResponse().getValue() == null));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() != null));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError() instanceof PipelinedStoreException));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError().getMessage().equals(errorMessage)));
    verify(region1SiteAClient, times(1)).getByIndex(columnsMapByIndex);

    when(region1SiteAClient.getByIndex(eq(cellByIndex))).thenReturn(respFuture);
    PipelinedResponse<StoreOperationResponse<List<Cell>>> responseCell =
        syncStore.getByIndex(cellByIndex, routeMetaChOptional, Optional.empty(), hystrixSettings);
    assertTrue("Should have null response", (responseCell.getOperationResponse().getValue() == null));
    assertTrue("Should not throw exception", (responseCell.getOperationResponse().getError() != null));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError() instanceof PipelinedStoreException));
    assertTrue("Should throw exception",
        (response.getOperationResponse().getError().getMessage().equals(errorMessage)));
    verify(region1SiteAClient, times(1)).getByIndex(cellByIndex);
  }

  @Test public void testGetByIndexFromRegion1SiteAWithNoSitesException() throws InterruptedException {
    CompletableFuture<PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>>> future = new CompletableFuture<>();
    when(region1SiteAClient.getByIndex(eq(columnsMapByIndex))).thenReturn(CompletableFuture.completedFuture(columns));
    store.getByIndex(columnsMapByIndex, Optional.of("Dummy"), Optional.empty(), hystrixSettings,
        responseHandler(future));
    try {
      future.get();
      assertTrue("should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof NoSiteAvailableToHandleException);
      assertTrue("Incorrect exception message", (ex.getCause().getMessage().equals(FAILED_TO_ROUTE_MESSAGE)));
    }
    verify(region1SiteAClient, times(0)).getByIndex(columnsMapByIndex);

    CompletableFuture<PipelinedResponse<StoreOperationResponse<List<Cell>>>> futureCell = new CompletableFuture<>();
    when(region1SiteAClient.getByIndex(eq(cellByIndex))).thenReturn(CompletableFuture.completedFuture(cells));
    store.getByIndex(cellByIndex, Optional.of("Dummy"), Optional.empty(), hystrixSettings, responseHandler(futureCell));

    try {
      futureCell.get();
      assertTrue("should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof NoSiteAvailableToHandleException);
      assertTrue("Incorrect exception message", (ex.getCause().getMessage().equals(FAILED_TO_ROUTE_MESSAGE)));
    }
    verify(region1SiteAClient, times(0)).getByIndex(cellByIndex);
  }

  @Test public void testGetByIndexFromRegion1SiteAWithSomeSitesInitializeFail() throws Exception {
    PowerMockito.whenNew(AsyncStoreClientImpl.class)
        .withArguments(eq(getStoreConfigRegion1()), eq(Optional.of(keyDistributorMap)), Matchers.anyInt(), eq(registry))
        .thenThrow(new StoreException("No sites are accepting any connections"));

    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_PREFERRED, WriteConsistency.PRIMARY_MANDATORY,
            router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);
    CompletableFuture<PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>>> future = new CompletableFuture<>();
    when(region1SiteBClient.getByIndex(eq(columnsMapByIndex))).thenReturn(CompletableFuture.completedFuture(columns));
    store.getByIndex(columnsMapByIndex, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue().equals(columns)));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteB)));
    assertTrue("Should be stale", (response.isStale()));
    verify(region1SiteAClient, times(0)).getByIndex(columnsMapByIndex);
    verify(region1SiteBClient, times(1)).getByIndex(columnsMapByIndex);

    CompletableFuture<PipelinedResponse<StoreOperationResponse<List<Cell>>>> futureCell = new CompletableFuture<>();
    when(region1SiteBClient.getByIndex(eq(cellByIndex))).thenReturn(CompletableFuture.completedFuture(cells));
    store.getByIndex(cellByIndex, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(futureCell));
    PipelinedResponse<StoreOperationResponse<List<Cell>>> responseCell = futureCell.get();
    if (responseCell.getOperationResponse().getError() != null) {
      throw new ExecutionException(responseCell.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (responseCell.getOperationResponse().getValue().equals(cells)));
    assertTrue("Should not throw exception", (responseCell.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (responseCell.getOperationResponse().getSite().equals(Region1SiteB)));
    assertTrue("Should be stale", (responseCell.isStale()));
    verify(region1SiteAClient, times(0)).getByIndex(cellByIndex);
    verify(region1SiteBClient, times(1)).getByIndex(cellByIndex);
  }

  @Test public void testGetByIndexFromRegion1SiteBAsFallback() throws Exception {
    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_PREFERRED, WriteConsistency.PRIMARY_MANDATORY,
            router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    String errorMessage = "Failed";
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException(errorMessage));

    CompletableFuture<PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>>> future = new CompletableFuture<>();
    when(region1SiteAClient.getByIndex(eq(columnsMapByIndex))).thenReturn(respFuture);
    when(region1SiteBClient.getByIndex(eq(columnsMapByIndex))).thenReturn(CompletableFuture.completedFuture(columns));
    store.getByIndex(columnsMapByIndex, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue().equals(columns)));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteA", (response.getOperationResponse().getSite().equals(Region1SiteB)));
    assertTrue("Should be stale", (response.isStale()));
    verify(region1SiteAClient, times(1)).getByIndex(columnsMapByIndex);
    verify(region1SiteBClient, times(1)).getByIndex(columnsMapByIndex);

    CompletableFuture<PipelinedResponse<StoreOperationResponse<List<Cell>>>> futureCell = new CompletableFuture<>();
    when(region1SiteAClient.getByIndex(eq(cellByIndex))).thenReturn(respFuture);
    when(region1SiteBClient.getByIndex(eq(cellByIndex))).thenReturn(CompletableFuture.completedFuture(cells));
    store.getByIndex(cellByIndex, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(futureCell));
    PipelinedResponse<StoreOperationResponse<List<Cell>>> responseCell = futureCell.get();
    if (responseCell.getOperationResponse().getError() != null) {
      throw new ExecutionException(responseCell.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (responseCell.getOperationResponse().getValue().equals(cells)));
    assertTrue("Should not throw exception", (responseCell.getOperationResponse().getError() == null));
    assertTrue("Should match with region1 siteB", (responseCell.getOperationResponse().getSite().equals(Region1SiteB)));
    assertTrue("Should be stale", (responseCell.isStale()));
    verify(region1SiteAClient, times(1)).getByIndex(cellByIndex);
    verify(region1SiteBClient, times(1)).getByIndex(cellByIndex);
  }

  @Test public void testGetByIndexFromRegion1SiteBAsDoubleFallback() throws Exception {
    storeRoute =
        new StoreRoute(Region.REGION_1, ReadConsistency.PRIMARY_PREFERRED, WriteConsistency.PRIMARY_MANDATORY,
            router);
    store = generatePipelinedStore(storeConfig, storeRoute, registry, intentStore);

    String errorMessage = "Failed";
    CompletableFuture respFuture = new CompletableFuture();
    respFuture.completeExceptionally(new PipelinedStoreException(errorMessage));

    CompletableFuture<PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>>> future = new CompletableFuture<>();
    when(region1SiteAClient.getByIndex(eq(columnsMapByIndex))).thenReturn(respFuture);
    when(region1SiteBClient.getByIndex(eq(columnsMapByIndex))).thenReturn(respFuture);
    when(region2SiteAClient.getByIndex(eq(columnsMapByIndex))).thenReturn(CompletableFuture.completedFuture(columns));
    store.getByIndex(columnsMapByIndex, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(future));
    PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>> response = future.get();
    if (response.getOperationResponse().getError() != null) {
      throw new ExecutionException(response.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (response.getOperationResponse().getValue().equals(columns)));
    assertTrue("Should not throw exception", (response.getOperationResponse().getError() == null));
    assertTrue("Should match with hyd siteA", (response.getOperationResponse().getSite().equals(Region2SiteA)));
    assertTrue("Should be stale", (response.isStale()));
    verify(region1SiteAClient, times(1)).getByIndex(columnsMapByIndex);
    verify(region1SiteBClient, times(1)).getByIndex(columnsMapByIndex);

    CompletableFuture<PipelinedResponse<StoreOperationResponse<List<Cell>>>> futureCell = new CompletableFuture<>();
    when(region1SiteAClient.getByIndex(eq(cellByIndex))).thenReturn(respFuture);
    when(region1SiteBClient.getByIndex(eq(cellByIndex))).thenReturn(respFuture);
    when(region2SiteAClient.getByIndex(eq(cellByIndex))).thenReturn(CompletableFuture.completedFuture(cells));
    store.getByIndex(cellByIndex, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(futureCell));
    PipelinedResponse<StoreOperationResponse<List<Cell>>> responseCell = futureCell.get();
    if (responseCell.getOperationResponse().getError() != null) {
      throw new ExecutionException(responseCell.getOperationResponse().getError());
    }
    assertTrue("Should have null response", (responseCell.getOperationResponse().getValue().equals(cells)));
    assertTrue("Should not throw exception", (responseCell.getOperationResponse().getError() == null));
    assertTrue("Should match with hyd siteA", (responseCell.getOperationResponse().getSite().equals(Region2SiteA)));
    assertTrue("Should be stale", (responseCell.isStale()));
    verify(region1SiteAClient, times(1)).getByIndex(cellByIndex);
    verify(region1SiteBClient, times(1)).getByIndex(cellByIndex);
    verify(region2SiteAClient, times(1)).getByIndex(cellByIndex);
  }
}
