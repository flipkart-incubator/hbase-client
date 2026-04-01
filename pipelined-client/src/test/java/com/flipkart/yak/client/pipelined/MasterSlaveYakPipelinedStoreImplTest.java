package com.flipkart.yak.client.pipelined;

import com.flipkart.yak.client.pipelined.models.PipelineConfig;
import com.flipkart.yak.client.pipelined.models.PipelinedResponse;
import com.flipkart.yak.client.pipelined.models.StoreOperationResponse;
import com.flipkart.yak.models.BatchData;
import com.flipkart.yak.models.BatchDataBuilder;
import com.flipkart.yak.models.Cell;
import com.flipkart.yak.models.CheckAndStoreData;
import com.flipkart.yak.models.ColumnsMap;
import com.flipkart.yak.models.DeleteData;
import com.flipkart.yak.models.DeleteDataBuilder;
import com.flipkart.yak.models.GetByIndexBuilder;
import com.flipkart.yak.models.GetCellByIndex;
import com.flipkart.yak.models.GetColumnsMapByIndex;
import com.flipkart.yak.models.GetDataBuilder;
import com.flipkart.yak.models.GetRow;
import com.flipkart.yak.models.IncrementData;
import com.flipkart.yak.models.IncrementDataBuilder;
import com.flipkart.yak.models.ResultMap;
import com.flipkart.yak.models.ScanData;
import com.flipkart.yak.models.ScanDataBuilder;
import com.flipkart.yak.models.SimpleIndexAttributes;
import com.flipkart.yak.models.CheckAndDeleteData;
import com.flipkart.yak.models.StoreData;
import com.flipkart.yak.models.StoreDataBuilder;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.powermock.core.classloader.annotations.PowerMockIgnore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Executor queue checks and, for each pipelined primitive, happy/failure paths through
 * {@code CompletableFuture.whenComplete} into the supplied {@code BiConsumer}.
 */
@PowerMockIgnore({ "java.lang.*", "javax.management.*" })
public class MasterSlaveYakPipelinedStoreImplTest extends PipelinedClientBaseTest {

  private StoreData putData;
  private List<StoreData> putListOneRow;
  private BatchData batchData;
  private DeleteData deleteOne;
  private List<DeleteData> deleteListOneRow;
  private GetRow getRow;
  private List<GetRow> getRowListOne;
  private IncrementData incrementData;
  private ResultMap resultMap;
  private CheckAndStoreData checkAndPutData;
  private CheckAndDeleteData checkAndDeleteData;
  private ScanData scanData;
  private Map<String, ResultMap> scanResult;
  private GetColumnsMapByIndex columnsMapByIndex;
  private List<ColumnsMap> indexColumnsResult;
  private GetCellByIndex cellByIndex;
  private List<Cell> indexCellsResult;
  private StoreData appendData;

  @Parameterized.Parameters(name = "hystrix={0}, intent={1}")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][] { { false, false } });
  }

  public MasterSlaveYakPipelinedStoreImplTest(boolean runWithHystrix, boolean runWithIntent) {
    this.runWithHystrix = runWithHystrix;
    this.runWithIntent = runWithIntent;
  }

  @Before
  public void setupPrimitivePayloads() {
    putData =
        new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).addColumn(cf1, cq1, qv1.getBytes()).build();
    putListOneRow = Collections.singletonList(putData);

    DeleteData batchDelete = new DeleteDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey2.getBytes()).build();
    batchData = new BatchDataBuilder(FULL_TABLE_NAME).addStoreData(putData).addDeleteData(batchDelete).build();

    deleteOne = new DeleteDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).build();
    deleteListOneRow = Collections.singletonList(deleteOne);

    getRow = new GetDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).build();
    getRowListOne = Collections.singletonList(getRow);

    incrementData = new IncrementDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes())
        .addColumn(cf1.getBytes(), cq1.getBytes(), 1L).build();

    resultMap = buildResultMap(rowKey1, cf1, cq1, qv1);

    checkAndPutData =
        new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).addColumn(cf1, cq1, "2".getBytes())
            .buildWithCheckAndVerifyColumn(cf1, cq1, qv1.getBytes(), CompareOperator.EQUAL.name());

    checkAndDeleteData = new DeleteDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes())
        .buildWithCheckAndVerifyColumn(cf1, cq1, qv1.getBytes(), CompareOperator.EQUAL.name());

    FilterList filterList = new FilterList();
    filterList.addFilter(new ColumnPrefixFilter(rowKey1.getBytes()));
    scanData = new ScanDataBuilder(FULL_TABLE_NAME).withFilterList(filterList).withLimit(1).build();
    scanResult = new HashMap<>();
    scanResult.put(rowKey1, resultMap);

    ColumnsMap map1 = new ColumnsMap();
    map1.put(cq1, new Cell(qv1.getBytes()));
    indexColumnsResult = Collections.singletonList(map1);
    columnsMapByIndex =
        new GetByIndexBuilder(FULL_TABLE_NAME, cf1).withIndexKey(indexKey1, new SimpleIndexAttributes()).build();

    indexCellsResult = Collections.singletonList(new Cell(qv1.getBytes()));
    cellByIndex = new GetByIndexBuilder(FULL_TABLE_NAME, cf1).withIndexKey(indexKey1, new SimpleIndexAttributes())
        .buildForCloumn(cq1);

    appendData =
        new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).addColumn(cf1, cq1, qv1.getBytes()).build();
  }

  private static ThreadPoolExecutor executorFrom(MasterSlaveYakPipelinedStoreImpl<?, ?, ?> impl) throws Exception {
    Field f = MasterSlaveYakPipelinedStoreImpl.class.getDeclaredField("executor");
    f.setAccessible(true);
    return (ThreadPoolExecutor) f.get(impl);
  }

  private static <T> CompletableFuture<T> failedFuture(Throwable t) {
    CompletableFuture<T> f = new CompletableFuture<>();
    f.completeExceptionally(t);
    return f;
  }

  @After
  public void shutdownStore() throws IOException, InterruptedException {
    if (store != null) {
      store.shutdown();
    }
  }

  @Test
  public void executorWorkQueueIsUnboundedWhenExecutorQueueCapacityUnset() throws Exception {
    assertTrue(store instanceof MasterSlaveYakPipelinedStoreImpl);
    ThreadPoolExecutor ex = executorFrom((MasterSlaveYakPipelinedStoreImpl<?, ?, ?>) store);
    assertTrue(ex.getQueue() instanceof LinkedBlockingQueue);
    assertEquals(Integer.MAX_VALUE, ((LinkedBlockingQueue<?>) ex.getQueue()).remainingCapacity());
  }

  @Test
  public void executorWorkQueueMatchesPipelineConfigCapacity() throws Exception {
    int capacity = 17;
    PipelineConfig boundedConfig =
        new PipelineConfig(buildConfig(), timeout, poolSize, STORE_NAME, Optional.of(keyDistributorMap),
            siteBootstrapRetryCount, siteBootstrapRetryDelayInMillis, capacity);
    MasterSlaveYakPipelinedStoreImpl<?, ?, ?> impl =
        new MasterSlaveYakPipelinedStoreImpl<>(boundedConfig, storeRoute, registry);
    try {
      ThreadPoolExecutor ex = executorFrom(impl);
      LinkedBlockingQueue<?> q = (LinkedBlockingQueue<?>) ex.getQueue();
      assertEquals(capacity, q.remainingCapacity() + q.size());
      assertEquals(capacity, q.remainingCapacity());
    } finally {
      impl.shutdown();
    }
  }

  @Test
  public void putSuccessRunsWhenCompleteAndDeliversPipelinedResponse() throws Exception {
    assertImpl();
    when(region1SiteAClient.put(eq(putData))).thenReturn(CompletableFuture.completedFuture(null));
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Void>>> done = new CompletableFuture<>();
    store.put(putData, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(done));
    assertVoidSuccess(done.get());
    verify(region1SiteAClient, times(1)).put(putData);
  }

  @Test
  public void putClientFailureRunsWhenCompleteAndDeliversErrorInResponse() throws Exception {
    assertImpl();
    RuntimeException failure = new RuntimeException("put failed");
    when(region1SiteAClient.put(eq(putData))).thenReturn(failedFuture(failure));
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Void>>> done = new CompletableFuture<>();
    store.put(putData, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(done));
    assertVoidFailure(done.get(), failure);
    verify(region1SiteAClient, times(1)).put(putData);
  }

  @Test
  public void incrementSuccessRunsWhenCompleteAndDeliversPipelinedResponse() throws Exception {
    assertImpl();
    when(region1SiteAClient.increment(eq(incrementData))).thenReturn(CompletableFuture.completedFuture(resultMap));
    CompletableFuture<PipelinedResponse<StoreOperationResponse<ResultMap>>> done = new CompletableFuture<>();
    store.increment(incrementData, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(done));
    PipelinedResponse<StoreOperationResponse<ResultMap>> r = done.get();
    assertNull(r.getOperationResponse().getError());
    assertEquals(resultMap, r.getOperationResponse().getValue());
    assertEquals(Region1SiteA, r.getOperationResponse().getSite());
    assertFalse(r.isStale());
    verify(region1SiteAClient, times(1)).increment(incrementData);
  }

  @Test
  public void incrementClientFailureRunsWhenCompleteAndDeliversErrorInResponse() throws Exception {
    assertImpl();
    RuntimeException failure = new RuntimeException("increment failed");
    when(region1SiteAClient.increment(eq(incrementData))).thenReturn(failedFuture(failure));
    CompletableFuture<PipelinedResponse<StoreOperationResponse<ResultMap>>> done = new CompletableFuture<>();
    store.increment(incrementData, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(done));
    assertFailure(done.get(), failure);
    verify(region1SiteAClient, times(1)).increment(incrementData);
  }

  @Test
  public void batchSuccessRunsWhenCompleteAndDeliversPipelinedResponse() throws Exception {
    assertImpl();
    when(region1SiteAClient.batch(eq(batchData))).thenReturn(CompletableFuture.completedFuture(null));
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Void>>> done = new CompletableFuture<>();
    store.batch(batchData, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(done));
    assertVoidSuccess(done.get());
    verify(region1SiteAClient, times(1)).batch(batchData);
  }

  @Test
  public void batchClientFailureRunsWhenCompleteAndDeliversErrorInResponse() throws Exception {
    assertImpl();
    RuntimeException failure = new RuntimeException("batch failed");
    when(region1SiteAClient.batch(eq(batchData))).thenReturn(failedFuture(failure));
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Void>>> done = new CompletableFuture<>();
    store.batch(batchData, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(done));
    assertVoidFailure(done.get(), failure);
    verify(region1SiteAClient, times(1)).batch(batchData);
  }

  @Test
  public void batchPutListSuccessRunsWhenCompleteAndDeliversPipelinedResponse() throws Exception {
    assertImpl();
    when(region1SiteAClient.put(eq(putListOneRow)))
        .thenReturn(Collections.singletonList(CompletableFuture.completedFuture(null)));
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<Void>>>> done = new CompletableFuture<>();
    store.put(putListOneRow, routeMetaChOptional, Optional.empty(), hystrixSettings, responsesHandler(done));
    PipelinedResponse<List<StoreOperationResponse<Void>>> r = done.get();
    assertEquals(1, r.getOperationResponse().size());
    assertNull(r.getOperationResponse().get(0).getError());
    assertEquals(Region1SiteA, r.getOperationResponse().get(0).getSite());
    assertFalse(r.isStale());
    verify(region1SiteAClient, times(1)).put(putListOneRow);
  }

  @Test
  public void batchPutListClientFailureRunsWhenCompleteAndDeliversErrorInResponse() throws Exception {
    assertImpl();
    RuntimeException failure = new RuntimeException("batch put failed");
    when(region1SiteAClient.put(eq(putListOneRow)))
        .thenReturn(Collections.singletonList(failedFuture(failure)));
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<Void>>>> done = new CompletableFuture<>();
    store.put(putListOneRow, routeMetaChOptional, Optional.empty(), hystrixSettings, responsesHandler(done));
    PipelinedResponse<List<StoreOperationResponse<Void>>> r = done.get();
    assertEquals(1, r.getOperationResponse().size());
    assertEquals(failure, r.getOperationResponse().get(0).getError());
    verify(region1SiteAClient, times(1)).put(putListOneRow);
  }

  @Test
  public void checkAndPutSuccessRunsWhenCompleteAndDeliversPipelinedResponse() throws Exception {
    assertImpl();
    when(region1SiteAClient.checkAndPut(eq(checkAndPutData))).thenReturn(CompletableFuture.completedFuture(true));
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> done = new CompletableFuture<>();
    store.checkAndPut(checkAndPutData, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(done));
    PipelinedResponse<StoreOperationResponse<Boolean>> r = done.get();
    assertNull(r.getOperationResponse().getError());
    assertTrue(r.getOperationResponse().getValue());
    assertEquals(Region1SiteA, r.getOperationResponse().getSite());
    assertFalse(r.isStale());
    verify(region1SiteAClient, times(1)).checkAndPut(checkAndPutData);
  }

  @Test
  public void checkAndPutClientFailureRunsWhenCompleteAndDeliversErrorInResponse() throws Exception {
    assertImpl();
    RuntimeException failure = new RuntimeException("cas put failed");
    when(region1SiteAClient.checkAndPut(eq(checkAndPutData))).thenReturn(failedFuture(failure));
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> done = new CompletableFuture<>();
    store.checkAndPut(checkAndPutData, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(done));
    assertFailure(done.get(), failure);
    verify(region1SiteAClient, times(1)).checkAndPut(checkAndPutData);
  }

  @Test
  public void appendSuccessRunsWhenCompleteAndDeliversPipelinedResponse() throws Exception {
    assertImpl();
    when(region1SiteAClient.append(eq(appendData))).thenReturn(CompletableFuture.completedFuture(resultMap));
    CompletableFuture<PipelinedResponse<StoreOperationResponse<ResultMap>>> done = new CompletableFuture<>();
    store.append(appendData, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(done));
    PipelinedResponse<StoreOperationResponse<ResultMap>> r = done.get();
    assertNull(r.getOperationResponse().getError());
    assertEquals(resultMap, r.getOperationResponse().getValue());
    assertEquals(Region1SiteA, r.getOperationResponse().getSite());
    assertFalse(r.isStale());
    verify(region1SiteAClient, times(1)).append(appendData);
  }

  @Test
  public void appendClientFailureRunsWhenCompleteAndDeliversErrorInResponse() throws Exception {
    assertImpl();
    RuntimeException failure = new RuntimeException("append failed");
    when(region1SiteAClient.append(eq(appendData))).thenReturn(failedFuture(failure));
    CompletableFuture<PipelinedResponse<StoreOperationResponse<ResultMap>>> done = new CompletableFuture<>();
    store.append(appendData, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(done));
    assertFailure(done.get(), failure);
    verify(region1SiteAClient, times(1)).append(appendData);
  }

  @Test
  public void batchDeleteListSuccessRunsWhenCompleteAndDeliversPipelinedResponse() throws Exception {
    assertImpl();
    when(region1SiteAClient.delete(eq(deleteListOneRow)))
        .thenReturn(Collections.singletonList(CompletableFuture.completedFuture(null)));
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<Void>>>> done = new CompletableFuture<>();
    store.delete(deleteListOneRow, routeMetaChOptional, Optional.empty(), hystrixSettings, responsesHandler(done));
    PipelinedResponse<List<StoreOperationResponse<Void>>> r = done.get();
    assertEquals(1, r.getOperationResponse().size());
    assertNull(r.getOperationResponse().get(0).getError());
    assertEquals(Region1SiteA, r.getOperationResponse().get(0).getSite());
    assertFalse(r.isStale());
    verify(region1SiteAClient, times(1)).delete(deleteListOneRow);
  }

  @Test
  public void batchDeleteListClientFailureRunsWhenCompleteAndDeliversErrorInResponse() throws Exception {
    assertImpl();
    RuntimeException failure = new RuntimeException("batch delete failed");
    when(region1SiteAClient.delete(eq(deleteListOneRow)))
        .thenReturn(Collections.singletonList(failedFuture(failure)));
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<Void>>>> done = new CompletableFuture<>();
    store.delete(deleteListOneRow, routeMetaChOptional, Optional.empty(), hystrixSettings, responsesHandler(done));
    PipelinedResponse<List<StoreOperationResponse<Void>>> r = done.get();
    assertEquals(1, r.getOperationResponse().size());
    assertEquals(failure, r.getOperationResponse().get(0).getError());
    verify(region1SiteAClient, times(1)).delete(deleteListOneRow);
  }

  @Test
  public void checkAndDeleteSuccessRunsWhenCompleteAndDeliversPipelinedResponse() throws Exception {
    assertImpl();
    when(region1SiteAClient.checkAndDelete(eq(checkAndDeleteData))).thenReturn(CompletableFuture.completedFuture(true));
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> done = new CompletableFuture<>();
    store.checkAndDelete(checkAndDeleteData, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(done));
    PipelinedResponse<StoreOperationResponse<Boolean>> r = done.get();
    assertNull(r.getOperationResponse().getError());
    assertTrue(r.getOperationResponse().getValue());
    assertEquals(Region1SiteA, r.getOperationResponse().getSite());
    assertFalse(r.isStale());
    verify(region1SiteAClient, times(1)).checkAndDelete(checkAndDeleteData);
  }

  @Test
  public void checkAndDeleteClientFailureRunsWhenCompleteAndDeliversErrorInResponse() throws Exception {
    assertImpl();
    RuntimeException failure = new RuntimeException("check delete failed");
    when(region1SiteAClient.checkAndDelete(eq(checkAndDeleteData))).thenReturn(failedFuture(failure));
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Boolean>>> done = new CompletableFuture<>();
    store.checkAndDelete(checkAndDeleteData, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(done));
    assertFailure(done.get(), failure);
    verify(region1SiteAClient, times(1)).checkAndDelete(checkAndDeleteData);
  }

  @Test
  public void scanSuccessRunsWhenCompleteAndDeliversPipelinedResponse() throws Exception {
    assertImpl();
    when(region1SiteAClient.scan(eq(scanData))).thenReturn(CompletableFuture.completedFuture(scanResult));
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>>> done = new CompletableFuture<>();
    store.scan(scanData, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(done));
    PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>> r = done.get();
    assertNull(r.getOperationResponse().getError());
    assertEquals(scanResult, r.getOperationResponse().getValue());
    assertEquals(Region1SiteA, r.getOperationResponse().getSite());
    assertFalse(r.isStale());
    verify(region1SiteAClient, times(1)).scan(scanData);
  }

  @Test
  public void scanClientFailureRunsWhenCompleteAndDeliversErrorInResponse() throws Exception {
    assertImpl();
    RuntimeException failure = new RuntimeException("scan failed");
    when(region1SiteAClient.scan(eq(scanData))).thenReturn(failedFuture(failure));
    CompletableFuture<PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>>> done = new CompletableFuture<>();
    store.scan(scanData, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(done));
    PipelinedResponse<StoreOperationResponse<Map<String, ResultMap>>> r = done.get();
    assertNotNull(r.getOperationResponse().getError());
    assertEquals(failure, r.getOperationResponse().getError());
    assertEquals(Region1SiteA, r.getOperationResponse().getSite());
    verify(region1SiteAClient, times(1)).scan(scanData);
  }

  @Test
  public void getSuccessRunsWhenCompleteAndDeliversPipelinedResponse() throws Exception {
    assertImpl();
    when(region1SiteAClient.get(eq(getRow))).thenReturn(CompletableFuture.completedFuture(resultMap));
    CompletableFuture<PipelinedResponse<StoreOperationResponse<ResultMap>>> done = new CompletableFuture<>();
    store.get(getRow, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(done));
    PipelinedResponse<StoreOperationResponse<ResultMap>> r = done.get();
    assertNull(r.getOperationResponse().getError());
    assertEquals(resultMap, r.getOperationResponse().getValue());
    assertEquals(Region1SiteA, r.getOperationResponse().getSite());
    assertFalse(r.isStale());
    verify(region1SiteAClient, times(1)).get(getRow);
  }

  @Test
  public void getClientFailureRunsWhenCompleteAndDeliversErrorInResponse() throws Exception {
    assertImpl();
    RuntimeException failure = new RuntimeException("get failed");
    when(region1SiteAClient.get(eq(getRow))).thenReturn(failedFuture(failure));
    CompletableFuture<PipelinedResponse<StoreOperationResponse<ResultMap>>> done = new CompletableFuture<>();
    store.get(getRow, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(done));
    assertFailure(done.get(), failure);
    verify(region1SiteAClient, times(1)).get(getRow);
  }

  @Test
  public void batchGetListSuccessRunsWhenCompleteAndDeliversPipelinedResponse() throws Exception {
    assertImpl();
    when(region1SiteAClient.get(eq(getRowListOne)))
        .thenReturn(Collections.singletonList(CompletableFuture.completedFuture(resultMap)));
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<ResultMap>>>> done = new CompletableFuture<>();
    store.get(getRowListOne, routeMetaChOptional, Optional.empty(), hystrixSettings, responsesHandler(done));
    PipelinedResponse<List<StoreOperationResponse<ResultMap>>> r = done.get();
    assertEquals(1, r.getOperationResponse().size());
    assertNull(r.getOperationResponse().get(0).getError());
    assertEquals(resultMap, r.getOperationResponse().get(0).getValue());
    assertFalse(r.isStale());
    verify(region1SiteAClient, times(1)).get(getRowListOne);
  }

  @Test
  public void batchGetListClientFailureRunsWhenCompleteAndDeliversErrorInResponse() throws Exception {
    assertImpl();
    RuntimeException failure = new RuntimeException("batch get failed");
    when(region1SiteAClient.get(eq(getRowListOne)))
        .thenReturn(Collections.singletonList(failedFuture(failure)));
    CompletableFuture<PipelinedResponse<List<StoreOperationResponse<ResultMap>>>> done = new CompletableFuture<>();
    store.get(getRowListOne, routeMetaChOptional, Optional.empty(), hystrixSettings, responsesHandler(done));
    PipelinedResponse<List<StoreOperationResponse<ResultMap>>> r = done.get();
    assertEquals(1, r.getOperationResponse().size());
    assertEquals(failure, r.getOperationResponse().get(0).getError());
    verify(region1SiteAClient, times(1)).get(getRowListOne);
  }

  @Test
  public void getByIndexColumnsSuccessRunsWhenCompleteAndDeliversPipelinedResponse() throws Exception {
    assertImpl();
    when(region1SiteAClient.getByIndex(eq(columnsMapByIndex)))
        .thenReturn(CompletableFuture.completedFuture(indexColumnsResult));
    CompletableFuture<PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>>> done = new CompletableFuture<>();
    store.getByIndex(columnsMapByIndex, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(done));
    PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>> r = done.get();
    assertNull(r.getOperationResponse().getError());
    assertEquals(indexColumnsResult, r.getOperationResponse().getValue());
    assertEquals(Region1SiteA, r.getOperationResponse().getSite());
    assertFalse(r.isStale());
    verify(region1SiteAClient, times(1)).getByIndex(columnsMapByIndex);
  }

  @Test
  public void getByIndexColumnsClientFailureRunsWhenCompleteAndDeliversErrorInResponse() throws Exception {
    assertImpl();
    RuntimeException failure = new RuntimeException("getByIndex columns failed");
    when(region1SiteAClient.getByIndex(eq(columnsMapByIndex))).thenReturn(failedFuture(failure));
    CompletableFuture<PipelinedResponse<StoreOperationResponse<List<ColumnsMap>>>> done = new CompletableFuture<>();
    store.getByIndex(columnsMapByIndex, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(done));
    assertFailure(done.get(), failure);
    verify(region1SiteAClient, times(1)).getByIndex(columnsMapByIndex);
  }

  @Test
  public void getByIndexCellSuccessRunsWhenCompleteAndDeliversPipelinedResponse() throws Exception {
    assertImpl();
    when(region1SiteAClient.getByIndex(eq(cellByIndex))).thenReturn(CompletableFuture.completedFuture(indexCellsResult));
    CompletableFuture<PipelinedResponse<StoreOperationResponse<List<Cell>>>> done = new CompletableFuture<>();
    store.getByIndex(cellByIndex, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(done));
    PipelinedResponse<StoreOperationResponse<List<Cell>>> r = done.get();
    assertNull(r.getOperationResponse().getError());
    assertEquals(indexCellsResult, r.getOperationResponse().getValue());
    assertEquals(Region1SiteA, r.getOperationResponse().getSite());
    assertFalse(r.isStale());
    verify(region1SiteAClient, times(1)).getByIndex(cellByIndex);
  }

  @Test
  public void getByIndexCellClientFailureRunsWhenCompleteAndDeliversErrorInResponse() throws Exception {
    assertImpl();
    RuntimeException failure = new RuntimeException("getByIndex cell failed");
    when(region1SiteAClient.getByIndex(eq(cellByIndex))).thenReturn(failedFuture(failure));
    CompletableFuture<PipelinedResponse<StoreOperationResponse<List<Cell>>>> done = new CompletableFuture<>();
    store.getByIndex(cellByIndex, routeMetaChOptional, Optional.empty(), hystrixSettings, responseHandler(done));
    assertFailure(done.get(), failure);
    verify(region1SiteAClient, times(1)).getByIndex(cellByIndex);
  }

  private void assertImpl() {
    assertTrue(store instanceof MasterSlaveYakPipelinedStoreImpl);
  }

  private void assertVoidSuccess(PipelinedResponse<StoreOperationResponse<Void>> r) {
    assertNull(r.getOperationResponse().getError());
    assertEquals(Region1SiteA, r.getOperationResponse().getSite());
    assertFalse(r.isStale());
  }

  private void assertVoidFailure(PipelinedResponse<StoreOperationResponse<Void>> r, Throwable failure) {
    assertNotNull(r.getOperationResponse().getError());
    assertEquals(failure, r.getOperationResponse().getError());
    assertEquals(Region1SiteA, r.getOperationResponse().getSite());
  }

  private <T> void assertFailure(PipelinedResponse<StoreOperationResponse<T>> r, Throwable failure) {
    assertNotNull(r.getOperationResponse().getError());
    assertEquals(failure, r.getOperationResponse().getError());
    assertEquals(Region1SiteA, r.getOperationResponse().getSite());
  }
}
