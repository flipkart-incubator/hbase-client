package com.flipkart.yak.client;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.distributor.KeyDistributor;
import com.flipkart.yak.distributor.MurmusHashDistribution;
import com.flipkart.yak.models.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.Matchers.argThat;
import static org.powermock.api.mockito.PowerMockito.when;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class) public class AsyncBatchGetTest extends AsyncBaseTest {

  private AsyncStoreClient storeClient;
  Map<String, KeyDistributor> keyDistributorMap = new HashMap<>();
  MetricRegistry registry = new MetricRegistry();
  int timeoutInSeconds = 30;
  SiteConfig config;

  private String rowKey1, rowKey2;
  private String cf1, cf2;
  private String cq1, cq2;
  private String qv1, qv2;
  private List<Get> gets;
  private List<GetRow> rows;

  @Before public void setUp() throws Exception {
    super.setup();
    config = buildStoreClientConfig();
    keyDistributorMap.put(FULL_TABLE_NAME, new MurmusHashDistribution(128));
    storeClient = new AsyncStoreClientImpl(config, Optional.of(keyDistributorMap), timeoutInSeconds, registry);

    rowKey1 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    cf1 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    cq1 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    qv1 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    rowKey2 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    cf2 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    cq2 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    qv2 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    GetRow row1 = new GetDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).build();
    GetRow row2 = new GetDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey2.getBytes()).build();
    rows = Arrays.asList(row1, row2);

    Get get1 = AsyncStoreClientUtis.buildGets(Arrays.asList(row1), keyDistributorMap).get(0);
    Get get2 = AsyncStoreClientUtis.buildGets(Arrays.asList(row2), keyDistributorMap).get(0);
    gets = Arrays.asList(get1, get2);
  }

  @After public void tearDown() throws Exception {
    storeClient.shutdown();
  }

  private void compareResult(ResultMap map, String cf, String cq, String qv, int cfSize, int cqSize) {
    assertNotNull("Expect result to be not null", map);
    assertTrue("Expect only one column family to be present", map.size() == cfSize);
    assertTrue("Expect column family to be present", map.containsKey(cf));
    assertTrue("Expect only one cell to be present", map.get(cf).size() == cqSize);
    assertTrue("Expect column to be present in column family", map.get(cf).containsKey(cq));
    assertTrue("Qualifier value should be equal", Bytes.toString(map.get(cf).get(cq).getValue()).equals(qv));
  }

  @Test public void testBatchGetByGetRow()
      throws StoreException, IOException, ExecutionException, InterruptedException {
    List<CompletableFuture<Result>> futures = rows.stream().map(get -> {
      return new CompletableFuture<Result>();
    }).collect(Collectors.toList());
    futures.get(0).complete(buildResponse(rowKey1, Arrays.asList(cf1), Arrays.asList(cq1), Arrays.asList(qv1)));
    futures.get(1).complete(buildResponse(rowKey2, Arrays.asList(cf2), Arrays.asList(cq2), Arrays.asList(qv2)));

    when(table.get(argThat(new BatchGetMatcher(gets)))).thenReturn(futures);
    List<CompletableFuture<ResultMap>> outputFutures = storeClient.get(rows);

    List<ResultMap> resultMaps =
        CompletableFuture.allOf(outputFutures.toArray(new CompletableFuture[outputFutures.size()])).thenApply(v -> {
          return outputFutures.stream().map(future -> future.join()).collect(Collectors.toList());
        }).get();

    assertEquals("Batch Get should have been " + resultMaps.size(), resultMaps.size(), outputFutures.size());
    compareResult(resultMaps.get(0), cf1, cq1, qv1, 1, 1);
    compareResult(resultMaps.get(1), cf2, cq2, qv2, 1, 1);
  }

  @Test public void testBatchGetByGetCell()
      throws StoreException, IOException, ExecutionException, InterruptedException {
    List<CompletableFuture<Result>> futures = rows.stream().map(get -> {
      return new CompletableFuture<Result>();
    }).collect(Collectors.toList());
    futures.get(0).complete(buildResponse(rowKey1, Arrays.asList(cf1), Arrays.asList(cq1), Arrays.asList(qv1)));
    futures.get(1).complete(buildResponse(rowKey2, Arrays.asList(cf2), Arrays.asList(cq2), Arrays.asList(qv2)));

    GetCell row1 = new GetDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).buildForColumn(cf1, cq1);
    GetCell row2 = new GetDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey2.getBytes()).buildForColumn(cf2, cq2);
    rows = Arrays.asList(row1, row2);

    Get get1 = AsyncStoreClientUtis.buildGets(Arrays.asList(row1), keyDistributorMap).get(0);
    Get get2 = AsyncStoreClientUtis.buildGets(Arrays.asList(row2), keyDistributorMap).get(0);
    gets = Arrays.asList(get1, get2);

    when(table.get(argThat(new BatchGetMatcher(gets)))).thenReturn(futures);
    List<CompletableFuture<ResultMap>> outputFutures = storeClient.get(rows);
    List<ResultMap> resultMaps =
        CompletableFuture.allOf(outputFutures.toArray(new CompletableFuture[outputFutures.size()])).thenApply(v -> {
          return outputFutures.stream().map(future -> future.join()).collect(Collectors.toList());
        }).get();

    assertEquals("Batch Get should have been " + resultMaps.size(), resultMaps.size(), outputFutures.size());
    compareResult(resultMaps.get(0), cf1, cq1, qv1, 1, 1);
    compareResult(resultMaps.get(1), cf2, cq2, qv2, 1, 1);
  }

  @Test public void testGetByGetCells() throws StoreException, IOException, ExecutionException, InterruptedException {
    Result result1 = buildResponse(rowKey1, Arrays.asList(cf1, cf2), Arrays.asList(cq1, cq2), Arrays.asList(qv1, qv2));
    Result result2 = buildResponse(rowKey2, Arrays.asList(cf1, cf2), Arrays.asList(cq1, cq2), Arrays.asList(qv1, qv2));
    List<CompletableFuture<Result>> futures = rows.stream().map(get -> {
      return new CompletableFuture<Result>();
    }).collect(Collectors.toList());
    futures.get(0).complete(result1);
    futures.get(1).complete(result2);

    GetCells row1 = (GetCells) new GetDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).addColl(cf1, cq1)
        .addColl(cf2, cq2).build();
    GetCells row2 = (GetCells) new GetDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey2.getBytes()).addColl(cf2, cq2)
        .addColl(cf1, cq1).build();
    rows = Arrays.asList(row1, row2);

    Get get1 = AsyncStoreClientUtis.buildGets(Arrays.asList(row1), keyDistributorMap).get(0);
    Get get2 = AsyncStoreClientUtis.buildGets(Arrays.asList(row2), keyDistributorMap).get(0);
    gets = Arrays.asList(get1, get2);

    when(table.get(argThat(new BatchGetMatcher(gets)))).thenReturn(futures);
    List<CompletableFuture<ResultMap>> outputFutures = storeClient.get(rows);
    List<ResultMap> resultMaps =
        CompletableFuture.allOf(outputFutures.toArray(new CompletableFuture[outputFutures.size()])).thenApply(v -> {
          return outputFutures.stream().map(future -> future.join()).collect(Collectors.toList());
        }).get();

    assertEquals("Batch Get should have been " + resultMaps.size(), resultMaps.size(), outputFutures.size());
    compareResult(resultMaps.get(0), cf2, cq2, qv2, 2, 1);
    compareResult(resultMaps.get(1), cf1, cq1, qv1, 2, 1);
  }

  @Test public void testGetByGetColumnsMap()
      throws StoreException, IOException, ExecutionException, InterruptedException {
    Result result1 = buildResponse(rowKey1, Arrays.asList(cf2), Arrays.asList(cq2), Arrays.asList(qv2));
    Result result2 = buildResponse(rowKey2, Arrays.asList(cf1), Arrays.asList(cq1), Arrays.asList(qv1));
    List<CompletableFuture<Result>> futures = rows.stream().map(get -> {
      return new CompletableFuture<Result>();
    }).collect(Collectors.toList());
    futures.get(0).complete(result1);
    futures.get(1).complete(result2);

    GetColumnMap row1 = new GetDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).buildForColFamily(cf2);
    GetColumnMap row2 = new GetDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey2.getBytes()).buildForColFamily(cf1);
    rows = Arrays.asList(row1, row2);

    Get get1 = AsyncStoreClientUtis.buildGets(Arrays.asList(row1), keyDistributorMap).get(0);
    Get get2 = AsyncStoreClientUtis.buildGets(Arrays.asList(row2), keyDistributorMap).get(0);
    gets = Arrays.asList(get1, get2);

    when(table.get(argThat(new BatchGetMatcher(gets)))).thenReturn(futures);
    List<CompletableFuture<ResultMap>> outputFutures = storeClient.get(rows);
    List<ResultMap> resultMaps =
        CompletableFuture.allOf(outputFutures.toArray(new CompletableFuture[outputFutures.size()])).thenApply(v -> {
          return outputFutures.stream().map(future -> future.join()).collect(Collectors.toList());
        }).get();

    assertEquals("Batch Get should have been " + resultMaps.size(), resultMaps.size(), outputFutures.size());
    compareResult(resultMaps.get(0), cf2, cq2, qv2, 1, 1);
    compareResult(resultMaps.get(1), cf1, cq1, qv1, 1, 1);
  }

  @Test public void testGetWithOutKeyDistributer()
      throws StoreException, IOException, ExecutionException, InterruptedException, TimeoutException {

    storeClient = new AsyncStoreClientImpl(config, Optional.empty(), timeoutInSeconds, registry);

    List<CompletableFuture<Result>> futures = rows.stream().map(get -> {
      return new CompletableFuture<Result>();
    }).collect(Collectors.toList());
    futures.get(0).complete(buildResponse(rowKey1, Arrays.asList(cf1), Arrays.asList(cq1), Arrays.asList(qv1)));
    futures.get(1).complete(buildResponse(rowKey2, Arrays.asList(cf2), Arrays.asList(cq2), Arrays.asList(qv2)));

    GetRow row1 = new GetDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).build();
    GetRow row2 = new GetDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey2.getBytes()).build();
    rows = Arrays.asList(row1, row2);

    Get get1 = AsyncStoreClientUtis.buildGets(Arrays.asList(row1), new HashMap<>()).get(0);
    Get get2 = AsyncStoreClientUtis.buildGets(Arrays.asList(row2), new HashMap<>()).get(0);
    gets = Arrays.asList(get1, get2);

    when(table.get(argThat(new BatchGetMatcher(gets)))).thenReturn(futures);
    List<CompletableFuture<ResultMap>> outputFutures = storeClient.get(rows);

    List<ResultMap> resultMaps =
        CompletableFuture.allOf(outputFutures.toArray(new CompletableFuture[outputFutures.size()])).thenApply(v -> {
          return outputFutures.stream().map(future -> future.join()).collect(Collectors.toList());
        }).get();

    assertEquals("Batch Get should have been " + resultMaps.size(), resultMaps.size(), outputFutures.size());
    compareResult(resultMaps.get(0), cf1, cq1, qv1, 1, 1);
    compareResult(resultMaps.get(1), cf2, cq2, qv2, 1, 1);
  }

  @Test public void testGetWithNoDataFoundException()
      throws InterruptedException, StoreException, IOException, ExecutionException {
    List<CompletableFuture<Result>> futures = rows.stream().map(get -> {
      return new CompletableFuture<Result>();
    }).collect(Collectors.toList());
    futures.get(0).complete(buildResponse(rowKey1, Arrays.asList(cf1), Arrays.asList(cq1), Arrays.asList(qv1)));
    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).build();
    futures.get(1).complete(buildResultData(storeData.getRow(), storeData.getCfs()));
    String errorMessage = "Data Not Found";

    when(table.get(argThat(new BatchGetMatcher(gets)))).thenReturn(futures);
    List<CompletableFuture<ResultMap>> outputFutures = storeClient.get(rows);

    compareResult(outputFutures.get(0).get(), cf1, cq1, qv1, 1, 1);
    try {
      outputFutures.get(1).get();
      Assert.assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof StoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }

  @Test public void testGetWithException() throws InterruptedException, StoreException, IOException {
    String errorMessage = "Failed with exception";
    List<CompletableFuture<Result>> futures = rows.stream().map(get -> {
      CompletableFuture<Result> future = new CompletableFuture<Result>();
      future.completeExceptionally(new StoreException(errorMessage));
      return future;
    }).collect(Collectors.toList());

    when(table.get(argThat(new BatchGetMatcher(gets)))).thenReturn(futures);
    List<CompletableFuture<ResultMap>> outputFutures = storeClient.get(rows);
    try {
      outputFutures.get(1).get();
      Assert.assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof StoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
    try {
      outputFutures.get(0).get();
      Assert.assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof StoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }

  @Test public void testBatchGetByGetRowWithVersions()
          throws StoreException, IOException, ExecutionException, InterruptedException {
    List<CompletableFuture<Result>> futures = rows.stream().map(get -> {
      return new CompletableFuture<Result>();
    }).collect(Collectors.toList());
    GetRow row1 = new GetDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).withMaxVersions(11).build();
    GetRow row2 = new GetDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey2.getBytes()).withMaxVersions(10).build();

    List<GetRow> rows = Arrays.asList(row1, row2);

    TreeMap<Long, byte[]> qv1Versions = new TreeMap((Comparator<Long>) (l1, l2) -> l2.compareTo(l1));
    TreeMap<Long, byte[]> qv2Versions = new TreeMap((Comparator<Long>) (l1, l2) -> l2.compareTo(l1));
    String qualifierValue = null;
    String qv1 = null;
    String qv2 = null;

    for(long i=0; i<10; i++){
      qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
      qv1Versions.put(i, qualifierValue.getBytes());
      qv2Versions.put(i, qualifierValue.getBytes());
    }

    qv2 = qualifierValue;
    qv1 = RandomStringUtils.randomAlphanumeric(11).toUpperCase();
    qv1Versions.put(11L, qv1.getBytes());

    futures.get(0).complete(buildResponse(rowKey1, Arrays.asList(cf1), Arrays.asList(cq1), Lists.newArrayList(qv1Versions)));
    futures.get(1).complete(buildResponse(rowKey2, Arrays.asList(cf2), Arrays.asList(cq2), Lists.newArrayList(qv2Versions)));

    when(table.get(argThat(new BatchGetMatcher(gets)))).thenReturn(futures);
    List<CompletableFuture<ResultMap>> outputFutures = storeClient.get(rows);

    List<ResultMap> resultMaps =
            CompletableFuture.allOf(outputFutures.toArray(new CompletableFuture[outputFutures.size()])).thenApply(v -> {
              return outputFutures.stream().map(future -> future.join()).collect(Collectors.toList());
            }).get();

    assertEquals("Batch Get should have been " + resultMaps.size(), resultMaps.size(), outputFutures.size());
    compareResult(resultMaps.get(0), cf1, cq1, qv1, 1, 1);
    compareResult(resultMaps.get(1), cf2, cq2, qv2, 1, 1);
    assertTrue("Versions should be equal", resultMaps.get(0).get(cf1).get(cq1).getVersionedValues().size() == 11);
    assertTrue("Versions should be equal", resultMaps.get(1).get(cf2).get(cq2).getVersionedValues().size() == 10);
  }
}
