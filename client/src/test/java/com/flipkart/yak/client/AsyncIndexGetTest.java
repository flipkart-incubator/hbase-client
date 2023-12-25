package com.flipkart.yak.client;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.exceptions.StoreDataNotFoundException;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.distributor.KeyDistributor;
import com.flipkart.yak.distributor.MurmusHashDistribution;
import com.flipkart.yak.models.AppendOnlyIndexAttributes;
import com.flipkart.yak.models.Cell;
import com.flipkart.yak.models.ColumnsMap;
import com.flipkart.yak.models.GetByIndexBuilder;
import com.flipkart.yak.models.GetCellByIndex;
import com.flipkart.yak.models.GetColumnsMapByIndex;
import com.flipkart.yak.models.IndexConstants;
import com.flipkart.yak.models.SimpleIndexAttributes;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.Matchers.argThat;
import static org.powermock.api.mockito.PowerMockito.when;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class) public class AsyncIndexGetTest extends AsyncBaseTest {
  private AsyncStoreClient storeClient;
  Map<String, KeyDistributor> keyDistributorMap = new HashMap<>();
  MetricRegistry registry = new MetricRegistry();
  int timeoutInSeconds = 30;
  SiteConfig config;

  private String rowKey1, rowKey2;
  private String cf;
  private String cq1, cq2;
  private String qv1, qv2, qv3;
  private String indexKey;

  @Before public void setUp() throws Exception {
    super.setup();
    config = buildStoreClientConfig();
    keyDistributorMap.put(FULL_TABLE_NAME, new MurmusHashDistribution(128));
    keyDistributorMap.put(FULL_TABLE_NAME_INDEX, new MurmusHashDistribution(128));
    storeClient = new AsyncStoreClientImpl(config, Optional.of(keyDistributorMap), timeoutInSeconds, registry);

    rowKey1 = "rk1-" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    rowKey2 = "rk2-" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    cf = "cf-" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    cq1 = "cq1-" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    cq2 = "cq2-" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    qv1 = "qv1-" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    qv2 = "qv2-" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    qv3 = "qv3-" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    indexKey = "id-" + RandomStringUtils.randomAlphanumeric(10).toUpperCase();
  }

  @After public void tearDown() throws Exception {
    storeClient.shutdown();
  }

  @Test public void testIndexGetColumnsMapWithSimpleAttributes()
      throws StoreException, IOException, ExecutionException, InterruptedException {

    // Index Search
    byte[] key = keyDistributorMap.get(FULL_TABLE_NAME_INDEX).enrichKey(indexKey.getBytes());
    Get indexGet = new Get(key).addColumn(IndexConstants.INDEX_CF, IndexConstants.INDEX_COL);
    Result indexResult = buildResponse(indexKey, Arrays.asList(Bytes.toString(IndexConstants.INDEX_CF)),
        Arrays.asList(Bytes.toString(IndexConstants.INDEX_COL)), Arrays.asList(rowKey1));
    when(indexTable.get(argThat(new GetMatcher(indexGet)))).thenReturn(CompletableFuture.completedFuture(indexResult));

    // Reverse Search
    Get dataGet =
        new Get(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey1.getBytes())).addFamily(cf.getBytes());
    Result dataResult =
        buildResponse(rowKey1, Arrays.asList(cf, cf), Arrays.asList(cq1, indexKey), Arrays.asList(qv1, qv3));
    when(table.get(argThat(new BatchGetMatcher(Arrays.asList(dataGet)))))
        .thenReturn(Arrays.asList(CompletableFuture.completedFuture(dataResult)));

    // Testing Method
    GetColumnsMapByIndex row =
        new GetByIndexBuilder(FULL_TABLE_NAME, cf).withIndexKey(indexKey, new SimpleIndexAttributes()).build();
    List<ColumnsMap> response = storeClient.getByIndex(row).get();

    Assert.assertTrue("Should contain 1 families", response.size() == 1);
    Assert.assertTrue("Should contain 2 columns", response.get(0).size() == 2);
    Assert.assertTrue("Should contain column: " + cq1, response.get(0).containsKey(cq1));
    Assert.assertTrue("Should contain column: " + indexKey, response.get(0).containsKey(indexKey));
    Assert
        .assertTrue("Should contain value: " + qv1, Arrays.equals(response.get(0).get(cq1).getValue(), qv1.getBytes()));
    Assert.assertTrue("Should contain value: " + qv3,
        Arrays.equals(response.get(0).get(indexKey).getValue(), qv3.getBytes()));
  }

  @Test public void testIndexGetCellsWithSimpleAttributes()
      throws StoreException, IOException, ExecutionException, InterruptedException {
    // Index Search
    byte[] key = keyDistributorMap.get(FULL_TABLE_NAME_INDEX).enrichKey(indexKey.getBytes());
    Get indexGet = new Get(key).addColumn(IndexConstants.INDEX_CF, IndexConstants.INDEX_COL);
    Result indexResult = buildResponse(indexKey, Arrays.asList(Bytes.toString(IndexConstants.INDEX_CF)),
        Arrays.asList(Bytes.toString(IndexConstants.INDEX_COL)), Arrays.asList(rowKey1));
    when(indexTable.get(argThat(new GetMatcher(indexGet)))).thenReturn(CompletableFuture.completedFuture(indexResult));

    // Reverse Search
    Get dataGet = new Get(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey1.getBytes()))
        .addColumn(cf.getBytes(), cq1.getBytes()).addColumn(cf.getBytes(), indexKey.getBytes());
    Result dataResult =
        buildResponse(rowKey1, Arrays.asList(cf, cf), Arrays.asList(cq1, indexKey), Arrays.asList(qv1, qv3));
    when(table.get(argThat(new BatchGetMatcher(Arrays.asList(dataGet)))))
        .thenReturn(Arrays.asList(CompletableFuture.completedFuture(dataResult)));

    // Testing Method
    GetCellByIndex row = new GetByIndexBuilder(FULL_TABLE_NAME, cf).withIndexKey(indexKey, new SimpleIndexAttributes())
        .buildForCloumn(cq1);
    List<Cell> response = storeClient.getByIndex(row).get();

    Assert.assertTrue("Should contain 1 cells", response.size() == 1);
    Assert.assertTrue("Should match value", Arrays.equals(response.get(0).getValue(), qv1.getBytes()));
  }

  @Test public void testIndexGetColumnsMapWithAppendOnlyAttributes()
      throws StoreException, IOException, ExecutionException, InterruptedException {

    // Index Search
    byte[] key = keyDistributorMap.get(FULL_TABLE_NAME_INDEX).enrichKey(indexKey.getBytes());
    Get indexGet = new Get(key).addFamily(IndexConstants.INDEX_CF);
    Result indexResult = buildResponse(indexKey,
        Arrays.asList(Bytes.toString(IndexConstants.INDEX_CF), Bytes.toString(IndexConstants.INDEX_CF)),
        Arrays.asList(rowKey1, rowKey2), Arrays.asList(rowKey1, rowKey2));
    when(indexTable.get(argThat(new GetMatcher(indexGet)))).thenReturn(CompletableFuture.completedFuture(indexResult));

    // Reverse Search
    Get dataGet1 =
        new Get(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey1.getBytes())).addFamily(cf.getBytes());
    Get dataGet2 =
        new Get(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey2.getBytes())).addFamily(cf.getBytes());
    Result dataResult1 =
        buildResponse(rowKey1, Arrays.asList(cf, cf), Arrays.asList(cq1, indexKey), Arrays.asList(qv1, qv3));
    Result dataResult2 =
        buildResponse(rowKey2, Arrays.asList(cf, cf), Arrays.asList(cq1, indexKey), Arrays.asList(qv2, qv3));
    when(table.get(argThat(new BatchGetMatcher(Arrays.asList(dataGet1, dataGet2))))).thenReturn(
        Arrays.asList(CompletableFuture.completedFuture(dataResult1), CompletableFuture.completedFuture(dataResult2)));
    when(table.get(argThat(new BatchGetMatcher(Arrays.asList(dataGet2, dataGet1))))).thenReturn(
        Arrays.asList(CompletableFuture.completedFuture(dataResult1), CompletableFuture.completedFuture(dataResult2)));

    // Testing Method
    GetColumnsMapByIndex row =
        new GetByIndexBuilder(FULL_TABLE_NAME, cf).withIndexKey(indexKey, new AppendOnlyIndexAttributes(0, 2)).build();
    List<ColumnsMap> response = storeClient.getByIndex(row).get();

    Assert.assertTrue("Should contain 1 families", response.size() == 2);
    Assert.assertTrue("Should contain 2 columns", response.get(0).size() == 2);
    Assert.assertTrue("Should contain column: " + cq1, response.get(0).containsKey(cq1));
    Assert.assertTrue("Should contain column: " + indexKey, response.get(0).containsKey(indexKey));
    Assert
        .assertTrue("Should contain value: " + qv1, Arrays.equals(response.get(0).get(cq1).getValue(), qv1.getBytes()));
    Assert.assertTrue("Should contain value: " + qv3,
        Arrays.equals(response.get(0).get(indexKey).getValue(), qv3.getBytes()));
  }

  @Test public void testIndexGetCellsWithAppendOnlyAttributes()
      throws StoreException, IOException, ExecutionException, InterruptedException {

    byte[] key = keyDistributorMap.get(FULL_TABLE_NAME_INDEX).enrichKey(indexKey.getBytes());
    Get indexGet = new Get(key).addFamily(IndexConstants.INDEX_CF);
    Result indexResult = buildResponse(indexKey,
        Arrays.asList(Bytes.toString(IndexConstants.INDEX_CF), Bytes.toString(IndexConstants.INDEX_CF)),
        Arrays.asList(rowKey1, rowKey2), Arrays.asList(rowKey1, rowKey2));
    when(indexTable.get(argThat(new GetMatcher(indexGet)))).thenReturn(CompletableFuture.completedFuture(indexResult));

    // Reverse Search
    Get dataGet1 = new Get(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey1.getBytes()))
        .addColumn(cf.getBytes(), cq1.getBytes()).addColumn(cf.getBytes(), indexKey.getBytes());
    Get dataGet2 = new Get(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey2.getBytes()))
        .addColumn(cf.getBytes(), cq1.getBytes()).addColumn(cf.getBytes(), indexKey.getBytes());
    Result dataResult1 =
        buildResponse(rowKey1, Arrays.asList(cf, cf), Arrays.asList(cq1, indexKey), Arrays.asList(qv1, qv3));
    Result dataResult2 =
        buildResponse(rowKey2, Arrays.asList(cf, cf), Arrays.asList(cq1, indexKey), Arrays.asList(qv2, qv3));
    when(table.get(argThat(new BatchGetMatcher(Arrays.asList(dataGet1, dataGet2))))).thenReturn(
        Arrays.asList(CompletableFuture.completedFuture(dataResult1), CompletableFuture.completedFuture(dataResult2)));
    when(table.get(argThat(new BatchGetMatcher(Arrays.asList(dataGet2, dataGet1))))).thenReturn(
        Arrays.asList(CompletableFuture.completedFuture(dataResult1), CompletableFuture.completedFuture(dataResult2)));

    // Testing Method
    GetCellByIndex row =
        new GetByIndexBuilder(FULL_TABLE_NAME, cf).withIndexKey(indexKey, new AppendOnlyIndexAttributes(0, 2))
            .buildForCloumn(cq1);
    List<Cell> response = storeClient.getByIndex(row).get();

    Assert.assertTrue("Should contain 2 cells", response.size() == 2);
    Assert.assertTrue("Should match value", Arrays.equals(response.get(0).getValue(), qv1.getBytes()));
    Assert.assertTrue("Should match value", Arrays.equals(response.get(1).getValue(), qv2.getBytes()));
  }

  @Test public void testIndexGetCellsWithAppendOnlyAttributesPagination()
      throws StoreException, IOException, ExecutionException, InterruptedException {

    byte[] key = keyDistributorMap.get(FULL_TABLE_NAME_INDEX).enrichKey(indexKey.getBytes());
    Get indexGet = new Get(key).addFamily(IndexConstants.INDEX_CF);
    Result indexResult = buildResponse(indexKey,
        Arrays.asList(Bytes.toString(IndexConstants.INDEX_CF), Bytes.toString(IndexConstants.INDEX_CF)),
        Arrays.asList(rowKey1, rowKey2), Arrays.asList(rowKey1, rowKey2));
    when(indexTable.get(argThat(new GetMatcher(indexGet)))).thenReturn(CompletableFuture.completedFuture(indexResult));

    // Reverse Search
    Get dataGet1 = new Get(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey1.getBytes()))
        .addColumn(cf.getBytes(), cq1.getBytes()).addColumn(cf.getBytes(), indexKey.getBytes());
    Get dataGet2 = new Get(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey2.getBytes()))
        .addColumn(cf.getBytes(), cq1.getBytes()).addColumn(cf.getBytes(), indexKey.getBytes());
    Result dataResult1 =
        buildResponse(rowKey1, Arrays.asList(cf, cf), Arrays.asList(cq1, indexKey), Arrays.asList(qv1, qv3));
    Result dataResult2 =
        buildResponse(rowKey2, Arrays.asList(cf, cf), Arrays.asList(cq1, indexKey), Arrays.asList(qv2, qv3));
    when(table.get(argThat(new BatchGetMatcher(Arrays.asList(dataGet1)))))
        .thenReturn(Arrays.asList(CompletableFuture.completedFuture(dataResult1)));
    when(table.get(argThat(new BatchGetMatcher(Arrays.asList(dataGet2)))))
        .thenReturn(Arrays.asList(CompletableFuture.completedFuture(dataResult2)));

    // Testing Method
    GetCellByIndex row =
        new GetByIndexBuilder(FULL_TABLE_NAME, cf).withIndexKey(indexKey, new AppendOnlyIndexAttributes(0, 1))
            .buildForCloumn(cq1);
    List<Cell> response = storeClient.getByIndex(row).get();

    Assert.assertTrue("Should contain 1 cells", response.size() == 1);
    Assert.assertTrue("Should match value", (
        Arrays.equals(response.get(0).getValue(), qv2.getBytes()) || Arrays.equals(response.get(0).getValue(),
            qv1.getBytes()))); // This is non-deterministic. Leaving for backward compatible
  }

  @Test public void testIndexGetCellsWithExceptionInIndexGet()
      throws StoreException, IOException, InterruptedException {

    String errorMessage = "Failed to get index";
    byte[] key = keyDistributorMap.get(FULL_TABLE_NAME_INDEX).enrichKey(indexKey.getBytes());
    Get indexGet = new Get(key).addFamily(IndexConstants.INDEX_CF);
    CompletableFuture<Result> future = new CompletableFuture<>();
    future.completeExceptionally(new StoreException(errorMessage));
    when(indexTable.get(argThat(new GetMatcher(indexGet)))).thenReturn(future);

    GetCellByIndex row =
        new GetByIndexBuilder(FULL_TABLE_NAME, cf).withIndexKey(indexKey, new AppendOnlyIndexAttributes(0, 2))
            .buildForCloumn(cq1);
    try {
      storeClient.getByIndex(row).get();
      Assert.assertTrue("Should have fauiled with exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof StoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }

  @Test public void testIndexGetCellsWithExceptionInDataGet() throws StoreException, IOException, InterruptedException {
    // Index Search
    byte[] key = keyDistributorMap.get(FULL_TABLE_NAME_INDEX).enrichKey(indexKey.getBytes());
    Get indexGet = new Get(key).addColumn(IndexConstants.INDEX_CF, IndexConstants.INDEX_COL);
    Result indexResult = buildResponse(indexKey, Arrays.asList(Bytes.toString(IndexConstants.INDEX_CF)),
        Arrays.asList(Bytes.toString(IndexConstants.INDEX_COL)), Arrays.asList(rowKey1));
    when(indexTable.get(argThat(new GetMatcher(indexGet)))).thenReturn(CompletableFuture.completedFuture(indexResult));

    // Reverse Search
    String errorMessage = "Failed to perform data get";
    Get dataGet =
        new Get(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey1.getBytes())).addFamily(cf.getBytes());
    CompletableFuture<Result> future = new CompletableFuture<>();
    future.completeExceptionally(new StoreException(errorMessage));
    when(table.get(argThat(new BatchGetMatcher(Arrays.asList(dataGet))))).thenReturn(Arrays.asList(future));

    // Testing Method
    GetColumnsMapByIndex row =
        new GetByIndexBuilder(FULL_TABLE_NAME, cf).withIndexKey(indexKey, new SimpleIndexAttributes()).build();
    try {
      storeClient.getByIndex(row).get();
      Assert.assertTrue("Should have fauiled with exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof StoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }

  @Test public void testIndexGetCellsWithNoDataFoundException()
      throws StoreException, IOException, InterruptedException {
    // Index Search
    byte[] key = keyDistributorMap.get(FULL_TABLE_NAME_INDEX).enrichKey(indexKey.getBytes());
    Get indexGet = new Get(key).addColumn(IndexConstants.INDEX_CF, IndexConstants.INDEX_COL);
    Result indexResult = buildResponse(indexKey, Arrays.asList(Bytes.toString(IndexConstants.INDEX_CF)),
        Arrays.asList(Bytes.toString(IndexConstants.INDEX_COL)), Arrays.asList(rowKey1));
    when(indexTable.get(argThat(new GetMatcher(indexGet)))).thenReturn(CompletableFuture.completedFuture(indexResult));

    // Reverse Search
    String errorMessage = "Data Not Found";
    Get dataGet =
        new Get(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey1.getBytes())).addFamily(cf.getBytes());
    Result dataResult = buildResponse(rowKey1, Arrays.asList(), Arrays.asList(), Arrays.asList());
    when(table.get(argThat(new BatchGetMatcher(Arrays.asList(dataGet)))))
        .thenReturn(Arrays.asList(CompletableFuture.completedFuture(dataResult)));

    // Testing Method
    GetColumnsMapByIndex row =
        new GetByIndexBuilder(FULL_TABLE_NAME, cf).withIndexKey(indexKey, new SimpleIndexAttributes()).build();
    try {
      storeClient.getByIndex(row).get();
      Assert.assertTrue("Should have fauiled with exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof StoreDataNotFoundException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }
}
