package com.flipkart.yak.client;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.exceptions.RequestValidatorException;
import com.flipkart.yak.distributor.KeyDistributor;
import com.flipkart.yak.distributor.MurmusHashDistribution;
import com.flipkart.yak.models.CheckAndStoreData;
import com.flipkart.yak.models.StoreDataBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class AsyncBatchCASTest extends AsyncBaseTest {

  private AsyncStoreClient storeClient;
  private Map<String, KeyDistributor> keyDistributorMap = new HashMap<>();
  private MetricRegistry registry = new MetricRegistry();
  private int timeoutInSeconds = 30;
  private SiteConfig config;

  private String rowKey1, rowKey2;
  private String columnFamily, columnQualifier, expectedValue;
  private List<CheckAndStoreData> dataList;

  @Before public void setUp() throws Exception {
    super.setup();
    config = buildStoreClientConfig();
    keyDistributorMap.put(FULL_TABLE_NAME, new MurmusHashDistribution(128));
    storeClient = new AsyncStoreClientImpl(config, Optional.of(keyDistributorMap), timeoutInSeconds, registry);

    rowKey1 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    rowKey2 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    columnFamily = RandomStringUtils.randomAlphanumeric(6).toUpperCase();
    columnQualifier = RandomStringUtils.randomAlphanumeric(6).toUpperCase();
    expectedValue = RandomStringUtils.randomAlphanumeric(8).toUpperCase();

    CheckAndStoreData data1 = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes())
        .addColumn(columnFamily, columnQualifier, "newVal1".getBytes())
        .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, expectedValue.getBytes(), CompareOperator.EQUAL);
    CheckAndStoreData data2 = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey2.getBytes())
        .addColumn(columnFamily, columnQualifier, "newVal2".getBytes())
        .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, expectedValue.getBytes(), CompareOperator.EQUAL);
    dataList = Arrays.asList(data1, data2);
  }

  @After public void tearDown() throws Exception {
    storeClient.shutdown();
  }

  private CheckAndMutateResult mockResult(boolean success) {
    CheckAndMutateResult result = mock(CheckAndMutateResult.class);
    when(result.isSuccess()).thenReturn(success);
    return result;
  }

  @Test public void testBatchCASAllSucceed() throws ExecutionException, InterruptedException {
    CheckAndMutateResult successResult = mockResult(true);
    List<CompletableFuture<CheckAndMutateResult>> hbaseFutures = Arrays.asList(
        CompletableFuture.completedFuture(successResult),
        CompletableFuture.completedFuture(successResult));

    when(table.checkAndMutate(anyList())).thenReturn(hbaseFutures);

    List<CompletableFuture<Boolean>> futures = storeClient.checkAndPut(dataList);
    assertEquals("Should return one future per input", dataList.size(), futures.size());
    assertTrue("First CAS should succeed", futures.get(0).get());
    assertTrue("Second CAS should succeed", futures.get(1).get());
  }

  @Test public void testBatchCASPartialFalse() throws ExecutionException, InterruptedException {
    List<CompletableFuture<CheckAndMutateResult>> hbaseFutures = Arrays.asList(
        CompletableFuture.completedFuture(mockResult(true)),
        CompletableFuture.completedFuture(mockResult(false)));

    when(table.checkAndMutate(anyList())).thenReturn(hbaseFutures);

    List<CompletableFuture<Boolean>> futures = storeClient.checkAndPut(dataList);
    assertTrue("First CAS should succeed", futures.get(0).get());
    assertTrue("Second CAS should return false (check failed)", !futures.get(1).get());
  }

  @Test public void testBatchCASWithInfraException() throws InterruptedException {
    String errorMessage = "HBase RPC failure";
    CompletableFuture<CheckAndMutateResult> errorFuture = new CompletableFuture<>();
    errorFuture.completeExceptionally(new RuntimeException(errorMessage));

    when(table.checkAndMutate(anyList())).thenReturn(Arrays.asList(errorFuture, errorFuture));

    List<CompletableFuture<Boolean>> futures = storeClient.checkAndPut(dataList);
    assertEquals("Should return one future per input", dataList.size(), futures.size());

    for (CompletableFuture<Boolean> future : futures) {
      try {
        future.get();
        assertTrue("Should have thrown exception", false);
      } catch (ExecutionException ex) {
        assertNotNull("Cause should not be null", ex.getCause());
        assertTrue("Error message should match", ex.getCause().getMessage().equals(errorMessage));
      }
    }
  }

  @Test public void testBatchCASEmptyList() throws ExecutionException, InterruptedException {
    List<CompletableFuture<Boolean>> futures = storeClient.checkAndPut(Collections.emptyList());
    assertTrue("Empty input should return empty futures list", futures.isEmpty());
  }

  @Test public void testBatchCASBatchSizeValidationFailure() throws Exception {
    config = buildStoreClientConfig();
    config.withMaxBatchCheckAndPutSize(1);
    storeClient = new AsyncStoreClientImpl(config, Optional.of(keyDistributorMap), timeoutInSeconds, registry);

    String expectedMessage = "Request size should not be greater than the configured batch size 1";
    List<CompletableFuture<Boolean>> futures = storeClient.checkAndPut(dataList);
    assertEquals("Should return one future per input", dataList.size(), futures.size());

    for (CompletableFuture<Boolean> future : futures) {
      try {
        future.get();
        assertTrue("Should have failed with validation exception", false);
      } catch (ExecutionException ex) {
        assertNotNull("Cause should not be null", ex.getCause());
        assertTrue("Should be RequestValidatorException",
            ex.getCause() instanceof RequestValidatorException);
        assertEquals("Error message should match", expectedMessage, ex.getCause().getMessage());
      }
    }
  }

  @Test public void testBatchCASIfNotExists() throws ExecutionException, InterruptedException {
    CheckAndStoreData ifNotExistsData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes())
        .addColumn(columnFamily, columnQualifier, "newVal".getBytes())
        .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, null);
    List<CheckAndStoreData> ifNotExistsList = Arrays.asList(ifNotExistsData);

    CheckAndMutateResult successResult = mockResult(true);
    when(table.checkAndMutate(anyList()))
        .thenReturn(Arrays.asList(CompletableFuture.completedFuture(successResult)));

    List<CompletableFuture<Boolean>> futures = storeClient.checkAndPut(ifNotExistsList);
    assertEquals("Should return one future", 1, futures.size());
    assertTrue("ifNotExists CAS should succeed", futures.get(0).get());
  }

  @Test public void testBatchCASTableNameValidationFailure() throws Exception {
    CheckAndStoreData wrongTable = new StoreDataBuilder(FULL_TABLE_NAME + "_other")
        .withRowKey(rowKey1.getBytes())
        .addColumn(columnFamily, columnQualifier, "v".getBytes())
        .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, expectedValue.getBytes(), CompareOperator.EQUAL);
    List<CheckAndStoreData> mixedList = Arrays.asList(dataList.get(0), wrongTable);

    String expectedMessage = "Batch request should only come for one table";
    List<CompletableFuture<Boolean>> futures = storeClient.checkAndPut(mixedList);
    assertEquals("Should return one future per input", mixedList.size(), futures.size());

    for (CompletableFuture<Boolean> future : futures) {
      try {
        future.get();
        assertTrue("Should have failed with validation exception", false);
      } catch (ExecutionException ex) {
        assertNotNull("Cause should not be null", ex.getCause());
        assertTrue("Should be RequestValidatorException",
            ex.getCause() instanceof RequestValidatorException);
        assertEquals("Error message should match", expectedMessage, ex.getCause().getMessage());
      }
    }
  }
}
