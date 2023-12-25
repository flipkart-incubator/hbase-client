package com.flipkart.yak.client;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.mocks.MockCheckAndMutateBuilderImpl;
import com.flipkart.yak.client.exceptions.PayloadValidatorException;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.distributor.KeyDistributor;
import com.flipkart.yak.distributor.MurmusHashDistribution;
import com.flipkart.yak.models.CheckAndStoreData;
import com.flipkart.yak.models.IndexType;
import com.flipkart.yak.models.StoreData;
import com.flipkart.yak.models.StoreDataBuilder;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Put;
import org.junit.After;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.powermock.api.mockito.PowerMockito.when;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class) public class AsyncCasTest extends AsyncBaseTest {
  private AsyncStoreClient storeClient;
  Map<String, KeyDistributor> keyDistributorMap = new HashMap<>();
  MetricRegistry registry = new MetricRegistry();
  int timeoutInSeconds = 30;
  SiteConfig config;

  @Before public void setUp() throws Exception {
    super.setup();
    config = buildStoreClientConfig();
    keyDistributorMap.put(FULL_TABLE_NAME, new MurmusHashDistribution(128));
    storeClient = new AsyncStoreClientImpl(config, Optional.of(keyDistributorMap), timeoutInSeconds, registry);
  }

  @After public void tearDown() throws Exception {
    storeClient.shutdown();
  }

  private class PutMatcher extends ArgumentMatcher<Put> {
    private Put left;

    public PutMatcher(Put left) {
      this.left = left;
    }

    @Override public boolean matches(Object argument) {
      Put right = (Put) argument;
      return Arrays.equals(right.getRow(), left.getRow()) && right.getFamilyCellMap().equals(left.getFamilyCellMap())
          && right.getDurability().equals(left.getDurability());
    }
  }

  @Test public void testCas() throws StoreException, IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes()).build();

    AsyncStoreClientUtis.StorePuts puts =
        AsyncStoreClientUtis.buildStorePuts(storeData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    when(table.put(argThat(new PutMatcher(puts.entityPut)))).thenReturn(CompletableFuture.completedFuture(null));
    storeClient.put(storeData).get();

    CheckAndStoreData checkAndStoreData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, "2".getBytes())
        .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, qualifierValue.getBytes(), CompareOperator.EQUAL);

    when(table
        .checkAndMutate(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes()), columnFamily.getBytes()))
        .thenReturn(new MockCheckAndMutateBuilderImpl(true, null));
    Boolean success = storeClient.checkAndPut(checkAndStoreData).get();
    assertTrue("Cas operation should have been successful", success);
  }

  @Test public void testCasWithComparatorFailed()
      throws StoreException, IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes()).build();

    AsyncStoreClientUtis.StorePuts puts =
        AsyncStoreClientUtis.buildStorePuts(storeData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    when(table.put(argThat(new PutMatcher(puts.entityPut)))).thenReturn(CompletableFuture.completedFuture(null));
    storeClient.put(storeData).get();

    CheckAndStoreData checkAndStoreData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, "2".getBytes())
        .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, qualifierValue.getBytes(), CompareOperator.EQUAL);

    when(table
        .checkAndMutate(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes()), columnFamily.getBytes()))
        .thenReturn(new MockCheckAndMutateBuilderImpl(false, null));
    Boolean success = storeClient.checkAndPut(checkAndStoreData).get();
    assertTrue("Cas operation should have been failed", !success);
  }

  @Test public void testCasWithNewInsert()
      throws StoreException, IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    CheckAndStoreData checkAndStoreData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes())
        .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, null, CompareOperator.EQUAL);

    when(table
        .checkAndMutate(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes()), columnFamily.getBytes()))
        .thenReturn(new MockCheckAndMutateBuilderImpl(true, null));
    Boolean success = storeClient.checkAndPut(checkAndStoreData).get();
    assertTrue("Cas operation should have been successful", success);
  }

  @Test public void testCasWithPartitionKey()
          throws StoreException, IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String partitionKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    CheckAndStoreData checkAndStoreData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
            .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes()).withPartitionKey(partitionKey.getBytes())
            .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, null, CompareOperator.EQUAL);

    when(table
            .checkAndMutate(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes(), partitionKey.getBytes()), columnFamily.getBytes()))
            .thenReturn(new MockCheckAndMutateBuilderImpl(true, null));
    Boolean success = storeClient.checkAndPut(checkAndStoreData).get();
    assertTrue("Cas operation should have been successful", success);
  }

  @Test public void testCasWithIndexPut() throws StoreException, IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String indexKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes()).build();

    AsyncStoreClientUtis.StorePuts puts =
        AsyncStoreClientUtis.buildStorePuts(storeData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    when(table.put(argThat(new PutMatcher(puts.entityPut)))).thenReturn(CompletableFuture.completedFuture(null));
    storeClient.put(storeData).get();

    CheckAndStoreData checkAndStoreData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, "2".getBytes()).addIndex(columnFamily, indexKey, IndexType.SIMPLE)
        .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, qualifierValue.getBytes(), CompareOperator.EQUAL);

    when(indexTable.put(any(List.class))).thenReturn(Arrays.asList(CompletableFuture.completedFuture(null)));
    when(table
        .checkAndMutate(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes()), columnFamily.getBytes()))
        .thenReturn(new MockCheckAndMutateBuilderImpl(true, null));
    Boolean success = storeClient.checkAndPut(checkAndStoreData).get();
    assertTrue("Cas operation should have been successful", success);
  }

  @Test public void testCasWithIndexPutFailedWithException()
      throws StoreException, IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String indexKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String errorMessage = "Failed index put";

    CheckAndStoreData checkAndStoreData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, "2".getBytes()).addIndex(columnFamily, indexKey, IndexType.SIMPLE)
        .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, qualifierValue.getBytes(), CompareOperator.EQUAL);

    CompletableFuture<Void> future = new CompletableFuture<>();
    future.completeExceptionally(new StoreException(errorMessage));
    when(indexTable.put(any(List.class))).thenReturn(Arrays.asList(future));
    try {
      when(table
          .checkAndMutate(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes()), columnFamily.getBytes()))
          .thenReturn(new MockCheckAndMutateBuilderImpl(true, null));

      storeClient.checkAndPut(checkAndStoreData).get();
      assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof StoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }

  @Test public void testCasWithOutKeyDistributer()
      throws StoreException, IOException, ExecutionException, InterruptedException, TimeoutException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    storeClient = new AsyncStoreClientImpl(config, Optional.empty(), timeoutInSeconds, registry);

    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes()).build();

    AsyncStoreClientUtis.StorePuts puts =
        AsyncStoreClientUtis.buildStorePuts(storeData, new HashMap<>(), Optional.of(config.getDurabilityThreshold()));
    when(table.put(argThat(new PutMatcher(puts.entityPut)))).thenReturn(CompletableFuture.completedFuture(null));
    storeClient.put(storeData).get();

    CheckAndStoreData checkAndStoreData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, "2".getBytes())
        .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, qualifierValue.getBytes(), CompareOperator.EQUAL);

    when(table.checkAndMutate(rowKey.getBytes(), columnFamily.getBytes()))
        .thenReturn(new MockCheckAndMutateBuilderImpl(true, null));
    Boolean success = storeClient.checkAndPut(checkAndStoreData).get();
    assertTrue("Cas operation should have been successful", success);
  }

  @Test public void testCasWithException() throws InterruptedException, ExecutionException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String errorMessage = "Failed to put";

    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes()).build();

    AsyncStoreClientUtis.StorePuts puts =
        AsyncStoreClientUtis.buildStorePuts(storeData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    when(table.put(argThat(new PutMatcher(puts.entityPut)))).thenReturn(CompletableFuture.completedFuture(null));
    storeClient.put(storeData).get();

    CheckAndStoreData checkAndStoreData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, "2".getBytes())
        .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, qualifierValue.getBytes(), CompareOperator.EQUAL);

    when(table
        .checkAndMutate(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes()), columnFamily.getBytes()))
        .thenReturn(new MockCheckAndMutateBuilderImpl(null, new StoreException(errorMessage)));
    try {
      storeClient.checkAndPut(checkAndStoreData).get();
      assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof StoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }

  @Test public void testCasWithPayloadValidationFailed() throws StoreException, IOException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String errorMessage = "Payload size breach threshod 1, got 4";

    CheckAndStoreData checkAndStoreData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, "2000".getBytes())
        .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, qualifierValue.getBytes(), CompareOperator.EQUAL);

    when(namespaceDescriptor.getConfigurationValue(PAYLOAD_THREASHOLD)).thenReturn("1");
    when(admin.getNamespaceDescriptor(anyString())).thenReturn(CompletableFuture.completedFuture(namespaceDescriptor));
    when(table
            .checkAndMutate(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes()), columnFamily.getBytes()))
            .thenReturn(new MockCheckAndMutateBuilderImpl(true, null));

    try {
      storeClient.checkAndPut(checkAndStoreData).get();
      assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof PayloadValidatorException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }
}
