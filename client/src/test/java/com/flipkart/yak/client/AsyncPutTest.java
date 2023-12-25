package com.flipkart.yak.client;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.exceptions.PayloadValidatorException;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.distributor.KeyDistributor;
import com.flipkart.yak.distributor.MurmusHashDistribution;
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
import org.apache.hadoop.hbase.client.Put;
import org.junit.After;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.powermock.api.mockito.PowerMockito.when;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class) public class AsyncPutTest extends AsyncBaseTest {

  private AsyncStoreClient storeClient;
  Map<String, KeyDistributor> keyDistributorMap = new HashMap<>();
  MetricRegistry registry = new MetricRegistry();
  int timeoutInSeconds = 30;
  SiteConfig config;

  private String rowKey;
  private String columnFamily;
  private String columnQualifier;
  private String qualifierValue;
  private String indexKey;
  private String partitionKey;

  @Before public void setUp() throws Exception {
    super.setup();
    config = buildStoreClientConfig();
    keyDistributorMap.put(FULL_TABLE_NAME, new MurmusHashDistribution(128));
    storeClient = new AsyncStoreClientImpl(config, Optional.of(keyDistributorMap), timeoutInSeconds, registry);

    rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    indexKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    partitionKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
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

  @Test public void testPut() throws StoreException, IOException, ExecutionException, InterruptedException {
    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes()).build();

    AsyncStoreClientUtis.StorePuts puts =
        AsyncStoreClientUtis.buildStorePuts(storeData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    when(table.put(argThat(new PutMatcher(puts.entityPut)))).thenReturn(CompletableFuture.completedFuture(null));
    storeClient.put(storeData).get();
  }

  @Test public void testPutWithPartitionKey() throws StoreException, IOException, ExecutionException, InterruptedException {
    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes()).withPartitionKey(partitionKey.getBytes())
            .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes()).build();

    AsyncStoreClientUtis.StorePuts puts =
            AsyncStoreClientUtis.buildStorePuts(storeData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    when(table.put(argThat(new PutMatcher(puts.entityPut)))).thenReturn(CompletableFuture.completedFuture(null));
    storeClient.put(storeData).get();
  }

  @Test public void testPutWithSimpleIndexPut()
      throws StoreException, IOException, ExecutionException, InterruptedException {

    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes())
        .addIndex(columnFamily, indexKey, IndexType.APPEND_ONLY).build();

    AsyncStoreClientUtis.StorePuts puts =
        AsyncStoreClientUtis.buildStorePuts(storeData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    when(table.put(argThat(new PutMatcher(puts.entityPut)))).thenReturn(CompletableFuture.completedFuture(null));
    when(indexTable.put(puts.indexPuts)).thenReturn(Arrays.asList(CompletableFuture.completedFuture(null)));
    storeClient.put(storeData).get();
  }

  @Test public void testPutWithSimpleIndexPutFailedWithException()
      throws StoreException, IOException, ExecutionException, InterruptedException {
    String errorMessage = "Failed index put";

    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes())
        .addIndex(columnFamily, indexKey, IndexType.APPEND_ONLY).build();

    CompletableFuture<Void> future = new CompletableFuture<>();
    future.completeExceptionally(new StoreException(errorMessage));

    AsyncStoreClientUtis.StorePuts puts =
        AsyncStoreClientUtis.buildStorePuts(storeData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    when(table.put(argThat(new PutMatcher(puts.entityPut)))).thenReturn(CompletableFuture.completedFuture(null));
    when(indexTable.put(any(List.class))).thenReturn(Arrays.asList(future));
    try {
      storeClient.put(storeData).get();
      assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof StoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }

  @Test public void testPutWithAppendOnlyIndexPut()
      throws StoreException, IOException, ExecutionException, InterruptedException {
    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes())
        .addIndex(columnFamily, indexKey, IndexType.SIMPLE).build();

    AsyncStoreClientUtis.StorePuts puts =
        AsyncStoreClientUtis.buildStorePuts(storeData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    when(table.put(argThat(new PutMatcher(puts.entityPut)))).thenReturn(CompletableFuture.completedFuture(null));
    storeClient.put(storeData).get();
  }

  @Test public void testPutWithAppendOnlyIndexPutFailedWithException()
      throws StoreException, IOException, ExecutionException, InterruptedException {
    String errorMessage = "Failed index put";

    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes())
        .addIndex(columnFamily, indexKey, IndexType.SIMPLE).build();

    CompletableFuture<Void> future = new CompletableFuture<>();
    future.completeExceptionally(new StoreException(errorMessage));

    AsyncStoreClientUtis.StorePuts puts =
        AsyncStoreClientUtis.buildStorePuts(storeData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    when(table.put(argThat(new PutMatcher(puts.entityPut)))).thenReturn(CompletableFuture.completedFuture(null));
    when(indexTable.put(any(List.class))).thenReturn(Arrays.asList(future));
    try {
      storeClient.put(storeData).get();
      assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof StoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }

  @Test public void testPutWithOutKeyDistributer()
      throws StoreException, IOException, ExecutionException, InterruptedException, TimeoutException {
    storeClient = new AsyncStoreClientImpl(config, Optional.empty(), timeoutInSeconds, registry);

    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes()).build();

    AsyncStoreClientUtis.StorePuts puts =
        AsyncStoreClientUtis.buildStorePuts(storeData, new HashMap<>(), Optional.of(config.getDurabilityThreshold()));
    when(table.put(argThat(new PutMatcher(puts.entityPut)))).thenReturn(CompletableFuture.completedFuture(null));
    storeClient.put(storeData).get();
  }

  @Test public void testPutWithException() throws InterruptedException {
    String errorMessage = "Failed to put";

    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes()).build();

    CompletableFuture<Void> future = new CompletableFuture<>();
    future.completeExceptionally(new StoreException(errorMessage));
    AsyncStoreClientUtis.StorePuts puts =
        AsyncStoreClientUtis.buildStorePuts(storeData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    when(table.put(argThat(new PutMatcher(puts.entityPut)))).thenReturn(future);
    try {
      storeClient.put(storeData).get();
      assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof StoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }

  @Test public void testPutWithPayloadValidationFailed() throws StoreException, IOException, InterruptedException {
    String errorMessage = "Payload size breach threshod 1, got 10";

    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes()).build();

    when(namespaceDescriptor.getConfigurationValue(PAYLOAD_THREASHOLD)).thenReturn("1");
    when(admin.getNamespaceDescriptor(NAMESPACE_NAME))
        .thenReturn(CompletableFuture.completedFuture(namespaceDescriptor));

    AsyncStoreClientUtis.StorePuts puts =
        AsyncStoreClientUtis.buildStorePuts(storeData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    when(table.put(argThat(new PutMatcher(puts.entityPut)))).thenReturn(CompletableFuture.completedFuture(null));
    try {
      storeClient.put(storeData).get();
      assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof PayloadValidatorException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }
}
