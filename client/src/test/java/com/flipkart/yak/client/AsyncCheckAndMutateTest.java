package com.flipkart.yak.client;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.powermock.api.mockito.PowerMockito.when;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.exceptions.PayloadValidatorException;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.client.mocks.MockCheckAndMutateBuilderImpl;
import com.flipkart.yak.distributor.KeyDistributor;
import com.flipkart.yak.distributor.MurmusHashDistribution;
import com.flipkart.yak.models.CheckAndMutateData;
import com.flipkart.yak.models.DeleteData;
import com.flipkart.yak.models.DeleteDataBuilder;
import com.flipkart.yak.models.IndexType;
import com.flipkart.yak.models.MutateData;
import com.flipkart.yak.models.MutateDataBuilder;
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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.RowMutations;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class) public class AsyncCheckAndMutateTest extends AsyncBaseTest{
  private AsyncStoreClient storeClient;
  Map<String, KeyDistributor> keyDistributorMap = new HashMap<>();
  MetricRegistry registry = new MetricRegistry();
  int timeoutInSeconds = 30;
  SiteConfig config;

  @Before
  public void setUp() throws Exception {
    super.setup();
    config = buildStoreClientConfig();
    keyDistributorMap.put(FULL_TABLE_NAME, new MurmusHashDistribution(128));
    storeClient = new AsyncStoreClientImpl(config, Optional.of(keyDistributorMap), timeoutInSeconds, registry);
  }

  @After
  public void tearDown() throws Exception {
    storeClient.shutdown();
  }

  private class MutationMatcher extends ArgumentMatcher<RowMutations> {
    private RowMutations left;

    public MutationMatcher(RowMutations left) {
      this.left = left;
    }

    @Override public boolean matches(Object argument) {
      RowMutations right = (RowMutations) argument;
      return Arrays.equals(right.getRow(), left.getRow());
    }
  }

  @Test
  public void testCheckAndMutateWithNewInsert() throws ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String shortTTLColumnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String longTTLColumnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    long shortTTL = 5000;
    long longTTL = 100000;

    StoreData shortTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, shortTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(shortTTL).build();

    StoreData longTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, longTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(longTTL).build();

    CheckAndMutateData checkAndMutateData = new MutateDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addStoreData(shortTTLData).addStoreData(longTTLData)
        .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, null, CompareOperator.EQUAL);

    when(table
        .checkAndMutate(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes()), columnFamily.getBytes()))
        .thenReturn(new MockCheckAndMutateBuilderImpl(true, null));
    Boolean success = storeClient.checkAndMutate(checkAndMutateData).get();
    assertTrue("CheckAndMutate operation should have been successful", success);
  }

  @Test
  public void testCheckAndMutate() throws IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String shortTTLColumnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String longTTLColumnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    long shortTTL = 5000;
    long longTTL = 100000;

    StoreData shortTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, shortTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(shortTTL).build();

    StoreData longTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, longTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(longTTL).build();

    MutateData mutateData = new MutateDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes()).
        addStoreData(shortTTLData).addStoreData(longTTLData).build();
    AsyncStoreClientUtis.Mutation mutation =
        AsyncStoreClientUtis.buildMutation(mutateData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    when(table.mutateRow(argThat(new MutationMatcher(mutation.rowMutations)))).thenReturn(
        CompletableFuture.completedFuture(null));
    storeClient.mutate(mutateData).get();

    StoreData modifiedShortTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, shortTTLColumnQualifier, "newShortTTLData".getBytes()).withTTLInMilliseconds(shortTTL).build();

    StoreData modifiedLongTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, longTTLColumnQualifier, "newLongTTLData".getBytes()).withTTLInMilliseconds(longTTL).build();


    CheckAndMutateData checkAndMutateData = new MutateDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addStoreData(modifiedShortTTLData).addStoreData(modifiedLongTTLData)
        .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, qualifierValue.getBytes(), CompareOperator.EQUAL);

    when(table
        .checkAndMutate(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes()), columnFamily.getBytes()))
        .thenReturn(new MockCheckAndMutateBuilderImpl(true, null));
    Boolean success = storeClient.checkAndMutate(checkAndMutateData).get();
    assertTrue("CheckAndMutate operation should have been successful", success);
  }

  @Test
  public void testCheckAndMutateWithDelete() throws IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String shortTTLColumnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String longTTLColumnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    long shortTTL = 5000;
    long longTTL = 100000;

    StoreData shortTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
            .addColumn(columnFamily, shortTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(shortTTL).build();

    StoreData longTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
            .addColumn(columnFamily, longTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(longTTL).build();
    MutateData mutateData = new MutateDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes()).
            addStoreData(shortTTLData).addStoreData(longTTLData).build();
    AsyncStoreClientUtis.Mutation mutation =
            AsyncStoreClientUtis.buildMutation(mutateData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    when(table.mutateRow(argThat(new MutationMatcher(mutation.rowMutations)))).thenReturn(
            CompletableFuture.completedFuture(null));
    storeClient.mutate(mutateData).get();

    StoreData modifiedShortTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
            .addColumn(columnFamily, shortTTLColumnQualifier, "newShortTTLData".getBytes()).withTTLInMilliseconds(shortTTL).build();

    StoreData modifiedLongTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
            .addColumn(columnFamily, longTTLColumnQualifier, "newLongTTLData".getBytes()).withTTLInMilliseconds(longTTL).build();


    CheckAndMutateData checkAndMutateData = new MutateDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
            .addStoreData(modifiedShortTTLData).addStoreData(modifiedLongTTLData)
            .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, qualifierValue.getBytes(), CompareOperator.EQUAL);

    when(table
            .checkAndMutate(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes()), columnFamily.getBytes()))
            .thenReturn(new MockCheckAndMutateBuilderImpl(true, null));
    storeClient.checkAndMutate(checkAndMutateData).get();
    DeleteData deleteData = new DeleteDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
            .addColumn(columnFamily, longTTLColumnQualifier).build();
    CheckAndMutateData checkAndDeleteData = new MutateDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
            .addStoreData(modifiedShortTTLData).addStoreData(modifiedLongTTLData)
            .addDeleteData(deleteData)
            .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, qualifierValue.getBytes(), CompareOperator.EQUAL);
    Boolean success = storeClient.checkAndMutate(checkAndDeleteData).get();
    assertTrue("CheckAndMutate operation should have been successful", success);
  }

  @Test
  public void testCheckAndMutateWithComparisonFailed() throws IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String shortTTLColumnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String longTTLColumnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    long shortTTL = 5000;
    long longTTL = 100000;

    StoreData shortTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, shortTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(shortTTL).build();

    StoreData longTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, longTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(longTTL).build();

    MutateData mutateData = new MutateDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes()).
        addStoreData(shortTTLData).addStoreData(longTTLData).build();
    AsyncStoreClientUtis.Mutation mutation =
        AsyncStoreClientUtis.buildMutation(mutateData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    when(table.mutateRow(argThat(new MutationMatcher(mutation.rowMutations)))).thenReturn(
        CompletableFuture.completedFuture(null));
    storeClient.mutate(mutateData).get();

    StoreData modifiedShortTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, shortTTLColumnQualifier, "newShortTTLData".getBytes()).withTTLInMilliseconds(shortTTL).build();

    StoreData modifiedLongTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, longTTLColumnQualifier, "newLongTTLData".getBytes()).withTTLInMilliseconds(longTTL).build();
    DeleteData deleteData = new DeleteDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
            .addColumn(columnFamily, longTTLColumnQualifier).build();

    CheckAndMutateData checkAndMutateData = new MutateDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addStoreData(modifiedShortTTLData).addStoreData(modifiedLongTTLData).addDeleteData(deleteData)
        .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, qualifierValue.getBytes(), CompareOperator.EQUAL);

    when(table
        .checkAndMutate(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes()), columnFamily.getBytes()))
        .thenReturn(new MockCheckAndMutateBuilderImpl(false, null));
    Boolean success = storeClient.checkAndMutate(checkAndMutateData).get();
    assertTrue("CheckAndMutate operation should have been failed", !success);
  }

  @Test
  public void testCheckAndMutateWithException() throws IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String shortTTLColumnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String longTTLColumnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    long shortTTL = 5000;
    long longTTL = 100000;
    String errorMessage = "Failed to check and put";

    StoreData shortTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, shortTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(shortTTL).build();

    StoreData longTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, longTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(longTTL).build();

    CheckAndMutateData checkAndMutateData = new MutateDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addStoreData(shortTTLData).addStoreData(longTTLData)
        .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, null, CompareOperator.EQUAL);

    when(table
        .checkAndMutate(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes()), columnFamily.getBytes()))
        .thenReturn(new MockCheckAndMutateBuilderImpl(null, new StoreException(errorMessage)));
    try {
      storeClient.checkAndMutate(checkAndMutateData).get();
      assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof StoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }

  @Test
  public void testCheckAndMutateWithPayloadException() throws IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String shortTTLColumnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    long shortTTL = 5000;
    String errorMessage = "Payload size breach threshod 1, got 5";

    StoreData shortTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, shortTTLColumnQualifier, "short".getBytes()).withTTLInMilliseconds(shortTTL).build();

    CheckAndMutateData checkAndMutateData = new MutateDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addStoreData(shortTTLData).buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, null, CompareOperator.EQUAL);

    when(table
        .checkAndMutate(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes()), columnFamily.getBytes()))
        .thenReturn(new MockCheckAndMutateBuilderImpl(true, null));
    when(namespaceDescriptor.getConfigurationValue(PAYLOAD_THREASHOLD)).thenReturn("1");
    when(admin.getNamespaceDescriptor(anyString())).thenReturn(CompletableFuture.completedFuture(namespaceDescriptor));
    try {
      storeClient.checkAndMutate(checkAndMutateData).get();
      assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof PayloadValidatorException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }

  @Test
  public void testCheckAndMutateWithIndexPut() throws IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String shortTTLColumnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String longTTLColumnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    long shortTTL = 5000;
    long longTTL = 100000;
    String shortTTLIndexKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String longTTLIndexKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    StoreData shortTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, shortTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(shortTTL)
        .addIndex(columnFamily, shortTTLIndexKey, IndexType.SIMPLE).build();

    StoreData longTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, longTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(longTTL).
        addIndex(columnFamily, longTTLIndexKey, IndexType.SIMPLE).build();

    CheckAndMutateData checkAndMutateData = new MutateDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addStoreData(shortTTLData).addStoreData(longTTLData)
        .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, null, CompareOperator.EQUAL);

    when(indexTable.put(any(List.class))).thenReturn(Arrays.asList(CompletableFuture.completedFuture(null)));
    when(table
        .checkAndMutate(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes()), columnFamily.getBytes()))
        .thenReturn(new MockCheckAndMutateBuilderImpl(true, null));
    Boolean success = storeClient.checkAndMutate(checkAndMutateData).get();
    assertTrue("CheckAndMutate operation should have been successful", success);
  }

  @Test
  public void testCheckAndMutateWithIndexException() throws IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String shortTTLColumnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String longTTLColumnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    long shortTTL = 5000;
    long longTTL = 100000;
    String shortTTLIndexKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String longTTLIndexKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String errorMessage = "Failed index put";

    StoreData shortTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, shortTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(shortTTL)
        .addIndex(columnFamily, shortTTLIndexKey, IndexType.SIMPLE).build();

    StoreData longTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, longTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(longTTL).
        addIndex(columnFamily, longTTLIndexKey, IndexType.SIMPLE).build();

    CheckAndMutateData checkAndMutateData = new MutateDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addStoreData(shortTTLData).addStoreData(longTTLData)
        .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, null, CompareOperator.EQUAL);

    CompletableFuture<Void> future = new CompletableFuture<>();
    future.completeExceptionally(new StoreException(errorMessage));
    when(indexTable.put(any(List.class))).thenReturn(Arrays.asList(future));
    when(table
        .checkAndMutate(keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes()), columnFamily.getBytes()))
        .thenReturn(new MockCheckAndMutateBuilderImpl(true, null));
    try {
      storeClient.checkAndMutate(checkAndMutateData).get();
      assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof StoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }

  @Test
  public void testIOException() throws Exception {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String shortTTLColumnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String longTTLColumnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    long shortTTL = 5000;
    long longTTL = 100000;

    StoreData shortTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, shortTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(shortTTL).build();

    StoreData longTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey("r".getBytes())
        .addColumn(columnFamily, longTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(longTTL).build();

    CheckAndMutateData checkAndMutateData = new MutateDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addStoreData(shortTTLData).addStoreData(longTTLData)
        .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, null, CompareOperator.EQUAL);

    try {
      storeClient.checkAndMutate(checkAndMutateData).get();
      assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof IOException);
    }
  }
}
