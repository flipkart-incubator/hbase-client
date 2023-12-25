package com.flipkart.yak.client;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.powermock.api.mockito.PowerMockito.when;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.exceptions.PayloadValidatorException;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.distributor.KeyDistributor;
import com.flipkart.yak.distributor.MurmusHashDistribution;
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
import org.apache.hadoop.hbase.client.RowMutations;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class) public class AsyncMutateTest extends AsyncBaseTest {
  private AsyncStoreClient storeClient;
  Map<String, KeyDistributor> keyDistributorMap = new HashMap<>();
  MetricRegistry registry = new MetricRegistry();
  int timeoutInSeconds = 30;
  SiteConfig config;

  private String rowKey;
  private String columnFamily;
  private String shortTTLColumnQualifier;
  private String longTTLColumnQualifier;
  private String qualifierValue;
  private String indexKey;
  long shortTTL = 5000;
  long longTTL = 100000;

  @Before
  public void setUp() throws Exception {
    super.setup();
    config = buildStoreClientConfig();
    keyDistributorMap.put(FULL_TABLE_NAME, new MurmusHashDistribution(128));
    storeClient = new AsyncStoreClientImpl(config, Optional.of(keyDistributorMap), timeoutInSeconds, registry);

    rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    shortTTLColumnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    longTTLColumnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    indexKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
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
  public void testMutate() throws StoreException, IOException, ExecutionException, InterruptedException {
    StoreData shortTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, shortTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(shortTTL).build();

    StoreData longTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, longTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(longTTL).build();

    MutateData mutateData = new MutateDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes()).
        addStoreData(shortTTLData).addStoreData(longTTLData).build();
    AsyncStoreClientUtis.Mutation mutation =
        AsyncStoreClientUtis.buildMutation(mutateData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    when(table.mutateRow(argThat(new MutationMatcher(mutation.rowMutations)))).thenReturn(CompletableFuture.completedFuture(null));
    storeClient.mutate(mutateData).get();
  }

  @Test
  public void testMutateAndDelete() throws StoreException, IOException, ExecutionException, InterruptedException {
    StoreData shortTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
            .addColumn(columnFamily, shortTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(shortTTL).build();

    StoreData longTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
            .addColumn(columnFamily, longTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(longTTL).build();

    MutateData mutateData = new MutateDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes()).
            addStoreData(shortTTLData).addStoreData(longTTLData).build();
    AsyncStoreClientUtis.Mutation mutation =
            AsyncStoreClientUtis.buildMutation(mutateData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    storeClient.mutate(mutateData);
    DeleteData deleteData = new DeleteDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
            .addColumn(columnFamily, longTTLColumnQualifier).build();
    MutateData deleteMutateData = new MutateDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes()).
            addDeleteData(deleteData).build();
    AsyncStoreClientUtis.Mutation deleteMutation =
            AsyncStoreClientUtis.buildMutation(deleteMutateData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    when(table.mutateRow(argThat(new MutationMatcher(deleteMutation.rowMutations)))).thenReturn(CompletableFuture.completedFuture(null));
    storeClient.mutate(deleteMutateData).isDone();
  }

  @Test
  public void testMutateWithIndex() throws StoreException, IOException, ExecutionException, InterruptedException {
    StoreData shortTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, shortTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(shortTTL).build();

    StoreData longTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, longTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(longTTL)
        .addIndex(columnFamily, indexKey, IndexType.APPEND_ONLY).build();

    MutateData mutateData = new MutateDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes()).
        addStoreData(shortTTLData).addStoreData(longTTLData).build();

    AsyncStoreClientUtis.Mutation mutation =
        AsyncStoreClientUtis.buildMutation(mutateData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    when(table.mutateRow(argThat(new MutationMatcher(mutation.rowMutations)))).thenReturn(CompletableFuture.completedFuture(null));
    when(indexTable.put(mutation.indexPuts)).thenReturn(Arrays.asList(CompletableFuture.completedFuture(null)));
    storeClient.mutate(mutateData).get();
  }

  @Test
  public void testMutateWithException() throws StoreException, IOException, ExecutionException, InterruptedException {
    String errorMessage = "Failed to mutate";

    StoreData shortTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, shortTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(shortTTL).build();

    StoreData longTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, longTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(longTTL).build();

    MutateData mutateData = new MutateDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes()).
        addStoreData(shortTTLData).addStoreData(longTTLData).build();

    CompletableFuture<Void> future = new CompletableFuture<>();
    future.completeExceptionally(new StoreException(errorMessage));
    AsyncStoreClientUtis.Mutation mutation =
        AsyncStoreClientUtis.buildMutation(mutateData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    when(table.mutateRow(argThat(new MutationMatcher(mutation.rowMutations)))).thenReturn(future);
    try {
      storeClient.mutate(mutateData).get();
      assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof StoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }

  @Test
  public void testMutateWithPayloadException() throws StoreException, IOException, ExecutionException, InterruptedException {
    String errorMessage = "Payload size breach threshod 1, got 10";

    StoreData shortTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, shortTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(shortTTL).build();

    StoreData longTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, longTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(longTTL).build();

    MutateData mutateData = new MutateDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes()).
        addStoreData(shortTTLData).addStoreData(longTTLData).build();

    when(namespaceDescriptor.getConfigurationValue(PAYLOAD_THREASHOLD)).thenReturn("1");
    when(admin.getNamespaceDescriptor(NAMESPACE_NAME))
        .thenReturn(CompletableFuture.completedFuture(namespaceDescriptor));

    AsyncStoreClientUtis.Mutation mutation =
        AsyncStoreClientUtis.buildMutation(mutateData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));

    when(table.mutateRow(argThat(new MutationMatcher(mutation.rowMutations)))).thenReturn(CompletableFuture.completedFuture(null));
    try {
      storeClient.mutate(mutateData).get();
      assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof PayloadValidatorException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }

  @Test
  public void testMutateWithIndexException() throws StoreException, IOException, ExecutionException, InterruptedException {
    String errorMessage = "Failed index put";

    StoreData shortTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, shortTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(shortTTL).build();

    StoreData longTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, longTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(longTTL)
        .addIndex(columnFamily, indexKey, IndexType.APPEND_ONLY).build();

    MutateData mutateData = new MutateDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes()).
        addStoreData(shortTTLData).addStoreData(longTTLData).build();

    CompletableFuture<Void> future = new CompletableFuture<>();
    future.completeExceptionally(new StoreException(errorMessage));

    AsyncStoreClientUtis.Mutation mutation =
        AsyncStoreClientUtis.buildMutation(mutateData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));

    when(table.mutateRow(argThat(new MutationMatcher(mutation.rowMutations)))).thenReturn(CompletableFuture.completedFuture(null));
    when(indexTable.put(any(List.class))).thenReturn(Arrays.asList(future));
    try {
      storeClient.mutate(mutateData).get();
      assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof StoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }

  @Test
  public void testIOException() throws Exception {

    StoreData shortTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, shortTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(shortTTL).build();

    StoreData longTTLData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey("r".getBytes())
        .addColumn(columnFamily, longTTLColumnQualifier, qualifierValue.getBytes()).withTTLInMilliseconds(longTTL).build();

    MutateData mutateData = new MutateDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes()).
        addStoreData(shortTTLData).addStoreData(longTTLData).build();

    try {
      storeClient.mutate(mutateData).get();
      assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof IOException);
    }
  }
}
