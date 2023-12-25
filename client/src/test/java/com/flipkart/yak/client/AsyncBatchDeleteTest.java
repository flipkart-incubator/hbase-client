package com.flipkart.yak.client;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.exceptions.RequestValidatorException;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.distributor.KeyDistributor;
import com.flipkart.yak.distributor.MurmusHashDistribution;
import com.flipkart.yak.models.DeleteData;
import com.flipkart.yak.models.DeleteDataBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
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

@RunWith(PowerMockRunner.class) public class AsyncBatchDeleteTest extends AsyncBaseTest {
  private AsyncStoreClient storeClient;
  private Map<String, KeyDistributor> keyDistributorMap = new HashMap<>();
  private MetricRegistry registry = new MetricRegistry();
  private int timeoutInSeconds = 30;
  private SiteConfig config;
  private String rowKey1, rowKey2, rowKey3;
  private byte[] key1, key2, key3;
  private List<Delete> deletes;
  private List<DeleteData> deleteData;

  @Before public void setUp() throws Exception {
    super.setup();
    config = buildStoreClientConfig();
    keyDistributorMap.put(FULL_TABLE_NAME, new MurmusHashDistribution(128));
    storeClient = new AsyncStoreClientImpl(config, Optional.of(keyDistributorMap), timeoutInSeconds, registry);

    rowKey1 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    DeleteData data1 = new DeleteDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).build();
    key1 = keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(data1.getRow());
    Delete delete1 = AsyncStoreClientUtis.buildDeleteQuery(data1, key1);

    rowKey2 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    DeleteData data2 = new DeleteDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey2.getBytes()).build();
    key2 = keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(data2.getRow());
    Delete delete2 = AsyncStoreClientUtis.buildDeleteQuery(data2, key2);

    rowKey3 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    DeleteData data3 = new DeleteDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey3.getBytes()).withType(KeyValue.Type.DeleteColumn).build();
    key3 = keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(data3.getRow());
    Delete delete3 = AsyncStoreClientUtis.buildDeleteQuery(data3, key3);


    deletes = Arrays.asList(delete1, delete2, delete3);
    deleteData = Arrays.asList(data1, data2, data3);
  }

  @After public void tearDown() throws Exception {
    storeClient.shutdown();
  }

  @Test public void testBatchDelete() throws ExecutionException, InterruptedException {
    List<CompletableFuture<Void>> futures = deletes.stream().map(delete -> {
      CompletableFuture<Void> future = new CompletableFuture<>();
      future.complete(null);
      return future;
    }).collect(Collectors.toList());

    when(table.delete(argThat(new BatchDeleteMatcher(deletes)))).thenReturn(futures);
    List<CompletableFuture<Void>> outputFutures = storeClient.delete(deleteData);

    CompletableFuture.allOf(outputFutures.toArray(new CompletableFuture[outputFutures.size()])).thenApply(v -> {
      return outputFutures.stream().map(future -> future.join()).collect(Collectors.toList());
    }).get();
    Assert.assertEquals("Batch Delete should have been " + deleteData.size(), deleteData.size(), outputFutures.size());
  }

  @Test public void testBatchDeleteWithException() throws ExecutionException, InterruptedException {
    String errorMessage = "Failed to delete";

    List<CompletableFuture<Void>> futures = deletes.stream().map(delete -> {
      return new CompletableFuture<Void>();
    }).collect(Collectors.toList());

    futures.get(0).completeExceptionally(new StoreException(errorMessage));
    futures.get(1).complete(null);
    futures.get(2).completeExceptionally(new StoreException(errorMessage));

    when(table.delete(argThat(new BatchDeleteMatcher(deletes)))).thenReturn(futures);
    List<CompletableFuture<Void>> outputFutures = storeClient.delete(deleteData);

    try {
      CompletableFuture.allOf(outputFutures.toArray(new CompletableFuture[outputFutures.size()])).thenApply(v -> {
        return outputFutures.stream().map(future -> future.join()).collect(Collectors.toList());
      }).get();
    } catch (Exception ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof StoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }

  @Test public void testBatchDeleteWithEmptyBatch() throws ExecutionException, InterruptedException {
    deletes = new ArrayList<>();
    deleteData = new ArrayList<>();

    List<CompletableFuture<Void>> futures = deletes.stream().map(delete -> {
      return new CompletableFuture<Void>();
    }).collect(Collectors.toList());

    when(table.delete(argThat(new BatchDeleteMatcher(deletes)))).thenReturn(futures);
    List<CompletableFuture<Void>> outputFutures = storeClient.delete(deleteData);

    List<Void> output =
        CompletableFuture.allOf(outputFutures.toArray(new CompletableFuture[outputFutures.size()])).thenApply(v -> {
          return outputFutures.stream().map(future -> future.join()).collect(Collectors.toList());
        }).get();

    assertTrue("Output list should be empty " + output.size(), output.isEmpty());
  }

  @Test public void testBatchDeleteWithBatchLimitValidationFailures() throws Exception {
    config = buildStoreClientConfig();
    config.withMaxBatchDeleteSize(1);

    String errorMessage =
        "Request size should not be greater than the configured batch size " + config.getMaxBatchDeleteSize();
    storeClient = new AsyncStoreClientImpl(config, Optional.empty(), timeoutInSeconds, registry);

    List<CompletableFuture<Void>> outputFutures = storeClient.delete(deleteData);
    assertEquals("Output list should be " + deleteData.size(), deleteData.size(), outputFutures.size());

    outputFutures.stream().forEach(future -> {
      try {
        future.get();
        assertTrue("Should have failed with exception", false);
      } catch (Exception error) {
        assertNotNull("Expect RequestValidatorException to be thrown", error.getCause());
        assertTrue("Expect RequestValidatorException to be thrown",
            error.getCause() instanceof RequestValidatorException);
        assertTrue("Expect exception message to be: " + errorMessage,
            error.getCause().getMessage().equals(errorMessage));
      }
    });
  }

  @Test public void testBatchDeleteWithTableNameBatchValidationFailures() throws Exception {
    String rowKey3 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    deleteData.set(0, new DeleteDataBuilder(FULL_TABLE_NAME + "Test").withRowKey(rowKey3.getBytes()).build());
    String errorMessage = "Batch request should only come for one table";

    List<CompletableFuture<Void>> outputFutures = storeClient.delete(deleteData);
    assertEquals("Output list should be " + deleteData.size(), deleteData.size(), outputFutures.size());

    outputFutures.stream().forEach(future -> {
      try {
        future.get();
        assertTrue("Should have failed with exception", false);
      } catch (Exception error) {
        assertNotNull("Expect RequestValidatorException to be thrown", error.getCause());
        assertTrue("Expect RequestValidatorException to be thrown",
            error.getCause() instanceof RequestValidatorException);
        assertTrue("Expect exception message to be: " + errorMessage,
            error.getCause().getMessage().equals(errorMessage));
      }
    });
  }
}
