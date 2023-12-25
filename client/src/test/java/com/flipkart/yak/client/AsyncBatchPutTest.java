package com.flipkart.yak.client;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.exceptions.RequestValidatorException;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.distributor.KeyDistributor;
import com.flipkart.yak.distributor.MurmusHashDistribution;
import com.flipkart.yak.models.StoreData;
import com.flipkart.yak.models.StoreDataBuilder;
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
import org.apache.hadoop.hbase.client.Put;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import static org.mockito.Matchers.argThat;
import static org.powermock.api.mockito.PowerMockito.when;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class) public class AsyncBatchPutTest extends AsyncBaseTest {
  private AsyncStoreClient storeClient;
  private Map<String, KeyDistributor> keyDistributorMap = new HashMap<>();
  private MetricRegistry registry = new MetricRegistry();
  private int timeoutInSeconds = 30;
  private SiteConfig config;
  private List<Put> puts;
  private List<StoreData> dataList;

  @Before public void setUp() throws Exception {
    super.setup();
    config = buildStoreClientConfig();
    keyDistributorMap.put(FULL_TABLE_NAME, new MurmusHashDistribution(128));
    storeClient = new AsyncStoreClientImpl(config, Optional.of(keyDistributorMap), timeoutInSeconds, registry);

    String rowKey1 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily1 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier1 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue1 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String indexKey1 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    String rowKey2 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily2 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier2 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue2 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String indexKey2 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    StoreData storeData1 = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes())
        .addColumn(columnFamily1, columnQualifier1, qualifierValue1.getBytes()).build();

    StoreData storeData2 = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey2.getBytes())
        .addColumn(columnFamily2, columnQualifier2, qualifierValue2.getBytes()).build();

    AsyncStoreClientUtis.StorePuts put1 =
        AsyncStoreClientUtis.buildStorePuts(storeData1, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    AsyncStoreClientUtis.StorePuts put2 =
        AsyncStoreClientUtis.buildStorePuts(storeData2, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));

    puts = Arrays.asList(put1.entityPut, put2.entityPut);
    dataList = Arrays.asList(storeData1, storeData2);
  }

  @After public void tearDown() throws Exception {
    storeClient.shutdown();
  }

  private class BatchPutMatcher extends ArgumentMatcher<List<Put>> {
    private List<Put> left;

    public BatchPutMatcher(List<Put> left) {
      this.left = left;
    }

    @Override public boolean matches(Object argument) {
      List<Put> right = (List<Put>) argument;
      if (right.size() != left.size()) {
        return false;
      }

      for (int index = 0; index < right.size(); index += 1) {
        Put rightItem = right.get(index);
        Put leftItem = left.get(index);
        boolean success = Arrays.equals(rightItem.getRow(), leftItem.getRow()) && rightItem.getFamilyCellMap()
            .equals(leftItem.getFamilyCellMap()) && rightItem.getDurability().equals(leftItem.getDurability());
        if (!success) {
          return false;
        }
      }
      return true;
    }
  }

  @Test public void testBatchPut() throws ExecutionException, InterruptedException {
    List<CompletableFuture<Void>> futures = puts.stream().map(put -> {
      CompletableFuture<Void> future = new CompletableFuture<>();
      future.complete(null);
      return future;
    }).collect(Collectors.toList());

    when(table.put(argThat(new BatchPutMatcher(puts)))).thenReturn(futures);
    List<CompletableFuture<Void>> outputFutures = storeClient.put(dataList);

    CompletableFuture.allOf(outputFutures.toArray(new CompletableFuture[outputFutures.size()])).thenApply(v -> {
      return outputFutures.stream().map(future -> future.join()).collect(Collectors.toList());
    }).get();
    assertEquals("Batch Put should have been " + dataList.size(), dataList.size(), outputFutures.size());
  }

  @Test public void testBatchPutWithException() throws ExecutionException, InterruptedException {
    String errorMessage = "Failed to delete";

    List<CompletableFuture<Void>> futures = puts.stream().map(put -> {
      return new CompletableFuture<Void>();
    }).collect(Collectors.toList());
    futures.get(0).completeExceptionally(new StoreException(errorMessage));
    futures.get(1).complete(null);

    when(table.put(argThat(new BatchPutMatcher(puts)))).thenReturn(futures);
    List<CompletableFuture<Void>> outputFutures = storeClient.put(dataList);

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

  @Test public void testBatchPutWithEmptyBatch() throws ExecutionException, InterruptedException {
    puts = new ArrayList<>();
    dataList = new ArrayList<>();

    List<CompletableFuture<Void>> futures = dataList.stream().map(delete -> {
      return new CompletableFuture<Void>();
    }).collect(Collectors.toList());

    when(table.put(argThat(new BatchPutMatcher(puts)))).thenReturn(futures);
    List<CompletableFuture<Void>> outputFutures = storeClient.put(dataList);

    List<Void> output =
        CompletableFuture.allOf(outputFutures.toArray(new CompletableFuture[outputFutures.size()])).thenApply(v -> {
          return outputFutures.stream().map(future -> future.join()).collect(Collectors.toList());
        }).get();

    assertTrue("Output list should be empty " + output.size(), output.isEmpty());
  }

  @Test public void testBatchPutWithTableNameBatchValidationFailures() throws Exception {
    String rowKey3 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    dataList.set(0, new StoreDataBuilder(FULL_TABLE_NAME + "1").withRowKey(rowKey3.getBytes()).build());
    String errorMessage = "Batch request should only come for one table";

    List<CompletableFuture<Void>> outputFutures = storeClient.put(dataList);
    assertEquals("Output list should be " + dataList.size(), dataList.size(), outputFutures.size());

    outputFutures.stream().forEach(future -> {
      try {
        future.get();
      } catch (Exception error) {
        assertNotNull("Expect StoreException to be thrown", error.getCause());
        assertTrue("Expect StoreException to be thrown", error.getCause() instanceof RequestValidatorException);
        assertTrue("Expect exception message to be: " + errorMessage,
            error.getCause().getMessage().equals(errorMessage));
      }
    });
  }
}
