package com.flipkart.yak.client;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.distributor.KeyDistributor;
import com.flipkart.yak.distributor.MurmusHashDistribution;
import com.flipkart.yak.models.IncrementData;
import com.flipkart.yak.models.IncrementDataBuilder;
import com.flipkart.yak.models.ResultMap;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
public class AsyncIncrementTest extends AsyncBaseTest {
  Map<String, KeyDistributor> keyDistributorMap = new HashMap<>();
  MetricRegistry registry = new MetricRegistry();
  int timeoutInSeconds = 30;
  SiteConfig config;
  private AsyncStoreClient storeClient;

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

  @Test
  public void testIncrementWithSingleCol() throws StoreException, IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    IncrementDataBuilder builder = new IncrementDataBuilder(FULL_TABLE_NAME);
    IncrementData incrementData = builder
      .withRowKey(rowKey.getBytes())
      .addColumn(columnFamily.getBytes(),
        columnQualifier.getBytes(), 1L)
      .build();
    Increment increment = AsyncStoreClientUtis.buildIncrement(incrementData, keyDistributorMap);
    when(table.increment(increment)).thenReturn(CompletableFuture.completedFuture(buildResponse(rowKey, columnFamily, columnQualifier, "2")));

    ResultMap resultMap = storeClient.increment(incrementData).get();

    assertNotNull("Expect result to be not null", resultMap);
    assertTrue("Expect column family to be present", resultMap.containsKey(columnFamily));
    assertTrue("Expect column to be present in column family", resultMap.get(columnFamily).containsKey(columnQualifier));
    assertTrue("Incremented value should be equal",
      Bytes.toString(resultMap.get(columnFamily).get(columnQualifier).getValue()).equals("2"));
  }

  @Test
  public void testIncrementWhenColIsNotPresent() throws StoreException, IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    IncrementDataBuilder builder = new IncrementDataBuilder(FULL_TABLE_NAME);
    IncrementData incrementData = builder
        .withRowKey(rowKey.getBytes())
        .addColumn(columnFamily.getBytes(),
            columnQualifier.getBytes(), 1L)
        .build();
    Increment increment = AsyncStoreClientUtis.buildIncrement(incrementData, keyDistributorMap);
    when(table.increment(increment)).thenReturn(CompletableFuture.completedFuture(buildResponse(rowKey, columnFamily, columnQualifier, "1")));

    ResultMap resultMap = storeClient.increment(incrementData).get();

    assertNotNull("Expect result to be not null", resultMap);
    assertTrue("Expect column family to be present", resultMap.containsKey(columnFamily));
    assertTrue("Expect column to be present in column family", resultMap.get(columnFamily).containsKey(columnQualifier));
    assertTrue("Incremented value should be equal",
        Bytes.toString(resultMap.get(columnFamily).get(columnQualifier).getValue()).equals("1"));
  }

  @Test
  public void testIncrementWithMultipleCol() throws StoreException, IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    List<String> columnFamilies = Lists.newArrayList(RandomStringUtils.randomAlphanumeric(10).toUpperCase(),
      RandomStringUtils.randomAlphanumeric(10).toUpperCase());
    List<String> columnQualifiers = Lists.newArrayList(RandomStringUtils.randomAlphanumeric(10).toUpperCase(),
      RandomStringUtils.randomAlphanumeric(10).toUpperCase());
    List<String> values = Lists.newArrayList("2", "2");

    IncrementDataBuilder builder = new IncrementDataBuilder(FULL_TABLE_NAME);
    IncrementData incrementData = builder
      .withRowKey(rowKey.getBytes())
      .addColumn(columnFamilies.get(0).getBytes(),
        columnQualifiers.get(0).getBytes(),
        1L)
      .addColumn(columnFamilies.get(1).getBytes(),
        columnQualifiers.get(1).getBytes(),
        1L)
      .build();
    Increment increment = AsyncStoreClientUtis.buildIncrement(incrementData, keyDistributorMap);

    when(table.increment(increment)).thenReturn(CompletableFuture.completedFuture(buildResponse(rowKey, columnFamilies, columnQualifiers, values)));

    ResultMap resultMap = storeClient.increment(incrementData).get();

    assertNotNull("Expect result to be not null", resultMap);

    for (int index = 0; index < columnFamilies.size(); index++) {
      assertTrue("Expect column family to be present", resultMap.containsKey(columnFamilies.get(index)));
      assertTrue("Expect column to be present in column family", resultMap.get(columnFamilies.get(index)).containsKey(columnQualifiers.get(index)));
      assertTrue("Incremented value should be equal",
        Bytes.toString(resultMap.get(columnFamilies.get(index)).get(columnQualifiers.get(index)).getValue()).equals("2"));

    }
  }

  @Test
  public void testIncrementWithException() throws InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String errorMessage = "Failed with exception";

    IncrementDataBuilder builder = new IncrementDataBuilder(FULL_TABLE_NAME);
    IncrementData incrementData = builder
      .withRowKey(rowKey.getBytes())
      .addColumn(columnFamily.getBytes(),
        columnQualifier.getBytes(), 1L)
      .build();

    Increment increment = AsyncStoreClientUtis.buildIncrement(incrementData, keyDistributorMap);
    CompletableFuture<Result> future = new CompletableFuture<>();
    future.completeExceptionally(new StoreException(errorMessage));
    when(table.increment(eq(increment))).thenReturn(future);
    try {
      storeClient.increment(incrementData).get();
      assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof StoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }
}
