package com.flipkart.yak.client;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.exceptions.PayloadValidatorException;
import com.flipkart.yak.client.exceptions.StoreDataNotFoundException;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.distributor.KeyDistributor;
import com.flipkart.yak.distributor.MurmusHashDistribution;
import com.flipkart.yak.models.ResultMap;
import com.flipkart.yak.models.StoreData;
import com.flipkart.yak.models.StoreDataBuilder;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
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

@RunWith(PowerMockRunner.class) public class AsyncAppendTest extends AsyncBaseTest {
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

  private class AppendMatcher extends ArgumentMatcher<Append> {
    private Append left;

    public AppendMatcher(Append left) {
      this.left = left;
    }

    @Override public boolean matches(Object argument) {
      Append right = (Append) argument;
      return Arrays.equals(right.getRow(), left.getRow()) && right.getFamilyCellMap().equals(left.getFamilyCellMap())
          && right.getDurability().equals(left.getDurability());
    }
  }

  @Test public void testAppend() throws StoreException, IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes()).build();

    Append append = AsyncStoreClientUtis
        .buildStoreAppend(storeData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    when(table.append(argThat(new AppendMatcher(append))))
        .thenReturn(CompletableFuture.completedFuture(buildResultData(storeData.getRow(), storeData.getCfs())));
    ResultMap map = storeClient.append(storeData).get();

    assertTrue("Result does not contain family mutated", map.containsKey(columnFamily));
    assertTrue("Result does not contain column multated", map.get(columnFamily).containsKey(columnQualifier));
    assertEquals("Result does not contain value multated", qualifierValue,
        Bytes.toString(map.get(columnFamily).get(columnQualifier).getValue()));
  }

  @Test public void testAppendWithOutKeyDistributer()
      throws StoreException, IOException, ExecutionException, InterruptedException, TimeoutException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    storeClient = new AsyncStoreClientImpl(config, Optional.empty(), timeoutInSeconds, registry);

    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes()).build();

    Append append =
        AsyncStoreClientUtis.buildStoreAppend(storeData, new HashMap<>(), Optional.of(config.getDurabilityThreshold()));
    when(table.append(argThat(new AppendMatcher(append))))
        .thenReturn(CompletableFuture.completedFuture(buildResultData(storeData.getRow(), storeData.getCfs())));
    ResultMap map = storeClient.append(storeData).get();

    assertTrue("Result does not contain family mutated", map.containsKey(columnFamily));
    assertTrue("Result does not contain column multated", map.get(columnFamily).containsKey(columnQualifier));
    assertEquals("Result does not contain value multated", qualifierValue,
        Bytes.toString(map.get(columnFamily).get(columnQualifier).getValue()));
  }

  @Test public void testAppendWithException() throws InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String errorMessage = "Failed to append";

    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes()).build();

    CompletableFuture<Result> future = new CompletableFuture<>();
    future.completeExceptionally(new StoreException(errorMessage));

    Append append = AsyncStoreClientUtis
        .buildStoreAppend(storeData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    when(table.append(argThat(new AppendMatcher(append)))).thenReturn(future);

    try {
      storeClient.append(storeData).get();
      assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof StoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }

  @Test public void testAppendWithNoRowFoundToAppend() throws StoreException, IOException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes()).build();

    Append append = AsyncStoreClientUtis
        .buildStoreAppend(storeData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    when(table.append(argThat(new AppendMatcher(append)))).thenReturn(CompletableFuture.completedFuture(new Result()));
    try {
      storeClient.append(storeData).get();
      assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreDataNotFoundException  to be thrown", ex.getCause());
      assertTrue("Expect StoreDataNotFoundException to be thrown", ex.getCause() instanceof StoreDataNotFoundException);
    }
  }

  @Test public void testAppendWithPayloadValidationFailed() throws StoreException, IOException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String errorMessage = "Payload size breach threshod 1, got 10";

    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes()).build();

    when(namespaceDescriptor.getConfigurationValue(PAYLOAD_THREASHOLD)).thenReturn("1");
    when(admin.getNamespaceDescriptor(NAMESPACE_NAME))
        .thenReturn(CompletableFuture.completedFuture(namespaceDescriptor));

    Append append = AsyncStoreClientUtis
        .buildStoreAppend(storeData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    when(table.append(argThat(new AppendMatcher(append)))).thenReturn(CompletableFuture.completedFuture(new Result()));
    try {
      storeClient.append(storeData).get();
      assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof PayloadValidatorException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }
}
