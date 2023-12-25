package com.flipkart.yak.client;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.distributor.KeyDistributor;
import com.flipkart.yak.distributor.MurmusHashDistribution;
import com.flipkart.yak.models.DeleteData;
import com.flipkart.yak.models.DeleteDataBuilder;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.junit.After;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.Matchers.argThat;
import static org.powermock.api.mockito.PowerMockito.when;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class) public class AsyncDeleteTest extends AsyncBaseTest {

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

  @Test public void testDelete() throws StoreException, IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    DeleteData data = new DeleteDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes()).build();
    byte[] key = keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(data.getRow());
    Delete delete = AsyncStoreClientUtis.buildDeleteQuery(data, key);

    when(table.delete(argThat(new DeleteMatcher(delete)))).thenReturn(CompletableFuture.completedFuture(null));
    storeClient.delete(data).get();
  }

  @Test public void testDeleteWithPartitionKey() throws StoreException, IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String partitionKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    DeleteData data = new DeleteDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes()).withPartitionKey(partitionKey.getBytes()).build();
    byte[] key = keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(data.getRow(), partitionKey.getBytes());
    Delete delete = AsyncStoreClientUtis.buildDeleteQuery(data, key);

    when(table.delete(argThat(new DeleteMatcher(delete)))).thenReturn(CompletableFuture.completedFuture(null));
    storeClient.delete(data).get();
  }

  @Test public void testDeleteWithFilter()
      throws StoreException, IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    DeleteData data =
        new DeleteDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes()).addColumn(columnFamily, columnQualifier)
            .build();
    byte[] key = keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(data.getRow());
    Delete delete = AsyncStoreClientUtis.buildDeleteQuery(data, key);

    when(table.delete(argThat(new DeleteMatcher(delete)))).thenReturn(CompletableFuture.completedFuture(null));
    storeClient.delete(data).get();
  }

  @Test public void testDeleteAllWithFilter()
          throws StoreException, IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    DeleteData data =
            new DeleteDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes()).withType(KeyValue.Type.DeleteColumn).addColumn(columnFamily, columnQualifier)
                    .build();
    byte[] key = keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(data.getRow());
    Delete delete = AsyncStoreClientUtis.buildDeleteQuery(data, key);

    when(table.delete(argThat(new DeleteMatcher(delete)))).thenReturn(CompletableFuture.completedFuture(null));
    storeClient.delete(data).get();
  }

  @Test public void testDeleteWithOutKeyDistributer()
      throws StoreException, IOException, ExecutionException, InterruptedException, TimeoutException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    storeClient = new AsyncStoreClientImpl(config, Optional.empty(), timeoutInSeconds, registry);
    DeleteData data = new DeleteDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes()).build();

    Delete delete = AsyncStoreClientUtis.buildDeleteQuery(data, data.getRow());
    when(table.delete(argThat(new DeleteMatcher(delete)))).thenReturn(CompletableFuture.completedFuture(null));
    storeClient.delete(data).get();
  }

  @Test public void testDeleteWithException() throws InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String errorMessage = "Failed with exception";

    DeleteData data = new DeleteDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes()).build();
    byte[] key = keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(data.getRow());
    Delete delete = AsyncStoreClientUtis.buildDeleteQuery(data, key);
    CompletableFuture future = new CompletableFuture();
    future.completeExceptionally(new StoreException(errorMessage));
    when(table.delete(argThat(new DeleteMatcher(delete)))).thenReturn(future);
    try {
      storeClient.delete(data).get();
      assertTrue("Should have thrown exception", false);
    } catch (Exception ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof StoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }
}

