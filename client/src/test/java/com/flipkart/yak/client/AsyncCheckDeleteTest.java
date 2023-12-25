package com.flipkart.yak.client;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.yak.client.mocks.MockCheckAndMutateBuilderImpl;
import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.distributor.KeyDistributor;
import com.flipkart.yak.distributor.MurmusHashDistribution;
import com.flipkart.yak.models.CheckAndDeleteData;
import com.flipkart.yak.models.DeleteDataBuilder;
import com.flipkart.yak.models.StoreData;
import com.flipkart.yak.models.StoreDataBuilder;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hbase.CompareOperator;
import org.junit.After;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.powermock.api.mockito.PowerMockito.when;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class) public class AsyncCheckDeleteTest extends AsyncBaseTest {
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

  @Test public void testCheckAndDelete() throws StoreException, IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes()).build();

    CheckAndDeleteData deleteData = new DeleteDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, qualifierValue.getBytes(), CompareOperator.EQUAL);

    byte[] key = keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes());
    when(table.checkAndMutate(key, columnFamily.getBytes())).thenReturn(new MockCheckAndMutateBuilderImpl(true, null));
    Boolean success = storeClient.checkAndDelete(deleteData).get();
    assertTrue("Check and delete operation should have been successful", success);
  }

  @Test public void testCheckAndDeleteWithComparatorFailed()
      throws StoreException, IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes()).build();

    CheckAndDeleteData deleteData = new DeleteDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, qualifierValue.getBytes(), CompareOperator.EQUAL);

    byte[] key = keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes());
    when(table.checkAndMutate(key, columnFamily.getBytes())).thenReturn(new MockCheckAndMutateBuilderImpl(false, null));
    Boolean success = storeClient.checkAndDelete(deleteData).get();
    assertTrue("Check and delete operation should have been failed", !success);
  }

  @Test public void testCheckAndDeleteWithOutKeyDistributer()
      throws StoreException, IOException, ExecutionException, InterruptedException, TimeoutException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    storeClient = new AsyncStoreClientImpl(config, Optional.empty(), timeoutInSeconds, registry);

    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes()).build();

    CheckAndDeleteData deleteData = new DeleteDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, qualifierValue.getBytes(), CompareOperator.EQUAL);

    when(table.checkAndMutate(rowKey.getBytes(), columnFamily.getBytes()))
        .thenReturn(new MockCheckAndMutateBuilderImpl(true, null));
    Boolean success = storeClient.checkAndDelete(deleteData).get();
    assertTrue("Check and delete operation should have been successful", success);
  }

  @Test public void testCheckAndDeleteWithException() throws InterruptedException, ExecutionException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String errorMessage = "Failed to put";

    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .addColumn(columnFamily, columnQualifier, qualifierValue.getBytes()).build();

    CheckAndDeleteData deleteData = new DeleteDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey.getBytes())
        .buildWithCheckAndVerifyColumn(columnFamily, columnQualifier, qualifierValue.getBytes(), CompareOperator.EQUAL);

    byte[] key = keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes());
    when(table.checkAndMutate(key, columnFamily.getBytes()))
        .thenReturn(new MockCheckAndMutateBuilderImpl(false, new StoreException(errorMessage)));
    try {
      storeClient.checkAndDelete(deleteData).get();
      assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof StoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }
}
