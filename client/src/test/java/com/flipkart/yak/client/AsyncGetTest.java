package com.flipkart.yak.client;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.distributor.KeyDistributor;
import com.flipkart.yak.distributor.MurmusHashDistribution;
import com.flipkart.yak.models.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.powermock.api.mockito.PowerMockito.when;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class) public class AsyncGetTest extends AsyncBaseTest {

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

  @Test public void testGetByGetRow() throws StoreException, IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    byte[] rowKeyInBytes = keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes());
    GetDataBuilder builder = new GetDataBuilder(FULL_TABLE_NAME);
    GetRow row = builder.withRowKey(rowKey.getBytes()).build();

    Get get = AsyncStoreClientUtis.buildGets(Arrays.asList(row), keyDistributorMap).get(0);
    when(table.get(eq(get))).thenReturn(
        CompletableFuture.completedFuture(buildResponse(rowKey, columnFamily, columnQualifier, qualifierValue)));
    ResultMap map = storeClient.get(row).get();
    assertNotNull("Expect result to be not null", map);
    assertTrue("Expect column family to be present", map.containsKey(columnFamily));
    assertTrue("Expect column to be present in column family", map.get(columnFamily).containsKey(columnQualifier));
    assertTrue("Qualifier value should be equal",
        Bytes.toString(map.get(columnFamily).get(columnQualifier).getValue()).equals(qualifierValue));

    builder = new GetDataBuilder(FULL_TABLE_NAME);
    row = builder.withRowKey(rowKey.getBytes()).buildForColFamily(columnFamily);
    get = AsyncStoreClientUtis.buildGets(Arrays.asList(row), keyDistributorMap).get(0);
    when(table.get(eq(get))).thenReturn(
        CompletableFuture.completedFuture(buildResponse(rowKey, columnFamily, columnQualifier, qualifierValue)));
    map = storeClient.get(row).get();
    assertNotNull("Expect result to be not null", map);
    assertTrue("Expect column family to be present", map.containsKey(columnFamily));
    assertTrue("Expect column to be present in column family", map.get(columnFamily).containsKey(columnQualifier));
    assertTrue("Qualifier value should be equal",
        Bytes.toString(map.get(columnFamily).get(columnQualifier).getValue()).equals(qualifierValue));

    builder = new GetDataBuilder(FULL_TABLE_NAME);
    row = builder.withRowKey(rowKey.getBytes()).buildForColumn(columnFamily, columnQualifier);
    get = AsyncStoreClientUtis.buildGets(Arrays.asList(row), keyDistributorMap).get(0);
    when(table.get(eq(get))).thenReturn(
        CompletableFuture.completedFuture(buildResponse(rowKey, columnFamily, columnQualifier, qualifierValue)));
    map = storeClient.get(row).get();
    assertNotNull("Expect result to be not null", map);
    assertTrue("Expect column family to be present", map.containsKey(columnFamily));
    assertTrue("Expect column to be present in column family", map.get(columnFamily).containsKey(columnQualifier));
    assertTrue("Qualifier value should be equal",
        Bytes.toString(map.get(columnFamily).get(columnQualifier).getValue()).equals(qualifierValue));
  }

  @Test public void testGetByGetCell() throws StoreException, IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    byte[] rowKeyInBytes = keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes());
    GetDataBuilder builder = new GetDataBuilder(FULL_TABLE_NAME);
    GetCell row = builder.withRowKey(rowKey.getBytes()).buildForColumn(columnFamily, columnQualifier);

    Get get = AsyncStoreClientUtis.buildGets(Arrays.asList(row), keyDistributorMap).get(0);
    when(table.get(eq(get))).thenReturn(
        CompletableFuture.completedFuture(buildResponse(rowKey, columnFamily, columnQualifier, qualifierValue)));
    ResultMap map = storeClient.get(row).get();
    assertNotNull("Expect result to be not null", map);
    assertTrue("Expect column family to be present", map.containsKey(columnFamily));
    assertTrue("Expect column to be present in column family", map.get(columnFamily).containsKey(columnQualifier));
    assertTrue("Qualifier value should be equal",
        Bytes.toString(map.get(columnFamily).get(columnQualifier).getValue()).equals(qualifierValue));
  }

  @Test public void testGetByGetCells() throws StoreException, IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    byte[] rowKeyInBytes = keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes());
    GetDataBuilder builder = new GetDataBuilder(FULL_TABLE_NAME);
    GetCells row = (GetCells) builder.withRowKey(rowKey.getBytes()).addColl(columnFamily, columnQualifier).build();

    Get get = AsyncStoreClientUtis.buildGets(Arrays.asList(row), keyDistributorMap).get(0);
    when(table.get(eq(get))).thenReturn(
        CompletableFuture.completedFuture(buildResponse(rowKey, columnFamily, columnQualifier, qualifierValue)));
    ResultMap map = storeClient.get(row).get();
    assertNotNull("Expect result to be not null", map);
    assertTrue("Expect column family to be present", map.containsKey(columnFamily));
    assertTrue("Expect column to be present in column family", map.get(columnFamily).containsKey(columnQualifier));
    assertTrue("Qualifier value should be equal",
        Bytes.toString(map.get(columnFamily).get(columnQualifier).getValue()).equals(qualifierValue));
  }

  @Test public void testGetByPartitionKey() throws StoreException, IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String partitionKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    GetDataBuilder builder = new GetDataBuilder(FULL_TABLE_NAME);
    GetCells row = (GetCells) builder.withRowKey(rowKey.getBytes()).withPartitionKey(partitionKey.getBytes()).addColl(columnFamily, columnQualifier).build();

    Get get = AsyncStoreClientUtis.buildGets(Arrays.asList(row), keyDistributorMap).get(0);
    when(table.get(eq(get))).thenReturn(
            CompletableFuture.completedFuture(buildResponse(rowKey, columnFamily, columnQualifier, qualifierValue)));
    ResultMap map = storeClient.get(row).get();
    assertNotNull("Expect result to be not null", map);
    assertTrue("Expect column family to be present", map.containsKey(columnFamily));
    assertTrue("Expect column to be present in column family", map.get(columnFamily).containsKey(columnQualifier));
    assertTrue("Qualifier value should be equal",
            Bytes.toString(map.get(columnFamily).get(columnQualifier).getValue()).equals(qualifierValue));
  }

  @Test public void testGetByGetColumnsMap()
      throws StoreException, IOException, ExecutionException, InterruptedException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    byte[] rowKeyInBytes = keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes());
    GetDataBuilder builder = new GetDataBuilder(FULL_TABLE_NAME);
    GetColumnMap row = builder.withRowKey(rowKey.getBytes()).buildForColFamily(columnFamily);

    Get get = AsyncStoreClientUtis.buildGets(Arrays.asList(row), keyDistributorMap).get(0);
    when(table.get(eq(get))).thenReturn(
        CompletableFuture.completedFuture(buildResponse(rowKey, columnFamily, columnQualifier, qualifierValue)));
    ResultMap map = storeClient.get(row).get();
    assertNotNull("Expect result to be not null", map);
    assertTrue("Expect column family to be present", map.containsKey(columnFamily));
    assertTrue("Expect column to be present in column family", map.get(columnFamily).containsKey(columnQualifier));
    assertTrue("Qualifier value should be equal",
        Bytes.toString(map.get(columnFamily).get(columnQualifier).getValue()).equals(qualifierValue));
  }

  @Test public void testGetWithOutKeyDistributer()
      throws StoreException, IOException, ExecutionException, InterruptedException, TimeoutException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    storeClient = new AsyncStoreClientImpl(config, Optional.empty(), timeoutInSeconds, registry);
    GetDataBuilder builder = new GetDataBuilder(FULL_TABLE_NAME);
    GetRow row = builder.withRowKey(rowKey.getBytes()).build();

    Get get = AsyncStoreClientUtis.buildGets(Arrays.asList(row), new HashMap<>()).get(0);
    when(table.get(eq(get))).thenReturn(
        CompletableFuture.completedFuture(buildResponse(rowKey, columnFamily, columnQualifier, qualifierValue)));
    ResultMap map = storeClient.get(row).get();
    assertNotNull("Expect result to be not null", map);
    assertTrue("Expect column family to be present", map.containsKey(columnFamily));
    assertTrue("Expect column to be present in column family", map.get(columnFamily).containsKey(columnQualifier));
    assertTrue("Qualifier value should be equal",
        Bytes.toString(map.get(columnFamily).get(columnQualifier).getValue()).equals(qualifierValue));
  }

  @Test public void testGetWithTimeStamp()
          throws StoreException, IOException, ExecutionException, InterruptedException, TimeoutException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    storeClient = new AsyncStoreClientImpl(config, Optional.empty(), timeoutInSeconds, registry);
    GetDataBuilder builder = new GetDataBuilder(FULL_TABLE_NAME);
    GetRow row = builder.withRowKey(rowKey.getBytes()).build();

    Get get = AsyncStoreClientUtis.buildGets(Arrays.asList(row), new HashMap<>()).get(0);
    when(table.get(eq(get))).thenReturn(
            CompletableFuture.completedFuture(buildResponse(rowKey, columnFamily, columnQualifier, qualifierValue)));
    ResultMap map = storeClient.get(row).get();
    assertNotNull("Expect result to be not null", map);
    assertTrue("Expect column family to be present", map.containsKey(columnFamily));
    assertTrue("Expect column to be present in column family", map.get(columnFamily).containsKey(columnQualifier));
    assertTrue("Qualifier value should be equal",
            Bytes.toString(map.get(columnFamily).get(columnQualifier).getValue()).equals(qualifierValue));
    assertTrue("Qualifier First entry value should be equal",
            Bytes.toString(map.get(columnFamily).get(columnQualifier).getVersionedValues().firstEntry().getValue()).equals(qualifierValue));
    assertEquals("Qualifier First entry key should be equal",
            Long.MAX_VALUE, (long) map.get(columnFamily).get(columnQualifier).getVersionedValues().firstEntry().getKey());
  }

  @Test public void testGetWithVersions()
          throws StoreException, IOException, ExecutionException, InterruptedException, TimeoutException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    String qualifierValue = null;
    TreeMap<Long, byte[]> versionedCells = new TreeMap(new Comparator<Long>() {
      public int compare(Long l1, Long l2) {
        return l2.compareTo(l1);
      }
    });

    Set<Long> versions = Sets.newHashSet();
    for(long i=0; i<10; i++){
      qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
      versionedCells.put(i, qualifierValue.getBytes());
      versions.add(i);
    }

    storeClient = new AsyncStoreClientImpl(config, Optional.empty(), timeoutInSeconds, registry);
    GetDataBuilder builder = new GetDataBuilder(FULL_TABLE_NAME);
    GetRow row = builder.withRowKey(rowKey.getBytes()).withMaxVersions(11).build();

    Get get = AsyncStoreClientUtis.buildGets(Arrays.asList(row), new HashMap<>()).get(0);
    when(table.get(eq(get))).thenReturn(
            CompletableFuture.completedFuture(buildResponse(rowKey, columnFamily, columnQualifier, versionedCells)));
    ResultMap map = storeClient.get(row).get();
    assertNotNull("Expect result to be not null", map);
    assertTrue("Expect column family to be present", map.containsKey(columnFamily));
    assertTrue("Expect column to be present in column family", map.get(columnFamily).containsKey(columnQualifier));
    assertTrue("Qualifier value should be equal",
            Bytes.toString(map.get(columnFamily).get(columnQualifier).getValue()).equals(qualifierValue));
    assertTrue("Qualifier First entry value should be equal",
            Bytes.toString(map.get(columnFamily).get(columnQualifier).getVersionedValues().firstEntry().getValue()).equals(qualifierValue));
    assertEquals("Qualifier First entry key should be equal",
            9l, (long) map.get(columnFamily).get(columnQualifier).getVersionedValues().firstEntry().getKey());
    assertEquals("Versions fetched should be equals to",
            10l, map.get(columnFamily).get(columnQualifier).getVersionedValues().size());
    assertTrue("Versions fetched should be equals to",
            map.get(columnFamily).get(columnQualifier).getVersionedValues().keySet().containsAll(versions));
  }

  @Test
  public void testGetWithNegativeVersionNumber()
          throws StoreException, IOException, ExecutionException, InterruptedException, TimeoutException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    String qualifierValue = null;
    TreeMap<Long, byte[]> versionedCells = new TreeMap(new Comparator<Long>() {
      public int compare(Long l1, Long l2) {
        return l2.compareTo(l1);
      }
    });

    Set<Long> versions = Sets.newHashSet();
    for(long i=0; i<10; i++){
      qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
      versionedCells.put(i, qualifierValue.getBytes());
      versions.add(i);
    }

    storeClient = new AsyncStoreClientImpl(config, Optional.empty(), timeoutInSeconds, registry);
    GetDataBuilder builder = new GetDataBuilder(FULL_TABLE_NAME);
    GetRow row = builder.withRowKey(rowKey.getBytes()).withMaxVersions(-1).build();

    Get get = AsyncStoreClientUtis.buildGets(Arrays.asList(row), new HashMap<>()).get(0);
    when(table.get(eq(get))).thenReturn(
            CompletableFuture.completedFuture(buildResponse(rowKey, columnFamily, columnQualifier, versionedCells)));
    ResultMap map = storeClient.get(row).get();
    assertNotNull("Expect result to be not null", map);
    assertTrue("Expect column family to be present", map.containsKey(columnFamily));
    assertTrue("Expect column to be present in column family", map.get(columnFamily).containsKey(columnQualifier));
    assertTrue("Qualifier value should be equal",
            Bytes.toString(map.get(columnFamily).get(columnQualifier).getValue()).equals(qualifierValue));
    assertTrue("Qualifier First entry value should be equal",
            Bytes.toString(map.get(columnFamily).get(columnQualifier).getVersionedValues().firstEntry().getValue()).equals(qualifierValue));
    assertEquals("Qualifier First entry key should be equal",
            9l, (long) map.get(columnFamily).get(columnQualifier).getVersionedValues().firstEntry().getKey());
    assertEquals("Versions fetched should be equals to",
            10l, map.get(columnFamily).get(columnQualifier).getVersionedValues().size());
    assertTrue("Versions fetched should be equals to",
            map.get(columnFamily).get(columnQualifier).getVersionedValues().keySet().containsAll(versions));

  }

  @Test public void testGetWithNoDataFoundException() throws InterruptedException, StoreException, IOException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String errorMessage = "Data Not Found";

    byte[] rowKeyInBytes = keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes());
    GetDataBuilder builder = new GetDataBuilder(FULL_TABLE_NAME);
    GetRow row = builder.withRowKey(rowKey.getBytes()).build();

    Get get = AsyncStoreClientUtis.buildGets(Arrays.asList(row), keyDistributorMap).get(0);
    StoreData storeData = new StoreDataBuilder(FULL_TABLE_NAME).build();
    when(table.get(eq(get)))
        .thenReturn(CompletableFuture.completedFuture(buildResultData(storeData.getRow(), storeData.getCfs())));
    try {
      storeClient.get(row).get();
      assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof StoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }

  @Test public void testGetWithException() throws InterruptedException, StoreException, IOException {
    String rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String errorMessage = "Failed with exception";

    byte[] rowKeyInBytes = keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(rowKey.getBytes());
    GetDataBuilder builder = new GetDataBuilder(FULL_TABLE_NAME);
    GetRow row = builder.withRowKey(rowKey.getBytes()).build();

    Get get = AsyncStoreClientUtis.buildGets(Arrays.asList(row), keyDistributorMap).get(0);
    CompletableFuture<Void> future = new CompletableFuture<>();
    future.completeExceptionally(new StoreException(errorMessage));
    when(table.get(eq(get))).thenReturn(future);
    try {
      storeClient.get(row).get();
      assertTrue("Should have thrown exception", false);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof StoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }
}
