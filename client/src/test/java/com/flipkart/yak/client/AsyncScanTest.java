package com.flipkart.yak.client;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.distributor.KeyDistributor;
import com.flipkart.yak.distributor.NoDistribution;
import com.flipkart.yak.models.ResultMap;
import com.flipkart.yak.models.ScanData;
import com.flipkart.yak.models.ScanDataBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class) public class AsyncScanTest extends AsyncBaseTest {

  private AsyncStoreClient storeClient;
  Map<String, KeyDistributor> keyDistributorMap = new HashMap<>();
  MetricRegistry registry = new MetricRegistry();
  int timeoutInSeconds = 30;
  SiteConfig config;

  private String rowKey;
  private String columnFamily;
  private String columnQualifier;
  private String qualifierValue;
  private ScanData scanData;

  @Before public void setUp() throws Exception {
    super.setup();
    config = buildStoreClientConfig();
    keyDistributorMap.put(FULL_TABLE_NAME, NoDistribution.INSTANCE);
    storeClient = new AsyncStoreClientImpl(config, Optional.of(keyDistributorMap), timeoutInSeconds, registry);

    rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    columnFamily = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    columnQualifier = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    qualifierValue = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    FilterList filterList = new FilterList();
    filterList.addFilter(new ColumnPrefixFilter(rowKey.getBytes()));
    scanData = (new ScanDataBuilder(FULL_TABLE_NAME)).withFilterList(filterList).withLimit(1)
        .withStartRow("0".getBytes()).withStopRow("9".getBytes()).build();
  }

  private class ScanMatcher extends ArgumentMatcher<Scan> {
    private Scan left;

    public ScanMatcher(Scan left) {
      this.left = left;
    }

    @Override public boolean matches(Object argument) {
      Scan right = (Scan) argument;
      return right.getFilter().equals(left.getFilter()) && right.getStartRow().equals(left.getStartRow()) && right
          .getStopRow().equals(left.getStopRow()) && right.getMaxVersions() == left.getMaxVersions()
          && right.getBatch() == left.getBatch() && right.getAllowPartialResults() == left.getAllowPartialResults()
          && right.getMaxResultSize() == left.getMaxResultSize() && right.getCacheBlocks() == left.getCacheBlocks()
          && right.isReversed() == left.isReversed() && right.getCaching() == left.getCaching()
          && right.getLimit() == left.getLimit() && right.getFamilyMap().equals(left.getFamilyMap());
    }
  }

  @After public void tearDown() throws Exception {
    storeClient.shutdown();
  }

  @Test public void testScan() throws StoreException, IOException, ExecutionException, InterruptedException {
    Scan scan = AsyncStoreClientUtis.buildScan(scanData, keyDistributorMap);
    List<Result> results = Arrays.asList(buildResponse(rowKey, columnFamily, columnQualifier, qualifierValue));
    when(table.scanAll(argThat(new ScanMatcher(scan)))).thenReturn(CompletableFuture.completedFuture(results));

    Map<String, ResultMap> resultMaps = storeClient.scan(scanData).get();
    assertNotNull("Expect result to be not null", resultMaps);
    assertEquals("Expect result to be of size 1", 1, resultMaps.size());
    assertTrue("Expect column family to be present", resultMaps.get(rowKey).containsKey(columnFamily));
    assertTrue("Expect column to be present in column family",
        resultMaps.get(rowKey).get(columnFamily).containsKey(columnQualifier));
    assertTrue("Qualifier value should be equal",
        Bytes.toString(resultMaps.get(rowKey).get(columnFamily).get(columnQualifier).getValue()).equals(qualifierValue));
    verify(table, times(1)).scanAll(argThat(new ScanMatcher(scan)));
  }


  /*
    For a scan with start and end row, yak/hbase returns a LIST of rows.
    We want to preserve this order when the data is returned from the Async Client as a MAP.
   */
  @Test public void testScanResultOrderEqualsResultListOrderFromYak() throws StoreException, IOException, ExecutionException, InterruptedException {
    Scan scan = AsyncStoreClientUtis.buildScan(scanData, keyDistributorMap);
    List<Result> results = new ArrayList<>();

    for(int i=0; i<100; i++){
      results.add(buildResponse(fetchRandomString(), columnFamily, columnQualifier, fetchRandomString()));
    }

    when(table.scanAll(argThat(new ScanMatcher(scan)))).thenReturn(CompletableFuture.completedFuture(results));

    Map<String, ResultMap> resultMaps = storeClient.scan(scanData).get();
    assertNotNull("Expect result to be not null", resultMaps);
    assertEquals("Expect result to be of size 100", 100, resultMaps.size());

    int resultIndex = 0;
    for(String key: resultMaps.keySet()){
      String expectedKey = Bytes.toString(results.get(resultIndex).getRow());
      assertEquals("Keys at the same index should be same in Client Scan Response and HBase/Yak response", expectedKey, key);
      assertTrue("Expect column family to be present", resultMaps.get(key).containsKey(columnFamily));
      assertTrue("Expect column to be present in column family",
              resultMaps.get(key).get(columnFamily).containsKey(columnQualifier));

      String expectedColumnValue = Bytes.toString(results.get(resultIndex).getValue(columnFamily.getBytes(), columnQualifier.getBytes()));

      assertEquals("Qualifier value should be equal", expectedColumnValue,
              Bytes.toString(resultMaps.get(key).get(columnFamily).get(columnQualifier).getValue()));
      resultIndex++;
    }

    verify(table, times(1)).scanAll(argThat(new ScanMatcher(scan)));
  }

  @Test public void testScanWithInputValidationFailed()
      throws StoreException, IOException, ExecutionException, InterruptedException {
    scanData = (new ScanDataBuilder(FULL_TABLE_NAME)).withLimit(1).build();
    try {
      storeClient.scan(scanData).get();
      assertFalse("Should have thrown exception", true);
    } catch (Exception ex) {
      assertNotNull("Expect result to be not null", ex.getCause());
      assertTrue("Should throw StoreException", ex.getCause() instanceof StoreException);
      assertTrue("Should exception match",
          ex.getCause().getMessage().equals("startRow and stopRow can not be null/empty"));
    }
    verify(table, times(0)).scanAll(any());

    long now = (new Date()).getTime();
    scanData =
        (new ScanDataBuilder(FULL_TABLE_NAME)).withStartRow("0".getBytes()).withStopRow("9".getBytes())
            .withMinStamp(now).withMaxStamp(now - 10).build();
    try {
      storeClient.scan(scanData).get();
      assertFalse("Should have thrown exception", true);
    } catch (Exception ex) {
      assertTrue("Should throw StoreException", ex instanceof IllegalArgumentException);
      assertTrue("Should exception match", ex.getMessage().equals("maxStamp is smaller than minStamp"));
    }
    verify(table, times(0)).scanAll(any());
  }

  @Test public void testScanWithException()
      throws StoreException, IOException, ExecutionException, InterruptedException {
    String errorMessage = "Failed to put";
    CompletableFuture<Void> future = new CompletableFuture<>();
    future.completeExceptionally(new StoreException(errorMessage));
    Scan scan = AsyncStoreClientUtis.buildScan(scanData, keyDistributorMap);
    when(table.scanAll(argThat(new ScanMatcher(scan)))).thenReturn(future);

    try {
      storeClient.scan(scanData).get();
      assertFalse("Should have thrown exception", true);
    } catch (ExecutionException ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof StoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
    verify(table, times(1)).scanAll(argThat(new ScanMatcher(scan)));
  }

  private String fetchRandomString(){
    return RandomStringUtils.randomAlphanumeric(10).toUpperCase();
  }
}
