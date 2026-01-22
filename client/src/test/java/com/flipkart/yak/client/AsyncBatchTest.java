package com.flipkart.yak.client;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.exceptions.RequestValidatorException;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.distributor.KeyDistributor;
import com.flipkart.yak.distributor.MurmusHashDistribution;
import com.flipkart.yak.models.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Captor;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class AsyncBatchTest extends AsyncBaseTest {

  @Captor
  ArgumentCaptor<List<Row>> captor;

  private AsyncStoreClient storeClient;
  private Map<String, KeyDistributor> keyDistributorMap = new HashMap<>();
  private MetricRegistry registry = new MetricRegistry();
  private int timeoutInSeconds = 30;
  private SiteConfig config;
  private DeleteData deleteData;
  private StoreData storeData;
  private List<Row> actionList;
  private AsyncStoreClientUtis.BatchActions batchActions;
  private BatchData batchData;
  List<CompletableFuture<Void>> futures;

  @Before public void setUp() throws Exception {
    super.setup();
    config = buildStoreClientConfig();
    keyDistributorMap.put(FULL_TABLE_NAME, new MurmusHashDistribution(128));
    storeClient = new AsyncStoreClientImpl(config, Optional.of(keyDistributorMap), timeoutInSeconds, registry);

    String rowKey1 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnFamily1 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String columnQualifier1 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    String qualifierValue1 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();

    storeData = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes())
            .addColumn(columnFamily1, columnQualifier1, qualifierValue1.getBytes()).build();

    String rowKey2 = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    deleteData = new DeleteDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey2.getBytes()).build();

    batchData = new BatchDataBuilder(FULL_TABLE_NAME)
            .addStoreData(storeData)
            .addDeleteData(deleteData)
            .build();
    batchActions = AsyncStoreClientUtis.buildBatch(batchData, keyDistributorMap, Optional.of(config.getDurabilityThreshold()));
    actionList = batchActions.actions;

    futures = new ArrayList<>();
    futures.add(CompletableFuture.completedFuture(null));
    futures.add(CompletableFuture.completedFuture(null));
  }

  @After public void tearDown() throws Exception {
    storeClient.shutdown();
  }

  private class BatchMatcher extends ArgumentMatcher<List<Row>> {
    private List<Row> left;

    public BatchMatcher(List<Row> left) {
      this.left = left;
    }

    @Override public boolean matches(Object argument) {
      List<Row> right = (List<Row>) argument;
      if (right.size() != left.size()) {
        return false;
      }

      for (int index = 0; index < right.size(); index += 1) {
        Row rightItem = right.get(index);
        Row leftItem = left.get(index);

        if(right instanceof Delete && left instanceof Delete) {
          boolean success = Arrays.equals(rightItem.getRow(), leftItem.getRow()) && ((Delete)rightItem).getFamilyCellMap()
                  .equals(((Delete) leftItem).getFamilyCellMap());
          if (!success) {
            return false;
          }
        }else if(right instanceof Put && left instanceof Put) {
          boolean success = Arrays.equals(rightItem.getRow(), leftItem.getRow()) && ((Put)rightItem).getFamilyCellMap()
                  .equals(((Put) leftItem).getFamilyCellMap()) && ((Put)rightItem).getDurability().equals(((Put)leftItem).getDurability());
          if (!success) {
            return false;
          }
        } else {
          return  false;
        }
      }
      return true;
    }
  }

  @Test public void testBatch() throws ExecutionException, InterruptedException {
    when(table.batch(argThat(new BatchMatcher(actionList)))).thenAnswer(invocation -> futures);

    storeClient.batch(batchData).get();

    verify(table, times(1)).batch(captor.capture());
    List<Row> capturedActions = captor.getValue();
    assertTrue("Should contain 2 actions", capturedActions.size() == 2);
    assertTrue("First action should be Put", capturedActions.get(0) instanceof Put);
    assertTrue("Second action should be Delete", capturedActions.get(1) instanceof Delete);
  }

  @Test public void testBatchWithException() throws ExecutionException, InterruptedException {
    String errorMessage = "Failed to delete";

    futures.get(0).completeExceptionally(new StoreException(errorMessage));
    futures.get(1).complete(null);

    when(table.batch(argThat(new BatchMatcher(actionList)))).thenAnswer(invocation -> futures);

    try {
      storeClient.batch(batchData).get();
    } catch (Exception ex) {
      assertNotNull("Expect StoreException to be thrown", ex.getCause());
      assertTrue("Expect StoreException to be thrown", ex.getCause() instanceof StoreException);
      assertTrue("Expect exception message to be: " + errorMessage, ex.getCause().getMessage().equals(errorMessage));
    }
  }

  @Test public void testBatchWithTableNameBatchValidationFailures() throws Exception {
        // Create BatchData with two StoreData objects from different tables
        String anotherTable = FULL_TABLE_NAME + "_DIFFERENT";
        StoreData storeData1 = new StoreDataBuilder(FULL_TABLE_NAME)
                .withRowKey(RandomStringUtils.randomAlphanumeric(10).toUpperCase().getBytes())
                .addColumn("cf", "cq", "val".getBytes())
                .build();
        StoreData storeData2 = new StoreDataBuilder(anotherTable)
                .withRowKey(RandomStringUtils.randomAlphanumeric(10).toUpperCase().getBytes())
                .addColumn("cf", "cq", "val".getBytes())
                .build();
        BatchData invalidBatchData = new BatchDataBuilder(FULL_TABLE_NAME)
                .addStoreData(storeData1)
                .addStoreData(storeData2)
                .build();

        try {
            storeClient.batch(invalidBatchData).get();
            fail("Should have thrown RequestValidatorException");
        } catch (Exception ex) {
            Throwable cause = ex.getCause();
            assertNotNull("Expect RequestValidatorException to be thrown", cause);
            assertTrue("Expect RequestValidatorException", cause instanceof RequestValidatorException);
            assertTrue("Expect exception message to mention one table", cause.getMessage().contains("Batch request should only come for one table"));
        }
    }
}