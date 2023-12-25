package com.flipkart.yak.client;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.daemons.AsyncIndexPurgeDaemon;
import com.flipkart.yak.distributor.KeyDistributor;
import com.flipkart.yak.distributor.MurmusHashDistribution;
import com.flipkart.yak.models.DeleteData;
import com.flipkart.yak.models.DeleteDataBuilder;
import com.flipkart.yak.models.IndexDeleteData;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AsyncIndexPurgeDaemonTest extends AsyncBaseTest {
  private AsyncStoreClient storeClient;
  Map<String, KeyDistributor> keyDistributorMap = new HashMap<>();
  MetricRegistry registry = new MetricRegistry();
  int timeoutInSeconds = 30;
  SiteConfig config;

  private String rowKey;
  private String cf;
  private String cq;
  private String qv;
  private String indexKey;

  @Before public void setup() throws Exception {
    super.setup();
    config = buildStoreClientConfig();
    keyDistributorMap.put(FULL_TABLE_NAME, new MurmusHashDistribution(128));
    keyDistributorMap.put(FULL_TABLE_NAME_INDEX, new MurmusHashDistribution(128));
    storeClient = new AsyncStoreClientImpl(config, Optional.of(keyDistributorMap), timeoutInSeconds, registry);

    rowKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    cf = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    cq = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    qv = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
    indexKey = RandomStringUtils.randomAlphanumeric(10).toUpperCase();
  }

  @After public void tearDown() throws Exception {
    this.storeClient.shutdown();
  }

  static Delete buildDeleteQuery(IndexDeleteData del) {
    Delete deleteRow = new Delete(del.getIndexKey());
    deleteRow.addColumn(del.getCf(), del.getQualifier());
    return deleteRow;
  }

  protected class BatchDeleteMatcher extends ArgumentMatcher<ArrayList<Delete>> {
    private ArrayList<Delete> left;

    public BatchDeleteMatcher(ArrayList<Delete> left) {
      this.left = left;
    }

    @Override public boolean matches(Object argument) {
      if (argument == null) {
        return true;
      }
      ArrayList<Delete> right = (ArrayList<Delete>) argument;
      if (right.size() != left.size()) {
        return false;
      }

      for (int index = 0; index < right.size(); index += 1) {
        Delete rightItem = right.get(index);
        Delete leftItem = left.get(index);
        boolean success = Arrays.equals(rightItem.getRow(), leftItem.getRow()) && rightItem.getFamilyCellMap()
            .equals(leftItem.getFamilyCellMap());
        if (!success) {
          return false;
        }
      }
      return true;
    }
  }

  // TODO: This is incomplete
  @Test public void indexPurgeDaemonTest() throws InterruptedException, IOException, ExecutionException {
    BlockingQueue<IndexDeleteData> blockingQueue = new ArrayBlockingQueue<>(100);
    AsyncIndexPurgeDaemon indexPurgeDaemon = new AsyncIndexPurgeDaemon(connection, blockingQueue, new HashMap<>());

    IndexDeleteData indexDeleteData =
        new IndexDeleteData(FULL_TABLE_NAME, cf.getBytes(), cq.getBytes(), indexKey.getBytes(), rowKey.getBytes());

    blockingQueue.put(indexDeleteData);
    indexPurgeDaemon.run();
    byte[] key = keyDistributorMap.get(FULL_TABLE_NAME).enrichKey(indexDeleteData.getIndexKey());
    Delete delete = buildDeleteQuery(indexDeleteData);

    ArrayList<Delete> deleteList = new ArrayList<>();
    deleteList.add(delete);

    when(indexTable.delete(argThat(new BatchDeleteMatcher(deleteList))))
        .thenReturn(Arrays.asList(CompletableFuture.completedFuture(null)));

    DeleteData data = new DeleteDataBuilder(FULL_TABLE_NAME).withRowKey(indexKey.getBytes()).addColumn(cf, cq).build();

    verify(indexTable).delete((List<Delete>) argumentCaptor.capture());
    List<Delete> deletes = (List<Delete>) argumentCaptor.getValue();

    Cell cell = delete.getFamilyCellMap().get(cf.getBytes()).iterator().next();
    assertEquals(Bytes.toString(delete.getRow()), indexKey);
    assertEquals(
        Bytes.toString(Bytes.copy(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())), cq);
  }
}