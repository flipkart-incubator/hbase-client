package com.flipkart.yak.client.daemons;

import com.flipkart.yak.distributor.KeyDistributor;
import com.flipkart.yak.distributor.NoDistribution;
import com.flipkart.yak.models.IndexDeleteData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncIndexPurgeDaemon implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncIndexPurgeDaemon.class);

  private final AsyncConnection connection;
  private final BlockingQueue<IndexDeleteData> indexQueue;
  private final Map<String, KeyDistributor> keyDistributorMap;

  public AsyncIndexPurgeDaemon(AsyncConnection conn, BlockingQueue<IndexDeleteData> queue,
      Map<String, KeyDistributor> keyDistributorMap) {
    this.connection = conn;
    this.indexQueue = queue;
    this.keyDistributorMap = keyDistributorMap;
  }

  @Override public void run() {
    LOG.trace("Index purge delete invoked");
    IndexDeleteData data = indexQueue.poll();
    // batch delete calls per table
    Map<String, List<Delete>> batchMap = new HashMap<>();
    while (data != null) {
      if (!batchMap.containsKey(data.getIndexTableName())) {
        batchMap.put(data.getIndexTableName(), new ArrayList<>());
      }
      Delete del = buildDelete(data);
      if (del == null) {
        continue; // case when deleteBuild failed should not fail other
      }
      batchMap.get(data.getIndexTableName()).add(del); // build
      data = indexQueue.poll();
    }

    if (!batchMap.isEmpty()) {
      flush(batchMap);
    }
  }

  private Delete buildDelete(IndexDeleteData data) {
    String indexKeyAsString = Bytes.toString(data.getIndexKey());
    try {
      KeyDistributor keyDistributor = keyDistributorMap.getOrDefault(data.getTableName(), NoDistribution.INSTANCE);
      KeyDistributor indexKeyDistributor = (keyDistributorMap.containsKey(data.getIndexTableName())) ?
          keyDistributorMap.get(data.getIndexTableName()) :
          keyDistributor;
      byte[] key = indexKeyDistributor.enrichKey(data.getIndexKey());
      Delete del = new Delete(key);
      del.addColumns(data.getCf(), data.getQualifier());
      return del;
    } catch (Exception ex) {
      LOG.error("Failed to build delete index key: {}, error: {}", indexKeyAsString, ex.getMessage(), ex);
    }
    return null;
  }

  private void flush(Map<String, List<Delete>> delMap) {
    for (Map.Entry<String, List<Delete>> tableEntry : delMap.entrySet()) {
      AsyncTable table = connection.getTable(TableName.valueOf(tableEntry.getKey()));
      List<CompletableFuture<Void>> completableFuture = table.delete(tableEntry.getValue());

      completableFuture.stream().forEach(future -> future.whenCompleteAsync((value, error) -> {
        if (error != null) {
          LOG.error("Error in deleting index with key: {}, error: {}", tableEntry.getKey(), error.getMessage(), error);
        }
      }));
    }
  }
}
