package com.flipkart.yak.client.validator;

import com.flipkart.yak.client.exceptions.PayloadValidatorException;
import com.flipkart.yak.models.Cell;
import com.flipkart.yak.models.ColumnsMap;
import com.flipkart.yak.models.StoreData;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncPayloadValidatorImpl implements AsyncPayloadValidator {

  private static final String PAYLOAD_THREASHOLD = "hbase.yak.payload.threshold";
  private static final Integer DEFAULT_THRESHOLD = 10240; //10KB
  private static final Logger logger = LoggerFactory.getLogger(AsyncPayloadValidatorImpl.class);

  //concurrent read and writes to this are possible, but ideally should not cause any issues
  protected final ConcurrentMap<TableName, Integer> dataSizeInBytesPerRequestForTable = new ConcurrentHashMap<>(20);
  // 20 should ensure it does not resize, rare case of having > 20 tables -
  private final AsyncConnection conn;

  public AsyncPayloadValidatorImpl(AsyncConnection conn) {
    this.conn = conn;
  }

  private int getPayloadSize(StoreData storeData) {
    int len = 0;
    for (Map.Entry<String, ColumnsMap> cfEntry : storeData.getCfs().entrySet()) {
      ColumnsMap map = cfEntry.getValue();
      for (Map.Entry<String, Cell> colEntry : map.entrySet()) {
        Cell cell = colEntry.getValue();
        if (cell.getValue() != null) {
          len += cell.getValue().length;
        }
      }
    }
    return len;
  }

  @SuppressWarnings("java:S3776")
  @Override public CompletableFuture<Void> validate(List<StoreData> dataList) {
    if (dataList == null || dataList.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    TableName table = TableName.valueOf(dataList.get(0).getTableName()); // Assuming all of puts are from same table
    CompletableFuture<Void> refreshFuture = new CompletableFuture<>();
    CompletableFuture<Void> responseFuture = new CompletableFuture<>();

    if (!dataSizeInBytesPerRequestForTable.containsKey(table)) {
      refreshFuture = refresh(table);
    } else {
      refreshFuture.complete(null);
    }

    refreshFuture.whenCompleteAsync((value, error) -> {
      if (error != null) {
        logger.error("Failed in refresh " + error.getMessage(), error);
        responseFuture.completeExceptionally(error);
      } else {
        int thSize = dataSizeInBytesPerRequestForTable.get(table);

        boolean hasFailed = false;
        int failedPayloadSize = 0;
        for (StoreData data : dataList) {
          int payloadSize = getPayloadSize(data);
          if (payloadSize > thSize) {
            logger.error("Payload size breach threshod {}, got {}, tableName: {}, row: {}", thSize, payloadSize,
                data.getTableName(), data.getRow());
            hasFailed = true;
            failedPayloadSize = payloadSize;
            break;
          }
        }

        if (!hasFailed) {
          responseFuture.complete(null);
        } else {
          responseFuture.completeExceptionally(
              new PayloadValidatorException("Payload size breach threshod " + thSize + ", got " + failedPayloadSize));
        }
      }
    });
    return responseFuture;
  }

  @Override public CompletableFuture<Void> refresh(TableName table) {
    CompletableFuture<Void> response = new CompletableFuture<>();
    if (dataSizeInBytesPerRequestForTable.containsKey(table)) {
      return response;
    } else {
      AsyncAdmin admin = conn.getAdmin();
      CompletableFuture<NamespaceDescriptor> future = admin.getNamespaceDescriptor(table.getNamespaceAsString());
      future.whenCompleteAsync((ns, error) -> {
        if (error != null || ns == null) {
          if (error == null) {
            error =
                new PayloadValidatorException("Failed to fetch NamespaceDescriptor for refresh for payload validate");
          }
          response.completeExceptionally(error);
        } else {
          String val = ns.getConfigurationValue(PAYLOAD_THREASHOLD);
          int th = (val == null) ? DEFAULT_THRESHOLD : Integer.parseInt(val);
          dataSizeInBytesPerRequestForTable.putIfAbsent(table, th);
          response.complete(null);
        }
      });
    }
    return response;
  }
}
