package com.flipkart.yak.client;

import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.models.Cell;
import com.flipkart.yak.models.ColumnsMap;
import com.flipkart.yak.models.StoreData;
import com.flipkart.yak.models.StoreDataBuilder;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Captor;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import static org.powermock.api.mockito.PowerMockito.when;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class) @PrepareForTest({ HBaseConfiguration.class, Executors.class, ConnectionFactory.class })
@PowerMockIgnore({ "javax.management.*", "javax.xml.parsers.*", "com.sun.org.apache.xerces.internal.jaxp.*",
    "ch.qos.logback.*", "org.slf4j.*" }) public abstract class AsyncBaseTest {

  @Mock protected Configuration config;

  @Mock protected AsyncConnection connection;

  @Mock protected AsyncAdmin admin;

  @Mock protected NamespaceDescriptor namespaceDescriptor;

  @Mock protected AsyncTable table;

  @Mock protected AsyncTable indexTable;

  @Captor protected ArgumentCaptor argumentCaptor;

  @Captor protected ArgumentCaptor<byte[]> bytesCapture;

  protected final String PAYLOAD_THREASHOLD = "hbase.yak.payload.threshold";
  protected final String TABLE_NAME = "RandomTableName";
  protected final String NAMESPACE_NAME = "RandomNamespaceName";
  protected final String FULL_TABLE_NAME = NAMESPACE_NAME + ":" + TABLE_NAME;
  protected final String FULL_TABLE_NAME_INDEX = FULL_TABLE_NAME + "_index";
  protected Random random = new Random();

  @Before public void setup() throws Exception {
    MockitoAnnotations.initMocks(this);

    PowerMockito.mockStatic(HBaseConfiguration.class);
    when(HBaseConfiguration.create()).thenReturn(config);

    PowerMockito.mockStatic(Executors.class);

    PowerMockito.mockStatic(ConnectionFactory.class);
    when(ConnectionFactory.createAsyncConnection(eq(config))).thenReturn(CompletableFuture.completedFuture(connection));
    when(connection.getAdmin()).thenReturn(admin);
    when(admin.getNamespaceDescriptor(anyString())).thenReturn(CompletableFuture.completedFuture(namespaceDescriptor));
    when(connection.getTable(TableName.valueOf(FULL_TABLE_NAME))).thenReturn(table);
    when(connection.getTable(TableName.valueOf(FULL_TABLE_NAME_INDEX))).thenReturn(indexTable);
  }

  public static SiteConfig buildStoreClientConfig() throws Exception {
    Map<String, String> hbaseConfig = new HashMap<>();
    hbaseConfig.put("hbase.zookeeper.quorum", "preprod-yak-zk-1,preprod-yak-zk-2,preprod-yak-zk-3");
    hbaseConfig.put("hbase.yak.payload.threshold", "102400");
    hbaseConfig.put("hbase.client.retries.number", "2");
    hbaseConfig.put("hbase.client.max.perserver.tasks", "10");
    hbaseConfig.put("hbase.client.max.perregion.tasks", "1");
    hbaseConfig.put("hbase.client.ipc.pool.type", "RoundRobinPool");
    hbaseConfig.put("hbase.client.ipc.pool.size", "10");
    hbaseConfig.put("hbase.client.operation.timeout", "5000");
    hbaseConfig.put("hbase.client.meta.operation.timeout", "5000");
    hbaseConfig.put("hbase.rpc.timeout", "2000");
    hbaseConfig.put("hbase.rpc.shortoperation.timeout", "2000");
    hbaseConfig.put("zookeeper.recovery.retry", "0");
    hbaseConfig.put("zookeeper.session.timeout", "100");
    hbaseConfig.put("hbase.client.pause", "50");
    SiteConfig config = (new SiteConfig()).withDurabilityThreshold(Durability.SYNC_WAL).withStoreName("keyvalueStore")
        .withHbaseConfig(hbaseConfig);
    return config;
  }

  protected Result buildResultData(byte[] rowKey, Map<String, ColumnsMap> cfs) throws StoreException, IOException {

    TreeSet<KeyValue> set = new TreeSet<>(KeyValue.COMPARATOR);
    if (cfs != null && !cfs.isEmpty()) {
      for (String cfName : cfs.keySet()) {
        ColumnsMap map = cfs.get(cfName);
        for (String columnName : map.keySet()) {
          Cell cell = map.get(columnName);
          if (cell.getVersionedValues() != null) {
            for (Map.Entry<Long, byte[]> entry : cell.getVersionedValues().entrySet()) {
              set.add(new KeyValue(rowKey, cfName.getBytes(), columnName.getBytes(), entry.getKey(), entry.getValue()));
            }
          } else {
            set.add(new KeyValue(rowKey, cfName.getBytes(), columnName.getBytes(), cell.getValue()));
          }

        }
      }
    }
    KeyValue[] kvs = new KeyValue[set.size()];
    set.toArray(kvs);
    return Result.create(kvs);
  }

  protected class GetMatcher extends ArgumentMatcher<Get> {
    private Get left;

    public GetMatcher(Get left) {
      this.left = left;
    }

    @Override public boolean matches(Object argument) {
      Get right = (Get) argument;
      return Arrays.equals(right.getRow(), left.getRow()) && right.getFamilyMap().equals(left.getFamilyMap());
    }
  }

  protected class DeleteMatcher extends ArgumentMatcher<Delete> {
    private Delete left;

    public DeleteMatcher(Delete left) {
      this.left = left;
    }

    @Override public boolean matches(Object argument) {
      Delete right = (Delete) argument;
      return Arrays.equals(right.getRow(), left.getRow()) && right.getFamilyCellMap().equals(left.getFamilyCellMap());
    }
  }

  protected class BatchGetMatcher extends ArgumentMatcher<List<Get>> {
    private List<Get> left;

    public BatchGetMatcher(List<Get> left) {
      this.left = left;
    }

    @Override public boolean matches(Object argument) {
      List<Get> right = (List<Get>) argument;
      if (right == null) {
        return true;
      }
      if (right.size() != left.size()) {
        return false;
      }

      for (int index = 0; index < right.size(); index += 1) {
        Get rightItem = right.get(index);
        Get leftItem = left.get(index);
        boolean success = Arrays.equals(rightItem.getRow(), leftItem.getRow()) && rightItem.getFamilyMap()
            .equals(leftItem.getFamilyMap());
        if (!success) {
          return false;
        }
      }
      return true;
    }
  }

  protected class BatchDeleteMatcher extends ArgumentMatcher<List<Delete>> {
    private List<Delete> left;

    public BatchDeleteMatcher(List<Delete> left) {
      this.left = left;
    }

    @Override public boolean matches(Object argument) {
      List<Delete> right = (List<Delete>) argument;
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

  protected Result buildResponse(String rowkey, List<String> cf, List<String> cq, List<String> value)
      throws StoreException, IOException {
    StoreDataBuilder builder = new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowkey.getBytes());
    for (int index = 0; index < cf.size(); index += 1) {
      builder.addColumn(cf.get(index), cq.get(index), value.get(index).getBytes());
    }
    StoreData storeData = builder.build();
    return buildResultData(storeData.getRow(), storeData.getCfs());
  }

  protected Result buildResponse(String rowkey, String cf, String cq, String value) throws StoreException, IOException {
    StoreData data =
        new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowkey.getBytes()).addColumn(cf, cq, value.getBytes()).build();
    return buildResultData(data.getRow(), data.getCfs());
  }

  protected Result buildResponse(String rowkey, List<String> cf, List<String> cq, ArrayList<TreeMap<Long, byte[]>> versionedValuesList)
          throws StoreException, IOException {
    Map<String, ColumnsMap> cfMap = new HashMap();
    ColumnsMap colMap = new ColumnsMap();

    for (int index = 0; index < cf.size(); index += 1) {
      Cell cell = new Cell(versionedValuesList.get(index));
      colMap.put(cq.get(index), cell);
      cfMap.put(cf.get(index), colMap);

    }
    return buildResultData(Bytes.toBytes(rowkey), cfMap);
  }

  protected Result buildResponse(String rowkey, String cf, String cq, TreeMap<Long, byte[]> versionedValues) throws StoreException, IOException {
    return buildResponse(rowkey, Lists.newArrayList(cf), Lists.newArrayList(cq), Lists.newArrayList(versionedValues));
  }
}
