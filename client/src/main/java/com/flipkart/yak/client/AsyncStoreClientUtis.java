package com.flipkart.yak.client;

import com.flipkart.yak.client.config.SiteConfig;
import com.flipkart.yak.client.exceptions.StoreException;
import com.flipkart.yak.distributor.KeyDistributor;
import com.flipkart.yak.distributor.NoDistribution;
import com.flipkart.yak.models.*;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ReadOnlyZKClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncStoreClientUtis {
  private static final Logger logger = LoggerFactory.getLogger(AsyncStoreClientUtis.class);
  private static final int ZOOKEEPER_PORT = 2181;
  private static final int ZOOKEEPER_TIMEOUT = 60000;
  private static final int ZOOKEEPER_RETRY = 3;
  private static final int RETRY_COUNT = 3;
  private static final int RPC_TIMEOUT = 5000;
  private static final String TRUE_STRING = "true";

  private AsyncStoreClientUtis() {
    throw new IllegalStateException();
  }

  static Configuration buildHbaseConfiguration(SiteConfig siteConfig) {
    Configuration hbaseConf = HBaseConfiguration.create();
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.toString(ZOOKEEPER_PORT));
    hbaseConf.set(HConstants.ZK_SESSION_TIMEOUT, Integer.toString(ZOOKEEPER_TIMEOUT));
    hbaseConf.set(ReadOnlyZKClient.RECOVERY_RETRY, Integer.toString(ZOOKEEPER_RETRY));
    hbaseConf.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, Integer.toString(RETRY_COUNT));
    hbaseConf.set(HConstants.HBASE_RPC_TIMEOUT_KEY, Integer.toString(RPC_TIMEOUT));
    hbaseConf.set(MetricsConnection.CLIENT_SIDE_METRICS_ENABLED_KEY, TRUE_STRING);

    for (String key : siteConfig.getHbaseConfig().keySet()) {
      hbaseConf.set(key, siteConfig.getHbaseConfig().get(key));
    }
    return hbaseConf;
  }

  static List<Put> getIndexPuts(StoreData data, Map.Entry<String, Cell> qualEntry, KeyDistributor indexKeyDistributor,
      Optional<Durability> durability) {

    /*
     * Current implementation puts onus on developer to specify
     * immutable index correctly, there is no defensive check from
     * code if index is getting mutated. CheckAndPut can be used to
     * add such defensive check, but it would one per Put and will
     * reduce performance compared to the current batch
     * implementation.
     */

    List<Put> indexPuts = new ArrayList<>();
    Cell column = qualEntry.getValue();
    if (column instanceof IndexableCell) {
      IndexableCell icolum = (IndexableCell) column;
      if (icolum.getIndexType() == IndexType.SIMPLE) {
        byte[] key = indexKeyDistributor.enrichKey(Bytes.toBytes(qualEntry.getKey()));
        Put iput = new Put(key);
        durability.ifPresent(iput::setDurability);
        data.getDurabilityOverride().ifPresent(iput::setDurability);
        data.getTTL().ifPresent(iput::setTTL);
        iput.addColumn(IndexConstants.INDEX_CF, IndexConstants.INDEX_COL, data.getRow());
        indexPuts.add(iput);
      } else { // APPEND_ONLY case
        byte[] key = indexKeyDistributor.enrichKey(Bytes.toBytes(qualEntry.getKey()));
        Put iput = new Put(key);
        durability.ifPresent(iput::setDurability);
        data.getDurabilityOverride().ifPresent(iput::setDurability);
        data.getTTL().ifPresent(iput::setTTL);
        iput.addColumn(IndexConstants.INDEX_CF, data.getRow(), column.getValue());
        indexPuts.add(iput);
      }
    }
    return indexPuts;
  }

  static StorePuts buildStorePuts(StoreData data, Map<String, KeyDistributor> keyDistributorPerTable,
      Optional<Durability> durability) {
    // setup key Distributors
    KeyDistributor keyDistributor = keyDistributorPerTable.getOrDefault(data.getTableName(), NoDistribution.INSTANCE);
    KeyDistributor indexKeyDistributor = keyDistributorPerTable.get(data.getIndexTableName());
    if (indexKeyDistributor == null) { // apply the one defined for parent table
      indexKeyDistributor = keyDistributor;
    }

    byte[] rowKey = keyDistributor.enrichKey(data.getRow(), data.getPartitionKey());
    Put put = new Put(rowKey);
    durability.ifPresent(put::setDurability);
    data.getDurabilityOverride().ifPresent(put::setDurability);
    data.getTTL().ifPresent(put::setTTL);
    List<Put> indexPuts = new ArrayList<>();
    for (Map.Entry<String, ColumnsMap> cfsEntry : data.getCfs().entrySet()) {
      Map<String, Cell> qualMap = cfsEntry.getValue();
      for (Map.Entry<String, Cell> qualEntry : qualMap.entrySet()) {
        Cell column = qualEntry.getValue();

        indexPuts.addAll(getIndexPuts(data, qualEntry, indexKeyDistributor, durability));
        put.addColumn(cfsEntry.getKey().getBytes(), qualEntry.getKey().getBytes(), column.getValue());
      }
    }
    StorePuts output = new StorePuts();
    output.entityPut = put;
    output.indexPuts = indexPuts;
    return output;
  }

  static Delete buildDelete(DeleteData data, Map<String, KeyDistributor> keyDistributorPerTable) {
    KeyDistributor keyDistributor = keyDistributorPerTable.getOrDefault(data.getTableName(), NoDistribution.INSTANCE);
    byte[] rowKey = keyDistributor.enrichKey(data.getRow(), data.getPartitionKey());
    Delete delete = new Delete(rowKey);
    for (Map.Entry<String, Set<String>> cfsEntry : data.getCfs().entrySet()) {
      for (String col : cfsEntry.getValue()) {
        delete.addColumn(cfsEntry.getKey().getBytes(), col.getBytes());
      }
    }
    return delete;
  }

  static BatchActions buildBatch(BatchData data, Map<String, KeyDistributor> keyDistributorPerTable,
      Optional<Durability> durability)  throws IOException {
    BatchActions batchActions = new BatchActions();
    batchActions.actions = new ArrayList<>();
    batchActions.indexPuts = new ArrayList<>();

    if (data.getStoreDataList() != null && !data.getStoreDataList().isEmpty()) {
      for (StoreData storeData : data.getStoreDataList()) {
        StorePuts storePuts = buildStorePuts(storeData, keyDistributorPerTable, durability);
        batchActions.actions.add(storePuts.entityPut);
        if (storePuts.indexPuts != null) {
          batchActions.indexPuts.addAll(storePuts.indexPuts);
        }
      }
    }

    if (data.getDeleteDataList() != null && !data.getDeleteDataList().isEmpty()) {
      for (DeleteData deleteData : data.getDeleteDataList()) {
        Delete delete = buildDelete(deleteData, keyDistributorPerTable);
        batchActions.actions.add(delete);
      }
    }
    return batchActions;
  }

  static Mutation buildMutation(MutateData mutateData, Map<String, KeyDistributor> keyDistributorPerTable,
                                Optional<Durability> durability) throws IOException {
    Mutation mutation = new Mutation();
    List<StorePuts> storePuts = new ArrayList<>();
    RowMutations rowMutations = null;
    if (mutateData.getStoreDataList() != null && !mutateData.getStoreDataList().isEmpty()) {
      for (StoreData storeData : mutateData.getStoreDataList()) {
        storePuts.add(buildStorePuts(storeData, keyDistributorPerTable, durability));
      }

      rowMutations = new RowMutations(storePuts.get(0).entityPut.getRow());
      mutation.indexPuts = new ArrayList<>();
      for (StorePuts puts : storePuts) {
        rowMutations.add((org.apache.hadoop.hbase.client.Mutation) puts.entityPut);
        if (puts.indexPuts != null) {
          mutation.indexPuts.addAll(puts.indexPuts);
        }
      }
    }
    if (mutateData.getDeleteDataList() != null && !mutateData.getDeleteDataList().isEmpty()) {
      for (DeleteData deleteData : mutateData.getDeleteDataList()) {
        org.apache.hadoop.hbase.client.Mutation delete = buildDelete(deleteData, keyDistributorPerTable);
        if (rowMutations == null) {
          rowMutations = new RowMutations(delete.getRow());
        }
        rowMutations.add(delete);
      }
    }
    mutation.rowMutations = rowMutations;
    return mutation;
  }

  static Append buildStoreAppend(StoreData data, Map<String, KeyDistributor> keyDistributorPerTable,
      Optional<Durability> durability) {
    // setup key Distributors
    KeyDistributor keyDistributor = keyDistributorPerTable.getOrDefault(data.getTableName(), NoDistribution.INSTANCE);

    byte[] rowKey = keyDistributor.enrichKey(data.getRow(), data.getPartitionKey());
    Append append = new Append(rowKey);
    durability.ifPresent(append::setDurability);
    data.getDurabilityOverride().ifPresent(append::setDurability);
    data.getTTL().ifPresent(append::setTTL);

    for (Map.Entry<String, ColumnsMap> cfsEntry : data.getCfs().entrySet()) {
      Map<String, Cell> qualMap = cfsEntry.getValue();
      for (Map.Entry<String, Cell> qualEntry : qualMap.entrySet()) {
        Cell column = qualEntry.getValue();
        append.addColumn(cfsEntry.getKey().getBytes(), qualEntry.getKey().getBytes(), column.getValue());
      }
    }
    return append;
  }

  static Delete buildDeleteQuery(DeleteData del, byte[] key) {
    Delete deleteRow = new Delete(key);
    if (del.getCfs() != null) {
      for (Map.Entry<String, Set<String>> cfsEntry : del.getCfs().entrySet()) {
        byte[] cfBytes = Bytes.toBytes(cfsEntry.getKey());
        Set<String> cols = cfsEntry.getValue();
        if (cols.isEmpty()) {
          deleteRow.addFamily(cfBytes);
        } else {
          for (String col : cols) {
            if(del.getType().equals(KeyValue.Type.DeleteColumn)) {
              deleteRow.addColumns(cfBytes, Bytes.toBytes(col));
            } else {
              deleteRow.addColumn(cfBytes, Bytes.toBytes(col));
            }
          }
        }
      }
    }
    return deleteRow;
  }

  static byte[] getDistributedKey(Map<String, KeyDistributor> map, String tableName, byte[] rowKey, byte[] partitionKey) {
    return map.getOrDefault(tableName, NoDistribution.INSTANCE).enrichKey(rowKey, partitionKey);
  }

  static ResultMap buildResultMapFromResult(Result result) {
    ResultMap output = new ResultMap();
    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap = result.getMap();

    if (Objects.nonNull(familyMap)) {
      for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyEntry : familyMap.entrySet()) {
        if (Objects.isNull(familyEntry.getValue())) {
          continue;
        }
        String columnFamily = Bytes.toString(familyEntry.getKey());
        ColumnsMap colMap = new ColumnsMap();

        for (Map.Entry<byte[], NavigableMap<Long, byte[]>> qualifierEntry : familyEntry.getValue().entrySet()) {
          String column = Bytes.toString(qualifierEntry.getKey());
          TreeMap<Long, byte[]> versionedCells = new TreeMap<>(qualifierEntry.getValue().comparator());
          versionedCells.putAll(qualifierEntry.getValue());
          colMap.put(column, new Cell(versionedCells));
        }
        output.put(columnFamily, colMap);
      }
    }
    return output;
  }

  static List<Get> buildGets(List<? extends GetRow> rows, Map<String, KeyDistributor> distributorMap) {
    GetRow firstRow = rows.get(0);
    String tableName = firstRow.getTableName();
    if (firstRow instanceof GetCells) {
      return rows.stream().map(row -> {
        byte[] key = getDistributedKey(distributorMap, tableName, row.getKey(), row.getPartitionKey());
        Get hbaseGet = new Get(key);
        GetCells get = (GetCells) row;
        for (ColTupple tupple : get.getFetchSet()) {
          hbaseGet.addColumn(tupple.getCol(), tupple.getQualifier());
        }
        try {
          hbaseGet.readVersions(get.getMaxVersions());
        } catch (IOException e) {
          if(logger.isDebugEnabled()){
            logger.debug("Reading all versions since Negative number {} passed to readVersions method.", get.getMaxVersions());
          }
          hbaseGet.readAllVersions();
        }
        return hbaseGet;
      }).collect(Collectors.toList());
    } else if (firstRow instanceof GetColumnMap) {
      return rows.stream().map(row -> {
        byte[] key = getDistributedKey(distributorMap, tableName, row.getKey(), row.getPartitionKey());
        Get hbaseGet = new Get(key);
        GetColumnMap get = (GetColumnMap) row;
        get.getFilter().ifPresent(hbaseGet::setFilter);
        hbaseGet.addFamily(get.getCf());
        try {
          hbaseGet.readVersions(get.getMaxVersions());
        } catch (IOException e) {
          if(logger.isDebugEnabled()){
            logger.debug("Reading all versions since Negative number {} passed to readVersions method.", get.getMaxVersions());
          }

          hbaseGet.readAllVersions();
        }
        return hbaseGet;
      }).collect(Collectors.toList());
    } else if (firstRow instanceof GetCell) {
      return rows.stream().map(row -> {
        byte[] key = getDistributedKey(distributorMap, tableName, row.getKey(), row.getPartitionKey());
        Get hbaseGet = new Get(key);
        GetCell cell = (GetCell) row;
        cell.getFilter().ifPresent(hbaseGet::setFilter);
        hbaseGet.addColumn(cell.getCf(), cell.getQualifier());
        try {
          hbaseGet.readVersions(cell.getMaxVersions());
        } catch (IOException e) {
          if(logger.isDebugEnabled()){
            logger.error("Reading all versions since Negative number {} passed to readVersions method.", cell.getMaxVersions());
          }

          hbaseGet.readAllVersions();
        }
        return hbaseGet;
      }).collect(Collectors.toList());
    } else {
      return rows.stream().map(row -> {
        byte[] key = getDistributedKey(distributorMap, tableName, row.getKey(), row.getPartitionKey());
        Get hbaseGet = new Get(key);
        GetRow get = row;
        get.getFilter().ifPresent(hbaseGet::setFilter);
        try {
          hbaseGet.readVersions(get.getMaxVersions());
        } catch (IOException e) {
          if(logger.isDebugEnabled()){
            logger.error("Reading all versions since Negative number {} passed to readVersions method.", get.getMaxVersions());
          }
          hbaseGet.readAllVersions();
        }
        return hbaseGet;
      }).collect(Collectors.toList());
    }
  }

  public static Scan buildScan(ScanData scanData, Map<String, KeyDistributor> keyDistributorPerTable) throws StoreException {
    KeyDistributor keyDistributor = keyDistributorPerTable.getOrDefault(scanData.getTableName(), NoDistribution.INSTANCE);
    Scan scan = new Scan();
    scan.setFilter(scanData.getFilter());
    if(scanData.getBatch() != -1) {
      scan.setBatch(scanData.getBatch());
    }
    scan.setReversed(scanData.isReversed());
    scan.setCacheBlocks(scanData.isCacheBlocks());
    scan.setMaxResultSize(scanData.getMaxResultSize());
    scan.setMaxVersions(scanData.getMaxVersions());
    scan.setLimit(scanData.getLimit());
    scan.setAllowPartialResults(scanData.isAllowPartialResults());

    if (scanData.getStartRow() == null || Bytes.equals(scanData.getStartRow(), HConstants.EMPTY_START_ROW)
        || scanData.getStopRow() == null || Bytes.equals(scanData.getStopRow(), HConstants.EMPTY_END_ROW)) {

      throw new StoreException("startRow and stopRow can not be null/empty");
    }

    byte[] startRowKey = keyDistributor.enrichKey(scanData.getStartRow(), scanData.getPartitionKey());
    byte[] stopRowKey = keyDistributor.enrichKey(scanData.getStopRow(), scanData.getPartitionKey());
    scan.setStartRow(startRowKey);
    scan.setStopRow(stopRowKey);

    if (scanData.getCaching() == -1 || (scanData.getLimit() != -1 && scanData.getCaching() > scanData.getLimit())) {
      scan.setCaching(scanData.getLimit());
    } else {
      scan.setCaching(scanData.getCaching());
    }

    if (scanData.getColumnfamilies() != null && !scanData.getColumnfamilies().isEmpty()) {
      for (ColTupple colTupple : scanData.getColumnfamilies()) {
        scan.addColumn(colTupple.getCol(), colTupple.getQualifier());
      }
    }

    try {
      scan.setTimeRange(scanData.getMinStamp(), scanData.getMaxStamp());
    } catch (IOException e) {
      throw new StoreException(e);
    }
    return scan;
  }

  public static Increment buildIncrement(IncrementData incrementData, Map<String, KeyDistributor> keyDistributorPerTable) {
    KeyDistributor keyDistributor = keyDistributorPerTable.getOrDefault(incrementData.getTableName(), NoDistribution.INSTANCE);
    byte[] distributedKey = keyDistributor.enrichKey(incrementData.getRowkey(), incrementData.getPartitionKey());

    Increment increment = new Increment(distributedKey);
    for(IncrementData.ColumnData columnData: incrementData.getColumnDataList()){
      increment.addColumn(columnData.getFamily(), columnData.getQualifier(), columnData.getAmount());
    }
    return increment;
  }

  static class RecurseData<T> {
    byte[] indexKey;
    List<Tupple> rowKeys;
    List<T> output;
    int pageSize;
    String tableName;
    byte[] cf;
    byte[] qualifier;
    KeyDistributor keyDistributor;
    KeyDistributor indexKeyDistributor;

    RecurseData(IndexResponse indexResp, GetCellByIndex get, KeyDistributor keyDistributor,
        KeyDistributor indexKeyDistributor) {
      this.rowKeys = indexResp.rowKeys;
      this.output = new ArrayList<>();
      this.pageSize = indexResp.stop - indexResp.start;
      this.tableName = get.getTableName();
      this.cf = get.getCf();
      this.qualifier = get.getQualifier();
      this.indexKey = get.getKey();
      this.keyDistributor = keyDistributor;
      this.indexKeyDistributor = indexKeyDistributor;
    }

    RecurseData(IndexResponse indexResp, GetColumnsMapByIndex get, KeyDistributor keyDistributor,
        KeyDistributor indexKeyDistributor) {
      this.rowKeys = indexResp.rowKeys;
      this.output = new ArrayList<>();
      this.pageSize = indexResp.stop - indexResp.start;
      this.tableName = get.getTableName();
      this.cf = get.getCf();
      this.qualifier = null;
      this.indexKey = get.getKey();
      this.keyDistributor = keyDistributor;
      this.indexKeyDistributor = indexKeyDistributor;
    }

    IndexDeleteData buildDeleteData(Get get) {
      return new IndexDeleteData(this.tableName, this.cf, this.qualifier, this.indexKey, get.getRow());

    }

    void updateGet(Get entityGet) {
      if (this.qualifier != null) {
        entityGet.addColumn(this.cf, this.qualifier);
        entityGet.addColumn(this.cf, this.indexKey);
      } else {
        entityGet.addFamily(this.cf);
      }
    }

    void updateOutput(Result result) {
      if (this.qualifier != null) {
        byte[] val = result.getValue(cf, qualifier);
        Cell cell = new Cell(val);
        output.add((T) cell);
      } else {
        ColumnsMap mapOutput = new ColumnsMap();
        NavigableMap<byte[], byte[]> colMap = result.getFamilyMap(this.cf);
        for (Map.Entry<byte[], byte[]> colEntry : colMap.entrySet()) {
          byte[] val = colEntry.getValue();
          mapOutput.put(Bytes.toString(colEntry.getKey()), new Cell(val));
        }
        output.add((T) mapOutput);
      }
    }
  }

  static class IndexResponse {
    IndexResponse(List<Tupple> rowKeys, int start, int stop) {
      this.rowKeys = rowKeys;
      this.start = start;
      this.stop = stop;
    }

    List<Tupple> rowKeys;
    int start;
    int stop;
  }

  static class StorePuts {
    Put entityPut;
    List<Put> indexPuts;
  }

  static class BatchActions {
    List<Row> actions;
    List<Put> indexPuts;
  }

  static class Mutation {
    RowMutations rowMutations;
    List<Put> indexPuts;
  }

  static class Tupple implements Comparator<Tupple> {
    static final Comparator<? super Tupple> COMPARATOR = new Tupple();
    byte[] key;
    long timestamp;

    private Tupple() {
    }

    Tupple(byte[] key, long timestamp) {
      this.key = key;
      this.timestamp = timestamp;
    }

    @Override public int compare(Tupple o1, Tupple o2) {
      return Long.compare(o1.timestamp, o2.timestamp);
    }
  }
}
