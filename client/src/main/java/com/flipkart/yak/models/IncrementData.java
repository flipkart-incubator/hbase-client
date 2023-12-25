package com.flipkart.yak.models;

import java.util.List;

public class IncrementData extends IdentifierData {

    private final List<ColumnData> columnDataList;
    private final byte[] rowkey;
    private final byte[] partitionKey;


     IncrementData(String tableName, byte[] rowkey, byte[] partitionKey, List<ColumnData> columnDataList) {
        super(tableName);
        this.rowkey = rowkey;
        this.partitionKey = partitionKey;
        this.columnDataList = columnDataList;
    }

    public byte[] getRowkey() {
        return rowkey;
    }

    public byte[] getPartitionKey() {
        return partitionKey;
    }

    public List<ColumnData> getColumnDataList() {
        return columnDataList;
    }

    public static class ColumnData {
        private final byte[] family;
        private final byte[] qualifier;
        private final long amount;

         ColumnData(byte[] family, byte[] qualifier, long amount) {
            this.family = family;
            this.qualifier = qualifier;
            this.amount = amount;
        }

        public byte[] getFamily() {
            return family;
        }

        public byte[] getQualifier() {
            return qualifier;
        }

        public long getAmount() {
            return amount;
        }
    }
}
