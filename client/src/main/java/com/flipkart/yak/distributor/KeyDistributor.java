package com.flipkart.yak.distributor;

import java.util.Optional;

/**
 * Distribute key as per partitionHint.
 *
 * @author gokulvanan.v
 */
@SuppressWarnings({"java:S1214", "java:S115"})
public interface KeyDistributor {

  byte delim = '-';

  default byte[] enrichKey(byte[] key) {
    return enrichKey(key, null);
  }

  /**
   *  Computes hash bucket for a partitionKey if not null, else uses rowKey for hash computation
   * @param rowKey hbase row key of a given row
   * @param partitionKey partition hint on which hash will be computed
   * @return
   */
  default byte[] enrichKey(byte[] rowKey, byte[] partitionKey) {
    byte[] prefix = partitionHint(partitionKey != null ? partitionKey : rowKey);
    if (prefix == null) return rowKey;
    else {
      byte[] data = new byte[prefix.length + rowKey.length + 1];
      System.arraycopy(prefix, 0, data, 0, prefix.length);
      data[prefix.length] = delim;
      System.arraycopy(rowKey, 0, data, prefix.length + 1, rowKey.length);
      return data;
    }
  }

  byte[] partitionHint(byte[] key);
}
