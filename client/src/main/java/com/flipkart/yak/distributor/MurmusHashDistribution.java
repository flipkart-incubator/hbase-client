package com.flipkart.yak.distributor;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.math.IntMath;
import org.apache.hadoop.hbase.util.Bytes;

public class MurmusHashDistribution implements KeyDistributor {

  private final int bucketSize;
  private final HashFunction hashFunction;
  private final String format;

  public MurmusHashDistribution(int size) {
    this.bucketSize = size;
    this.hashFunction = Hashing.murmur3_128();
    format = "%0" + String.valueOf(size).length() + "d";
  }

  @Override public byte[] partitionHint(byte[] key) {
    return Bytes.toBytes(String.format(format, IntMath.mod(this.hashFunction.hashBytes(key).asInt(), bucketSize)));
  }

  public String getFormat() {
    return format;
  }
}
