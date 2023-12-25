package com.flipkart.yak.models;

import java.util.TreeMap;

public class Cell {

  private final byte[] value;
  private final TreeMap<Long, byte[]> versionedValues;

  public Cell(byte[] val) {
    this.value = val;
    versionedValues = null;
  }

  public Cell(TreeMap<Long, byte[]> versionedValues) {
    this.value = versionedValues.firstEntry().getValue();
    this.versionedValues = versionedValues;
  }

  public byte[] getValue() {
    return value;
  }

  /**
   * @return The {@link TreeMap} of cell Timestamp to Value.
   */
  public TreeMap<Long, byte[]> getVersionedValues(){
   return this.versionedValues;
  }

}
