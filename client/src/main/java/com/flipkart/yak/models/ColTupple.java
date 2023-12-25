package com.flipkart.yak.models;

import java.util.Arrays;

public class ColTupple {

  final byte[] col;
  final byte[] qualifier;

  public ColTupple(byte[] col, byte[] qualifier) {
    this.col = col;
    this.qualifier = qualifier;
  }

  public byte[] getCol() {
    return col;
  }

  public byte[] getQualifier() {
    return qualifier;
  }

  @Override public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(col);
    result = prime * result + Arrays.hashCode(qualifier);
    return result;
  }

  @SuppressWarnings("java:S1126")
  @Override public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    ColTupple other = (ColTupple) obj;
    if (!Arrays.equals(col, other.col)) return false;
    if (!Arrays.equals(qualifier, other.qualifier)) return false;
    return true;
  }

}
