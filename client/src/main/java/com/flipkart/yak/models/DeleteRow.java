package com.flipkart.yak.models;

public class DeleteRow extends IdentifierData {

  protected final byte[] key;

  DeleteRow(String tableName, byte[] row) {
    super(tableName);
    this.key = row;
  }

  public byte[] getKey() {
    return key;
  }

  public DeleteRow copy(byte[] newRow) {
    return new DeleteRow(this.tableName, newRow);
  }
}
