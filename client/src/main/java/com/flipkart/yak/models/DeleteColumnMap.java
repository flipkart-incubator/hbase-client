package com.flipkart.yak.models;

public class DeleteColumnMap extends DeleteRow {

  protected final byte[] cf;

  DeleteColumnMap(String tableName, byte[] row, byte[] cf) {
    super(tableName, row);
    this.cf = cf;
  }

  public byte[] getCf() {
    return cf;
  }

  @Override public DeleteColumnMap copy(byte[] newRow) {
    return new DeleteColumnMap(this.tableName, newRow, this.cf);
  }
}
