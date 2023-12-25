package com.flipkart.yak.models;

public class DeleteCell extends DeleteColumnMap {
  protected final byte[] qualifier;

  DeleteCell(String tableName, byte[] row, byte[] cf, byte[] qualifer) {
    super(tableName, row, cf);
    this.qualifier = qualifer;
  }

  public byte[] getQualifier() {
    return qualifier;
  }

  @Override public DeleteCell copy(byte[] newRow) {
    return new DeleteCell(this.tableName, newRow, this.cf, this.qualifier);
  }
}
