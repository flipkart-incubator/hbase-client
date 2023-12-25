package com.flipkart.yak.models;

import java.util.Set;

public class DeleteCells extends DeleteRow {

  protected final Set<ColTupple> delSet;

  DeleteCells(String tableName, byte[] row, Set<ColTupple> delSet) {
    super(tableName, row);
    this.delSet = delSet;
  }

  public Set<ColTupple> getDelSet() {
    return this.delSet;
  }

}
