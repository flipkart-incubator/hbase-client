package com.flipkart.yak.models;

/**
 * Identifier Data for all Get Request models All Get calls are expected to pass on tableName By convention for tables
 * where columns are to be indexed the indexTable name would tableName_index
 *
 * @author gokulvanan.v
 */
public class IdentifierData {

  protected final String tableName;

  public IdentifierData(String tableName) {
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getIndexTableName() {
    return tableName + "_index";
  }

}
