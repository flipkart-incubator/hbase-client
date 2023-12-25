package com.flipkart.yak.client.pipelined.models;

public enum Region {
  REGION_1("region--1"),
  REGION_2("region-2");

  private final String name;

  Region(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}

