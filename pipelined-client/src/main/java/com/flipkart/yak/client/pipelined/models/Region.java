package com.flipkart.yak.client.pipelined.models;

public enum Region implements DataCenter {
    REGION_1("region-1", "region-1"),
    REGION_2("region-2", "region-2"),
    REGION_3("region-3", "region-3");

    private final String name;
    private final String description;

    Region(String name, String description) {
        this.name = name;
        this.description = description;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
