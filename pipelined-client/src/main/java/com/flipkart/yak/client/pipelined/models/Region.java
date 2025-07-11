package com.flipkart.yak.client.pipelined.models;

public enum Region implements DataCenter {
    REGION_1("REGION_1", "REGION_1"),
    REGION_2("REGION_2", "REGION_2"),
    REGION_3("REGION_3", "REGION_3");

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
