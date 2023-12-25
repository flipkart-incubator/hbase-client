package com.flipkart.yak.client.pipelined.models;

import com.netflix.hystrix.HystrixObservableCommand;

public class HystrixSettings implements CircuitBreakerSettings<HystrixObservableCommand.Setter> {
  private HystrixObservableCommand.Setter settings;

  public HystrixSettings(HystrixObservableCommand.Setter settings) {
    this.settings = settings;
  }

  public HystrixObservableCommand.Setter getSettings() {
    return settings;
  }
}
