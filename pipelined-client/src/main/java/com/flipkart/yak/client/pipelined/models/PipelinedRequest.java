package com.flipkart.yak.client.pipelined.models;

import java.util.List;

/**
 * This is to pass Pipelined Request from within a specific decorator to the next in pipeline
 */
@SuppressWarnings("java:S3740")
public class PipelinedRequest {
  private final String methodName;
  private final List<Class> parameterTypes;
  private final List<Object> parameters;

  public PipelinedRequest(String methodName, List<Class> parameterTypes, List<Object> parameters) {
    this.methodName = methodName;
    this.parameterTypes = parameterTypes;
    this.parameters = parameters;
  }

  public String getMethodName() {
    return methodName;
  }

  public List<Class> getParameterTypes() {
    return parameterTypes;
  }

  public List<Object> getParameters() {
    return parameters;
  }
}
