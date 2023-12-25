package com.flipkart.yak.client.mocks;

import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.io.TimeRange;

public class MockCheckAndMutateBuilderImpl implements AsyncTable.CheckAndMutateBuilder {

  private Boolean value;
  private Throwable error;

  public MockCheckAndMutateBuilderImpl(Boolean value, Throwable error) {
    this.value = value;
    this.error = error;
  }

  @Override public AsyncTable.CheckAndMutateBuilder qualifier(byte[] bytes) {
    return this;
  }

  @Override public AsyncTable.CheckAndMutateBuilder timeRange(TimeRange timeRange) {
    return this;
  }

  @Override public AsyncTable.CheckAndMutateBuilder ifNotExists() {
    return this;
  }

  @Override public AsyncTable.CheckAndMutateBuilder ifMatches(CompareOperator compareOperator, byte[] bytes) {
    return this;
  }

  @Override public CompletableFuture<Boolean> thenPut(Put put) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    if (error != null) {
      future.completeExceptionally(error);
    } else {
      future.complete(value);
    }
    return future;
  }

  @Override public CompletableFuture<Boolean> thenDelete(Delete delete) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    if (error != null) {
      future.completeExceptionally(error);
    } else {
      future.complete(value);
    }
    return future;
  }

  @Override public CompletableFuture<Boolean> thenMutate(RowMutations rowMutations) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    if (error != null) {
      future.completeExceptionally(error);
    } else {
      future.complete(value);
    }
    return future;
  }
}
