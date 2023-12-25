package com.flipkart.yak.client.validator;

import com.flipkart.yak.models.StoreData;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.TableName;

public interface AsyncPayloadValidator {

  CompletableFuture<Void> validate(List<StoreData> data);

  CompletableFuture<Void> refresh(TableName table);
}
