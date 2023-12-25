package com.flipkart.yak.client.validator;

import com.flipkart.yak.client.exceptions.RequestValidatorException;
import com.flipkart.yak.models.DeleteData;
import com.flipkart.yak.models.GetRow;
import com.flipkart.yak.models.IdentifierData;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestValidatorsImpl implements RequestValidators {

  private static final Logger logger = LoggerFactory.getLogger(RequestValidatorsImpl.class);
  private final int maxBatchGetSize;
  private final int maxBatchDeleteSize;

  public RequestValidatorsImpl(int maxBatchGetSize, int maxBatchDeleteSize) {
    this.maxBatchGetSize = maxBatchGetSize;
    this.maxBatchDeleteSize = maxBatchDeleteSize;
  }

  public <T extends IdentifierData> void validateTableName(String tableName, List<T> operations)
      throws RequestValidatorException {
    if (operations.stream().filter(ops -> ops.getTableName().equals(tableName)).count() != operations.size()) {
      throw new RequestValidatorException("Batch request should only come for one table");
    }
  }

  public <T extends GetRow> void validateBatchGetSize(List<T> gets) throws RequestValidatorException {
    if (gets.size() > maxBatchGetSize) {
      logger.error("Request size is greater than the configured batch size {} ", maxBatchGetSize);
      throw new RequestValidatorException(
          "Request size should not be greater than the configured batch size " + maxBatchGetSize);
    }
  }

  @Override public <T extends DeleteData> void validateBatchDeleteSize(List<T> deletes)
      throws RequestValidatorException {
    if (deletes.size() > maxBatchDeleteSize) {
      logger.error("Request size is greater than the configured batch size {} ", maxBatchDeleteSize);
      throw new RequestValidatorException(
          "Request size should not be greater than the configured batch size " + maxBatchDeleteSize);
    }
  }

}
