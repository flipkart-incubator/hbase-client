package com.flipkart.yak.client.validator;

import com.flipkart.yak.client.exceptions.RequestValidatorException;
import com.flipkart.yak.models.CheckAndStoreData;
import com.flipkart.yak.models.DeleteData;
import com.flipkart.yak.models.GetRow;
import com.flipkart.yak.models.IdentifierData;
import java.util.List;

public interface RequestValidators {

  <T extends IdentifierData> void validateTableName(String tableName, List<T> operations)
      throws RequestValidatorException;

  <T extends GetRow> void validateBatchGetSize(List<T> gets) throws RequestValidatorException;

  <T extends DeleteData> void validateBatchDeleteSize(List<T> deletes) throws RequestValidatorException;

  /**
   * Validates that the batch check-and-put list does not exceed the configured maximum size.
   *
   * @param dataList the list of {@link CheckAndStoreData} to validate
   * @throws RequestValidatorException if the list size exceeds the configured limit
   */
  void validateBatchCheckAndPutSize(List<CheckAndStoreData> dataList) throws RequestValidatorException;

}
