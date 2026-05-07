# Batch CheckAndPut (CAS) — Code Review Concerns

PR: https://github.com/flipkart-incubator/hbase-client/pull/18

---

## Critical — Must Fix Before Merging

### 1. `completeExceptionally(null)` on success
**File:** `AsyncStoreClientImpl.java` (~line 482)

The `whenCompleteAsync` handler calls `future.completeExceptionally(error)` and increments the error metric unconditionally, without checking if `error != null`. On a successful completion, `error == null`, so **every successful batch CAS call completes exceptionally with a null cause** — making the response completely unusable by callers.

**Fix:** Wrap the block in `if (error != null) { ... }`. The same issue reportedly exists in the `put(List)` handler.

```java
// Current (broken)
responseFutures.stream().forEach(future -> future.completeExceptionally(error));
publisher.incrementErrorMetric(BATCH_CAS_GEN_EXCEPTION, error);

// Fixed
if (error != null) {
    responseFutures.stream().forEach(future -> future.completeExceptionally(error));
    publisher.incrementErrorMetric(BATCH_CAS_GEN_EXCEPTION, error);
}
```

---

## High — Data Integrity Risk

### 2. Index writes happen before CAS
**File:** `AsyncStoreClientImpl.java` (~line 448)

Current flow: `validate → updateIndexesIfPresent → checkAndMutate`

If the CAS check fails (version mismatch), index entries are **already written** and become orphaned — no corresponding entity row exists for them.

**Correct flow:** `validate → checkAndMutate → if CAS succeeded, updateIndexesIfPresent`

On CAS failure, index writes must be skipped or compensated. This is relevant whenever entities have Yak secondary indexes and is a real data integrity risk in those cases.

---

## Functional Gaps

### 3. No batch size validation
**File:** `RequestValidators` / `AsyncStoreClientImpl`

`validateBatchGetSize` and `validateBatchDeleteSize` exist for other batch operations. Batch checkAndPut has no equivalent upper-bound check. A large unvalidated list can overload HBase region servers.

**Fix:** Add `validateBatchCheckAndPutSize` to `RequestValidators` and call it in the batch `checkAndPut` implementation, similar to how batch get and delete do it.

---

### 4. `IntentStoreDecorator` does not handle batch checkAndPut
**File:** `IntentStoreDecorator.java` (~line 73)

Single `checkAndPut` goes through intent write handling in `YakIntentStoreDecorator`. The batch variant passes through as a no-op, bypassing intent write logic entirely. If callers use intent writes for CAS operations, batch CAS will not be captured in the intent store — a silent functional gap.

**Fix:** Add an override that mirrors the single-item intent handling, using `handleIntentWrite` with `CHECK_PUT_METHOD_NAME` and the appropriate parameter types/values.

---

### 5. `BATCH_CHECK_PUT_METHOD_NAME` constant naming — and CodeRabbit's suggested fix is incorrect
**File:** `YakPipelinedStore.java` (line 54), `AsyncStoreClient.java`

`BATCH_CHECK_PUT_METHOD_NAME = "checkAndPut"` has the same string value as `CHECK_PUT_METHOD_NAME`. This is confusing.

**CodeRabbit suggested** changing the value to `"batchCheckAndPut"` — **this is incorrect as stated**. The constant value is used via reflection: `getMethod("checkAndPut", List.class)`. If the value is changed to `"batchCheckAndPut"` without also renaming the method in `AsyncStoreClient`, the reflection lookup will throw `NoSuchMethodException` at runtime.

**Correct fix:** Rename **both** the constant value AND the `AsyncStoreClient` method to `batchCheckAndPut` together. Or leave as-is with a comment explaining the reflection mechanism.

---

## Testing & Documentation

### 6. No unit tests for new batch CAS APIs
Existing batch operations have dedicated test files (`PipelinedClientBatchGetTest`, `PipelinedClientBatchDeleteTest`, `PipelinedClientBatchPutTest`, etc.). Batch checkAndPut has no equivalent.

**Key test cases needed:**
- All items return `true` (all CAS succeed)
- Some items return `false` (version mismatch for some)
- All items return `false`
- One item has an infra error — verify `UnexpectedYakDAOException` is thrown
- Empty input list
- Input exceeds max batch size — verify validation error
- Correct positional mapping (response[i] corresponds to input[i])

---

### 7. Docstring coverage below 80% threshold (currently 50%)
CI check fails. New methods added across `AsyncStoreClient`, `SyncYakPipelinedStore`, and `YakPipelinedStore` interfaces need JavaDoc consistent with existing methods.

---

## Nitpick

### 8. Null check ordering in `AsyncStoreClientUtis.buildCheckAndMutateForPut` (~line 178)
Builds the `ifEquals` builder first, then overwrites with `ifNotExists` if `vdata.getData() == null`. The null check should come first to avoid constructing an unnecessary builder.

```java
// Current
CheckAndMutate.Builder builder = CheckAndMutate.newBuilder(rowKey)
    .ifEquals(vdata.getCf().getBytes(), vdata.getQualifier().getBytes(), vdata.getData());
if (vdata.getData() == null) {
    builder = CheckAndMutate.newBuilder(rowKey)
        .ifNotExists(vdata.getCf().getBytes(), vdata.getQualifier().getBytes());
}

// Suggested
CheckAndMutate.Builder builder;
if (vdata.getData() == null) {
    builder = CheckAndMutate.newBuilder(rowKey)
        .ifNotExists(vdata.getCf().getBytes(), vdata.getQualifier().getBytes());
} else {
    builder = CheckAndMutate.newBuilder(rowKey)
        .ifEquals(vdata.getCf().getBytes(), vdata.getQualifier().getBytes(), vdata.getData());
}
```

---

## Summary

| # | Concern | Severity | File |
|---|---|---|---|
| 1 | `completeExceptionally(null)` on success | **Critical** | `AsyncStoreClientImpl` |
| 2 | Index writes before CAS — orphaned index risk | **High** | `AsyncStoreClientImpl` |
| 3 | No batch size validation | Medium | `RequestValidators` |
| 4 | `IntentStoreDecorator` bypass for batch CAS | Medium | `IntentStoreDecorator` |
| 5 | Method name constant — misleading + CodeRabbit fix wrong | Medium | `YakPipelinedStore`, `AsyncStoreClient` |
| 6 | No unit tests | Medium | New test file needed |
| 7 | Docstring coverage below threshold (50% vs 80%) | Low | All new interfaces |
| 8 | Null check ordering in builder | Nitpick | `AsyncStoreClientUtis` |
