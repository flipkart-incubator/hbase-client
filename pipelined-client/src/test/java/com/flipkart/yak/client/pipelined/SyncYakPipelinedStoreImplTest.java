package com.flipkart.yak.client.pipelined;

import com.flipkart.yak.models.CheckAndStoreData;
import com.flipkart.yak.models.StoreDataBuilder;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hbase.CompareOperator;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.mockito.Matchers;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

/**
 * {@link SyncYakPipelinedStoreImpl} coverage. Uses the same PowerMock + {@link BlockJUnit4ClassRunner} pattern as
 * {@link MasterSlaveYakPipelinedStoreImplTest} so single-method runs work in IntelliJ.
 */
@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(BlockJUnit4ClassRunner.class)
public class SyncYakPipelinedStoreImplTest extends PipelinedClientBaseTest {

  @After
  public void tearDown() throws IOException, InterruptedException {
    if (store != null) {
      store.shutdown();
    }
  }

  /**
   * Runs the real master/slave {@code checkAndPut} pipeline. The primary site’s client returns a
   * {@link CompletableFuture} that never completes, so the composed async chain never finishes and the user
   * callback is never invoked. {@link SyncYakPipelinedStoreImpl} should then block until the write timeout and throw
   * {@link TimeoutException}. Downstream sites must not be hit.
   */
  @Test
  public void checkAndPutTimesOutWhenFirstSiteFutureNeverCompletes() throws Exception {
    int writeTimeoutMs = 30;
    CheckAndStoreData data =
        new StoreDataBuilder(FULL_TABLE_NAME).withRowKey(rowKey1.getBytes()).addColumn(cf1, cq1, "2".getBytes())
            .buildWithCheckAndVerifyColumn(cf1, cq1, qv1.getBytes(), CompareOperator.EQUAL.name());

    CompletableFuture<Boolean> hanging = new CompletableFuture<>();
    when(region1SiteAClient.checkAndPut(eq(data))).thenReturn(hanging);

    SyncYakPipelinedStoreImpl syncWithShortTimeout =
        new SyncYakPipelinedStoreImpl(store).setWriteTimeoutInMillis(writeTimeoutMs);

    long startNanos = System.nanoTime();
    try {
      syncWithShortTimeout.checkAndPut(data, routeMetaChOptional, Optional.empty(), Optional.empty());
      fail("Expected TimeoutException when first-site async never completes");
    } catch (TimeoutException expected) {
      long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
      assertTrue(
          "Should wait at least writeTimeoutMs (elapsedMillis=" + elapsedMillis + ")",
          elapsedMillis >= writeTimeoutMs);
    }

    verify(region1SiteAClient, times(1)).checkAndPut(eq(data));
    verify(region1SiteBClient, never()).checkAndPut(Matchers.any(CheckAndStoreData.class));
    verify(region2SiteAClient, never()).checkAndPut(Matchers.any(CheckAndStoreData.class));
  }
}
