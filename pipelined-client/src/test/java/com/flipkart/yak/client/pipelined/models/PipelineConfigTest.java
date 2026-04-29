package com.flipkart.yak.client.pipelined.models;

import com.flipkart.yak.client.pipelined.config.MultiRegionStoreConfig;
import java.util.Optional;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link PipelineConfig}.
 */
public class PipelineConfigTest {

  private static MultiRegionStoreConfig minimalMultiRegion() {
    return new MultiRegionStoreConfig();
  }

  @Test
  public void executorQueueCapacityDefaultsToZero() {
    PipelineConfig config =
        new PipelineConfig(minimalMultiRegion(), 30, 10, "test-store", Optional.empty(), 3, 3000L);
    assertEquals(0, config.getExecutorQueueCapacity());
  }

  @Test
  public void storesConfiguredExecutorQueueCapacity() {
    PipelineConfig config = new PipelineConfig(minimalMultiRegion(), 30, 10, "test-store", Optional.empty(), 3, 3000L, 64);
    assertEquals(64, config.getExecutorQueueCapacity());
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsZeroExecutorQueueCapacity() {
    new PipelineConfig(minimalMultiRegion(), 30, 10, "test-store", Optional.empty(), 3, 3000L, 0);
  }

  @Test
  public void rejectsNegativeExecutorQueueCapacity() {
    try {
      new PipelineConfig(minimalMultiRegion(), 30, 10, "test-store", Optional.empty(), 3, 3000L, -1);
    } catch (IllegalArgumentException ex) {
      assertTrue(ex.getMessage().contains("executorQueueCapacity"));
      return;
    }
    org.junit.Assert.fail("expected IllegalArgumentException");
  }
}
