package com.flipkart.yak.client.pipelined.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.flipkart.yak.client.metrics.MetricsPublisherImpl;
import com.flipkart.yak.client.metrics.StoreClientMetricsPublisher;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("common-java:DuplicatedBlocks")
public class PipelinedClientMetricsPublisher {
  private static final String METRIC_METHOD_PUT_NAME = "put.";
  private static final String METRIC_METHOD_BATCH_PUT_NAME = "batchPut.";
  private static final String METRIC_METHOD_CAS_NAME = "cas.";
  private static final String METRIC_METHOD_APPEND_NAME = "append.";
  private static final String METRIC_METHOD_BATCH_DELETE_NAME = "batchDelete.";
  private static final String METRIC_METHOD_COMPARE_DELETE_NAME = "checkDelete.";
  private static final String METRIC_METHOD_READ_NAME = "get.";
  private static final String METRIC_METHOD_SCAN_NAME = "scan.";
  private static final String METRIC_METHOD_BATCH_READ_NAME = "batchGet.";
  private static final String METRIC_METHOD_INDEX_READ_NAME = "indexGet.";
  private static final String METRIC_METHOD_INCREMENT_NAME = "increment.";
  private static final String METRIC_REQUESTS_TYPE = "requests.";
  private static final String METRIC_FALLBACK = "fallback";
  private static final String METRIC_INIT = "init";
  private static final String METRIC_COMPLETE = "complete";
  private static final String METRIC_NO_SITES = "noSites";
  private static final String METRIC_TIMER = "duration";

  public static final String ACTIVE_THREAD_COUNT = "activeThreadCount";
  public static final String QUEUE_SIZE = "pendingQueueSize";
  public static final String THREAD_COUNT = "totalThreadCount";

  public static final String APPEND_FALLBACK = "appendFallback";
  public static final String PUT_FALLBACK = "putFallback";
  public static final String BATCH_PUT_FALLBACK = "batchPutFallback";
  public static final String CAS_FALLBACK = "casFallback";
  public static final String GET_FALLBACK = "getFallback";
  public static final String SCAN_FALLBACK = "scanFallback";
  public static final String BATCH_GET_FALLBACK = "batchGetFallback";
  public static final String BATCH_DELETE_FALLBACK = "batchDeleteFallback";
  public static final String CHECK_DELETE_FALLBACK = "checkDeleteFallback";
  public static final String INDEX_GET_FALLBACK = "indexGetFallback";
  public static final String INCREMENT_FALLBACK = "incrementFallback";

  public static final String APPEND_NO_SITES = "appendNoSites";
  public static final String PUT_NO_SITES = "putNoSites";
  public static final String BATCH_PUT_NO_SITES = "batchPutNoSites";
  public static final String CAS_NO_SITES = "casNoSites";
  public static final String GET_NO_SITES = "getNoSites";
  public static final String SCAN_NO_SITES = "scanNoSites";
  public static final String BATCH_GET_NO_SITES = "batchGetNoSites";
  public static final String BATCH_DELETE_NO_SITES = "batchDeleteNoSites";
  public static final String CHECK_DELETE_NO_SITES = "checkDeleteNoSites";
  public static final String INDEX_GET_NO_SITES = "indexGetNoSites";
  public static final String INCREMENT_NO_SITES = "incrementNoSites";

  public static final String APPEND_INIT = "appendInit";
  public static final String PUT_INIT = "putInit";
  public static final String BATCH_PUT_INIT = "batchPutInit";
  public static final String CAS_INIT = "casInit";
  public static final String GET_INIT = "getInit";
  public static final String SCAN_INIT = "scanInit";
  public static final String BATCH_GET_INIT = "batchGetInit";
  public static final String BATCH_DELETE_INIT = "batchDeleteInit";
  public static final String CHECK_DELETE_INIT = "checkDeleteInit";
  public static final String INDEX_GET_INIT = "indexGetInit";
  public static final String INCREMENT_INIT = "incrementInit";

  public static final String APPEND_COMPLETE = "appendComplete";
  public static final String PUT_COMPLETE = "putComplete";
  public static final String BATCH_PUT_COMPLETE = "batchPutComplete";
  public static final String CAS_COMPLETE = "casComplete";
  public static final String GET_COMPLETE = "getComplete";
  public static final String SCAN_COMPLETE = "scanComplete";
  public static final String BATCH_GET_COMPLETE = "batchGetComplete";
  public static final String BATCH_DELETE_COMPLETE = "batchDeleteComplete";
  public static final String CHECK_DELETE_COMPLETE = "checkDeleteComplete";
  public static final String INDEX_GET_COMPLETE = "indexGetComplete";
  public static final String INCREMENT_COMPLETE = "incrementComplete";

  public static final String APPEND_TIMER = "appendTimer";
  public static final String PUT_TIMER = "putTimer";
  public static final String BATCH_PUT_TIMER = "batchPutTimer";
  public static final String CAS_TIMER = "casTimer";
  public static final String GET_TIMER = "getTimer";
  public static final String SCAN_TIMER = "scanTimer";
  public static final String BATCH_GET_TIMER = "batchGetTimer";
  public static final String BATCH_DELETE_TIMER = "batchDeleteTimer";
  public static final String CHECK_DELETE_TIMER = "checkDeleteTimer";
  public static final String INDEX_GET_TIMER = "indexGetTimer";
  public static final String INCREMENT_TIMER = "incrementTimer";

  private Map<String, String> meterMapping = new HashMap<>();
  private Map<String, String> timerMapping = new HashMap<>();
  private Map<String, String> counterMapping = new HashMap<>();
  private MetricsPublisherImpl publisher;

  public PipelinedClientMetricsPublisher(MetricRegistry registry, String prefix) {
    publisher = new MetricsPublisherImpl(registry, prefix);

    counterMapping.put(ACTIVE_THREAD_COUNT, ACTIVE_THREAD_COUNT);
    counterMapping.put(QUEUE_SIZE, QUEUE_SIZE);
    counterMapping.put(THREAD_COUNT, THREAD_COUNT);

    timerMapping.put(APPEND_TIMER, METRIC_METHOD_APPEND_NAME + METRIC_TIMER);
    timerMapping.put(PUT_TIMER, METRIC_METHOD_PUT_NAME + METRIC_TIMER);
    timerMapping.put(BATCH_PUT_TIMER, METRIC_METHOD_BATCH_PUT_NAME + METRIC_TIMER);
    timerMapping.put(CAS_TIMER, METRIC_METHOD_CAS_NAME + METRIC_TIMER);
    timerMapping.put(GET_TIMER, METRIC_METHOD_READ_NAME + METRIC_TIMER);
    timerMapping.put(SCAN_TIMER, METRIC_METHOD_SCAN_NAME + METRIC_TIMER);
    timerMapping.put(BATCH_GET_TIMER, METRIC_METHOD_BATCH_READ_NAME + METRIC_TIMER);
    timerMapping.put(BATCH_DELETE_TIMER, METRIC_METHOD_BATCH_DELETE_NAME + METRIC_TIMER);
    timerMapping.put(CHECK_DELETE_TIMER, METRIC_METHOD_COMPARE_DELETE_NAME + METRIC_TIMER);
    timerMapping.put(INDEX_GET_TIMER, METRIC_METHOD_INDEX_READ_NAME + METRIC_TIMER);
    timerMapping.put(INCREMENT_TIMER, METRIC_METHOD_INCREMENT_NAME + METRIC_TIMER);

    meterMapping.put(APPEND_NO_SITES, METRIC_METHOD_APPEND_NAME + METRIC_REQUESTS_TYPE + METRIC_NO_SITES);
    meterMapping.put(PUT_NO_SITES, METRIC_METHOD_PUT_NAME + METRIC_REQUESTS_TYPE + METRIC_NO_SITES);
    meterMapping.put(BATCH_PUT_NO_SITES, METRIC_METHOD_BATCH_PUT_NAME + METRIC_REQUESTS_TYPE + METRIC_NO_SITES);
    meterMapping.put(CAS_NO_SITES, METRIC_METHOD_CAS_NAME + METRIC_REQUESTS_TYPE + METRIC_NO_SITES);
    meterMapping.put(GET_NO_SITES, METRIC_METHOD_READ_NAME + METRIC_REQUESTS_TYPE + METRIC_NO_SITES);
    meterMapping.put(SCAN_NO_SITES, METRIC_METHOD_SCAN_NAME + METRIC_REQUESTS_TYPE + METRIC_NO_SITES);
    meterMapping.put(BATCH_GET_NO_SITES, METRIC_METHOD_BATCH_READ_NAME + METRIC_REQUESTS_TYPE + METRIC_NO_SITES);
    meterMapping.put(BATCH_DELETE_NO_SITES, METRIC_METHOD_BATCH_DELETE_NAME + METRIC_REQUESTS_TYPE + METRIC_NO_SITES);
    meterMapping.put(CHECK_DELETE_NO_SITES, METRIC_METHOD_COMPARE_DELETE_NAME + METRIC_REQUESTS_TYPE + METRIC_NO_SITES);
    meterMapping.put(INDEX_GET_NO_SITES, METRIC_METHOD_INDEX_READ_NAME + METRIC_REQUESTS_TYPE + METRIC_NO_SITES);
    meterMapping.put(INCREMENT_NO_SITES, METRIC_METHOD_INCREMENT_NAME + METRIC_REQUESTS_TYPE + METRIC_NO_SITES);

    meterMapping.put(APPEND_FALLBACK, METRIC_METHOD_APPEND_NAME + METRIC_REQUESTS_TYPE + METRIC_FALLBACK);
    meterMapping.put(PUT_FALLBACK, METRIC_METHOD_PUT_NAME + METRIC_REQUESTS_TYPE + METRIC_FALLBACK);
    meterMapping.put(BATCH_PUT_FALLBACK, METRIC_METHOD_BATCH_PUT_NAME + METRIC_REQUESTS_TYPE + METRIC_FALLBACK);
    meterMapping.put(CAS_FALLBACK, METRIC_METHOD_CAS_NAME + METRIC_REQUESTS_TYPE + METRIC_FALLBACK);
    meterMapping.put(GET_FALLBACK, METRIC_METHOD_READ_NAME + METRIC_REQUESTS_TYPE + METRIC_FALLBACK);
    meterMapping.put(SCAN_FALLBACK, METRIC_METHOD_SCAN_NAME + METRIC_REQUESTS_TYPE + METRIC_FALLBACK);
    meterMapping.put(BATCH_GET_FALLBACK, METRIC_METHOD_BATCH_READ_NAME + METRIC_REQUESTS_TYPE + METRIC_FALLBACK);
    meterMapping.put(BATCH_DELETE_FALLBACK, METRIC_METHOD_BATCH_DELETE_NAME + METRIC_REQUESTS_TYPE + METRIC_FALLBACK);
    meterMapping.put(CHECK_DELETE_FALLBACK, METRIC_METHOD_COMPARE_DELETE_NAME + METRIC_REQUESTS_TYPE + METRIC_FALLBACK);
    meterMapping.put(INDEX_GET_FALLBACK, METRIC_METHOD_INDEX_READ_NAME + METRIC_REQUESTS_TYPE + METRIC_FALLBACK);
    meterMapping.put(INCREMENT_FALLBACK, METRIC_METHOD_INCREMENT_NAME + METRIC_REQUESTS_TYPE + METRIC_FALLBACK);


    meterMapping.put(APPEND_INIT, METRIC_METHOD_APPEND_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(PUT_INIT, METRIC_METHOD_PUT_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(BATCH_PUT_INIT, METRIC_METHOD_BATCH_PUT_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(CAS_INIT, METRIC_METHOD_CAS_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(GET_INIT, METRIC_METHOD_READ_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(SCAN_INIT, METRIC_METHOD_SCAN_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(BATCH_GET_INIT, METRIC_METHOD_BATCH_READ_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(BATCH_DELETE_INIT, METRIC_METHOD_BATCH_DELETE_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(CHECK_DELETE_INIT, METRIC_METHOD_COMPARE_DELETE_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(INDEX_GET_INIT, METRIC_METHOD_INDEX_READ_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);
    meterMapping.put(INCREMENT_INIT, METRIC_METHOD_INCREMENT_NAME + METRIC_REQUESTS_TYPE + METRIC_INIT);

    meterMapping.put(BATCH_GET_COMPLETE, METRIC_METHOD_BATCH_READ_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);
    meterMapping.put(GET_COMPLETE, METRIC_METHOD_READ_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);
    meterMapping.put(SCAN_COMPLETE, METRIC_METHOD_SCAN_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);
    meterMapping.put(CAS_COMPLETE, METRIC_METHOD_CAS_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);
    meterMapping.put(APPEND_COMPLETE, METRIC_METHOD_APPEND_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);
    meterMapping.put(PUT_COMPLETE, METRIC_METHOD_PUT_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);
    meterMapping.put(BATCH_PUT_COMPLETE, METRIC_METHOD_BATCH_PUT_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);
    meterMapping.put(BATCH_DELETE_COMPLETE, METRIC_METHOD_BATCH_DELETE_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);
    meterMapping.put(CHECK_DELETE_COMPLETE, METRIC_METHOD_COMPARE_DELETE_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);
    meterMapping.put(INDEX_GET_COMPLETE, METRIC_METHOD_INDEX_READ_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);
    meterMapping.put(INCREMENT_COMPLETE, METRIC_METHOD_INDEX_READ_NAME + METRIC_REQUESTS_TYPE + METRIC_COMPLETE);
  }

  public void init() {
    for (String key : meterMapping.keySet()) {
      incrementMetric(key, 0L);
    }
    for (String key : timerMapping.keySet()) {
      getTimer(key).close();
    }

    for (String key : counterMapping.keySet()) {
      updateCounter(key, 0L);
    }
  }

  public void incrementMetric(String metricName, long value) {
    this.publisher.markMeter(meterMapping.get(metricName), value);
  }

  public void incrementMetric(String metricName) {
    this.publisher.markMeter(meterMapping.get(metricName));
  }

  public void incrementMetric(String metricName, boolean flag) {
    this.publisher.markMeter(meterMapping.get(metricName), flag);
  }

  public void updateThreadCounter(long activeCount, long queueSize, long threadCount) {
    updateCounter(StoreClientMetricsPublisher.ACTIVE_THREAD_COUNT, activeCount);
    updateCounter(StoreClientMetricsPublisher.QUEUE_SIZE, queueSize);
    updateCounter(StoreClientMetricsPublisher.THREAD_COUNT, threadCount);
  }

  public void updateCounter(String metricName, long newValue) {
    Counter counter = this.publisher.getCounter(counterMapping.get(metricName));
    long diff = counter.getCount() - newValue;
    if (diff > 0) {
      counter.dec(Math.abs(diff));
    } else if (diff < 0) {
      counter.inc(Math.abs(diff));
    }
  }

  public Timer.Context getTimer(String metricName) {
    return this.publisher.getTimer(timerMapping.get(metricName)).time();
  }
}
