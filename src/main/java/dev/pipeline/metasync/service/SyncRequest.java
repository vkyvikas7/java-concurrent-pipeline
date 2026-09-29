package dev.pipeline.metasync.service;

import dev.pipeline.metasync.domain.SyncMode;
import dev.pipeline.metasync.pipeline.OrderingMode;

import java.util.Objects;

/**
 * Fully resolved run parameters. The HTTP layer fills defaults before building this.
 */
public record SyncRequest(
        SyncMode mode,
        int itemCount,
        int latencyMillis,
        int workerCount,
        int queueCapacity,
        int batchSize,
        OrderingMode ordering
) {
    public static final int MAX_ITEM_COUNT = 20_000;
    public static final int MAX_LATENCY_MILLIS = 200;
    public static final int MAX_WORKERS = 64;
    public static final int MAX_QUEUE_CAPACITY = 10_000;
    public static final int MAX_BATCH_SIZE = 5_000;
    /**
     * Refuse a stage whose simulated sleep would run longer than this.
     * Sequential (and BOTH, which includes sequential) uses {@code itemCount * latency}.
     * Concurrent uses that product divided by the worker count.
     */
    public static final long MAX_ESTIMATED_STAGE_MILLIS = 120_000L;

    public SyncRequest {
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(ordering, "ordering");
        if (itemCount < 1 || itemCount > MAX_ITEM_COUNT) {
            throw new RejectedRunException("itemCount must be between 1 and " + MAX_ITEM_COUNT);
        }
        if (latencyMillis < 0 || latencyMillis > MAX_LATENCY_MILLIS) {
            throw new RejectedRunException("latencyMillis must be between 0 and " + MAX_LATENCY_MILLIS);
        }
        if (workerCount < 1 || workerCount > MAX_WORKERS) {
            throw new RejectedRunException("workerCount must be between 1 and " + MAX_WORKERS);
        }
        if (queueCapacity < 1 || queueCapacity > MAX_QUEUE_CAPACITY) {
            throw new RejectedRunException("queueCapacity must be between 1 and " + MAX_QUEUE_CAPACITY);
        }
        if (batchSize < 1 || batchSize > MAX_BATCH_SIZE) {
            throw new RejectedRunException("batchSize must be between 1 and " + MAX_BATCH_SIZE);
        }
        long sequentialMillis = (long) itemCount * latencyMillis;
        if (mode != SyncMode.CONCURRENT && sequentialMillis > MAX_ESTIMATED_STAGE_MILLIS) {
            throw new RejectedRunException(
                    "refusing run: sequential stage would sleep about " + sequentialMillis
                            + "ms. Lower itemCount or latencyMillis (limit "
                            + MAX_ESTIMATED_STAGE_MILLIS + "ms).");
        }
        long concurrentMillis = sequentialMillis / workerCount;
        if (concurrentMillis > MAX_ESTIMATED_STAGE_MILLIS) {
            throw new RejectedRunException(
                    "refusing run: concurrent stage would sleep about " + concurrentMillis
                            + "ms. Lower itemCount or latencyMillis, or raise workerCount (limit "
                            + MAX_ESTIMATED_STAGE_MILLIS + "ms).");
        }
    }
}
