package dev.pipeline.metasync.pipeline;

import java.time.Duration;
import java.util.Objects;

/**
 * Knobs for one concurrent run. The queue capacity is the backpressure bound:
 * the producer blocks when ingress is full, and workers block when egress is full.
 */
public record PipelineSettings(
        int workerCount,
        int queueCapacity,
        int batchSize,
        OrderingMode ordering,
        Duration runTimeout
) {
    public PipelineSettings {
        if (workerCount < 1 || workerCount > 256) {
            throw new IllegalArgumentException("workerCount must be between 1 and 256");
        }
        if (queueCapacity < 1) {
            throw new IllegalArgumentException("queueCapacity must be >= 1");
        }
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
        Objects.requireNonNull(ordering, "ordering");
        Objects.requireNonNull(runTimeout, "runTimeout");
        if (runTimeout.isZero() || runTimeout.isNegative()) {
            throw new IllegalArgumentException("runTimeout must be positive");
        }
    }

    public static PipelineSettings of(int workerCount, int queueCapacity, int batchSize, OrderingMode ordering) {
        return new PipelineSettings(workerCount, queueCapacity, batchSize, ordering, Duration.ofMinutes(10));
    }
}
