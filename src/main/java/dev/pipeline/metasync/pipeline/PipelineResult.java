package dev.pipeline.metasync.pipeline;

import java.util.List;

/**
 * Metrics for a finished run. {@code itemsPerSecond} is successful items divided by wall-clock seconds.
 */
public record PipelineResult(
        Engine engine,
        int submitted,
        int succeeded,
        int failed,
        int batchesFlushed,
        long wallClockNanos,
        long wallClockMillis,
        double itemsPerSecond,
        int workerCount,
        int queueCapacity,
        int batchSize,
        OrderingMode ordering,
        int ingressHighWatermark,
        int egressHighWatermark,
        long blockedIngressSubmissions,
        long blockedEgressSubmissions,
        int reorderHighWatermark,
        List<String> sampleErrors
) {
    public enum Engine {
        SEQUENTIAL,
        CONCURRENT
    }

    public PipelineResult {
        sampleErrors = List.copyOf(sampleErrors);
        if (ordering == null) {
            throw new IllegalArgumentException("ordering is required");
        }
        if (engine == null) {
            throw new IllegalArgumentException("engine is required");
        }
    }

    public static double ratePerSecond(int items, long nanos) {
        if (items <= 0) {
            return 0.0;
        }
        if (nanos <= 0) {
            return items;
        }
        return items / (nanos / 1_000_000_000.0);
    }
}
