package dev.pipeline.metasync.service;

import dev.pipeline.metasync.domain.EnrichedAsset;
import dev.pipeline.metasync.pipeline.OrderingMode;
import dev.pipeline.metasync.pipeline.PipelineResult;

import java.util.List;

/**
 * API-facing metrics for one engine. Rates use successful items over wall-clock time.
 */
public record SyncReport(
        PipelineResult.Engine engine,
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
        List<String> sampleErrors,
        List<EnrichedAsset> sampleAssets
) {
    public SyncReport {
        sampleErrors = List.copyOf(sampleErrors);
        sampleAssets = List.copyOf(sampleAssets);
    }

    static SyncReport from(PipelineResult result, List<EnrichedAsset> assets) {
        return new SyncReport(
                result.engine(),
                result.submitted(),
                result.succeeded(),
                result.failed(),
                result.batchesFlushed(),
                result.wallClockNanos(),
                result.wallClockMillis(),
                round2(result.itemsPerSecond()),
                result.workerCount(),
                result.queueCapacity(),
                result.batchSize(),
                result.ordering(),
                result.ingressHighWatermark(),
                result.egressHighWatermark(),
                result.blockedIngressSubmissions(),
                result.blockedEgressSubmissions(),
                result.reorderHighWatermark(),
                result.sampleErrors(),
                sample(assets)
        );
    }

    private static List<EnrichedAsset> sample(List<EnrichedAsset> assets) {
        int limit = Math.min(5, assets.size());
        return List.copyOf(assets.subList(0, limit));
    }

    static double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
