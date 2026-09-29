package dev.pipeline.metasync.service;

import dev.pipeline.metasync.domain.AssetEvent;
import dev.pipeline.metasync.domain.EnrichedAsset;
import dev.pipeline.metasync.domain.SyncMode;
import dev.pipeline.metasync.enrich.SimulatedLatencyEnricher;
import dev.pipeline.metasync.pipeline.BatchConsumer;
import dev.pipeline.metasync.pipeline.BoundedWorkerPipeline;
import dev.pipeline.metasync.pipeline.ItemProcessor;
import dev.pipeline.metasync.pipeline.PipelineResult;
import dev.pipeline.metasync.pipeline.PipelineSettings;
import dev.pipeline.metasync.pipeline.SequentialProcessor;
import dev.pipeline.metasync.source.AssetEventFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Semaphore;

/**
 * Runs the metadata sync demo. One permit is held for the whole comparison so sequential
 * and concurrent stages are not scheduled on top of each other, and a second HTTP call
 * receives {@link SyncInProgressException} instead of quietly splitting the CPU.
 */
@Service
public class MetadataSyncService {

    private static final Logger log = LoggerFactory.getLogger(MetadataSyncService.class);
    private static final Duration RUN_TIMEOUT = Duration.ofMinutes(10);

    private final Semaphore runGate = new Semaphore(1);

    public SyncRunResponse execute(SyncRequest request) {
        return execute(request, new SimulatedLatencyEnricher(Duration.ofMillis(request.latencyMillis())));
    }

    SyncRunResponse execute(SyncRequest request, ItemProcessor<AssetEvent, EnrichedAsset> processor) {
        if (!runGate.tryAcquire()) {
            throw new SyncInProgressException();
        }
        try {
            return run(request, processor);
        } finally {
            runGate.release();
        }
    }

    private SyncRunResponse run(SyncRequest request, ItemProcessor<AssetEvent, EnrichedAsset> processor) {
        List<AssetEvent> events = AssetEventFactory.generate(request.itemCount());
        SyncReport sequential = null;
        SyncReport concurrent = null;
        if (request.mode() == SyncMode.SEQUENTIAL || request.mode() == SyncMode.BOTH) {
            sequential = runSequential(events, processor, request.batchSize());
            logReport(sequential);
        }
        if (request.mode() == SyncMode.CONCURRENT || request.mode() == SyncMode.BOTH) {
            concurrent = runConcurrent(events, processor, request);
            logReport(concurrent);
        }
        Double speedup = speedup(sequential, concurrent);
        String summary = summary(sequential, concurrent, speedup);
        log.info(summary);
        return new SyncRunResponse(sequential, concurrent, speedup, summary);
    }

    private static SyncReport runSequential(
            List<AssetEvent> events,
            ItemProcessor<AssetEvent, EnrichedAsset> processor,
            int batchSize
    ) {
        MemoryBatch sink = new MemoryBatch();
        PipelineResult result = new SequentialProcessor<AssetEvent, EnrichedAsset>()
                .process(events, processor, sink, batchSize);
        return SyncReport.from(result, sink.snapshot());
    }

    private static SyncReport runConcurrent(
            List<AssetEvent> events,
            ItemProcessor<AssetEvent, EnrichedAsset> processor,
            SyncRequest request
    ) {
        MemoryBatch sink = new MemoryBatch();
        PipelineSettings settings = new PipelineSettings(
                request.workerCount(),
                request.queueCapacity(),
                request.batchSize(),
                request.ordering(),
                RUN_TIMEOUT
        );
        PipelineResult result = new BoundedWorkerPipeline<AssetEvent, EnrichedAsset>()
                .process(events, processor, sink, settings);
        return SyncReport.from(result, sink.snapshot());
    }

    private static Double speedup(SyncReport sequential, SyncReport concurrent) {
        if (sequential == null || concurrent == null) {
            return null;
        }
        if (sequential.wallClockNanos() <= 0 || concurrent.wallClockNanos() <= 0) {
            return null;
        }
        double ratio = sequential.wallClockNanos() / (double) concurrent.wallClockNanos();
        return SyncReport.round2(ratio);
    }

    private static String summary(SyncReport sequential, SyncReport concurrent, Double speedup) {
        if (sequential != null && concurrent != null && speedup != null) {
            return String.format(Locale.US,
                    "Concurrent pipeline processed %d items in %d ms (%.2f items/sec) versus sequential %d ms (%.2f items/sec). Speedup %.2fx (sequential nanos / concurrent nanos).",
                    concurrent.succeeded(),
                    concurrent.wallClockMillis(),
                    concurrent.itemsPerSecond(),
                    sequential.wallClockMillis(),
                    sequential.itemsPerSecond(),
                    speedup);
        }
        SyncReport only = concurrent != null ? concurrent : sequential;
        return String.format(Locale.US,
                "%s pipeline processed %d items in %d ms (%.2f items/sec).",
                only.engine(),
                only.succeeded(),
                only.wallClockMillis(),
                only.itemsPerSecond());
    }

    private static void logReport(SyncReport report) {
        log.info("engine={} submitted={} succeeded={} failed={} wallMs={} itemsPerSec={} blockedIngress={} ingressHighWatermark={}",
                report.engine(),
                report.submitted(),
                report.succeeded(),
                report.failed(),
                report.wallClockMillis(),
                report.itemsPerSecond(),
                report.blockedIngressSubmissions(),
                report.ingressHighWatermark());
    }

    /**
     * Single-thread sink buffer. The pipeline never calls {@link BatchConsumer#acceptBatch}
     * concurrently; synchronization here only protects the test snapshot.
     */
    private static final class MemoryBatch implements BatchConsumer<EnrichedAsset> {
        private final List<EnrichedAsset> items = new ArrayList<>();

        @Override
        public synchronized void acceptBatch(List<EnrichedAsset> batch) {
            items.addAll(batch);
        }

        synchronized List<EnrichedAsset> snapshot() {
            return List.copyOf(items);
        }
    }
}
