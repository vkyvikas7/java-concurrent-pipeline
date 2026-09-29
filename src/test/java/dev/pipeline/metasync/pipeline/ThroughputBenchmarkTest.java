package dev.pipeline.metasync.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Locale;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sleep stands in for a blocking metadata lookup. Eight workers should overlap that wait.
 * The floor is 3x rather than the ideal 8x so a noisy shared CI host still passes.
 */
class ThroughputBenchmarkTest {

    static final int ITEMS = 160;
    static final int LATENCY_MS = 20;
    static final int WORKERS = 8;
    static final double MINIMUM_SPEEDUP = 3.0;

    @Test
    @Timeout(30)
    void concurrentPipelineIsAtLeastThreeTimesFaster() throws Exception {
        List<Integer> warmup = IntStream.range(0, 16).boxed().toList();
        List<Integer> inputs = IntStream.range(0, ITEMS).boxed().toList();
        ItemProcessor<Integer, Integer> work = item -> {
            Thread.sleep(LATENCY_MS);
            return item;
        };
        SequentialProcessor<Integer, Integer> sequential = new SequentialProcessor<>();
        BoundedWorkerPipeline<Integer, Integer> concurrent = new BoundedWorkerPipeline<>();
        PipelineSettings settings = PipelineSettings.of(WORKERS, 32, 40, OrderingMode.UNORDERED);

        sequential.process(warmup, work, batch -> {
        }, 40);
        concurrent.process(warmup, work, batch -> {
        }, settings);

        PipelineResult before = sequential.process(inputs, work, batch -> {
        }, 40);
        PipelineResult after = concurrent.process(inputs, work, batch -> {
        }, settings);

        double speedup = before.wallClockNanos() / (double) after.wallClockNanos();
        System.out.printf(Locale.US,
                "BENCHMARK items=%d latencyMs=%d workers=%d sequentialMs=%d concurrentMs=%d sequentialItemsPerSec=%.2f concurrentItemsPerSec=%.2f speedup=%.2f%n",
                ITEMS,
                LATENCY_MS,
                WORKERS,
                before.wallClockMillis(),
                after.wallClockMillis(),
                before.itemsPerSecond(),
                after.itemsPerSecond(),
                speedup);

        assertThat(before.succeeded()).isEqualTo(ITEMS);
        assertThat(after.succeeded()).isEqualTo(ITEMS);
        assertThat(before.wallClockMillis())
                .as("sequential run should actually wait on the simulated lookup")
                .isGreaterThanOrEqualTo((long) (ITEMS * LATENCY_MS * 0.80));
        assertThat(speedup)
                .as("concurrent wall clock should be <= 1/3 of sequential")
                .isGreaterThanOrEqualTo(MINIMUM_SPEEDUP);
        assertThat(after.itemsPerSecond()).isGreaterThanOrEqualTo(before.itemsPerSecond() * MINIMUM_SPEEDUP);
    }
}
