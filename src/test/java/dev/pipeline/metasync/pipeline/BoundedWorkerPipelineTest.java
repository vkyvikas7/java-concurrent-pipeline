package dev.pipeline.metasync.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BoundedWorkerPipelineTest {

    private final BoundedWorkerPipeline<Integer, Integer> pipeline = new BoundedWorkerPipeline<>();

    @Test
    @Timeout(20)
    void deliversEachItemOnceUnderContention() {
        int count = 10_000;
        List<Integer> inputs = IntStream.range(0, count).boxed().toList();
        CopyOnWriteArrayList<Integer> written = new CopyOnWriteArrayList<>();
        PipelineSettings settings = PipelineSettings.of(8, 32, 64, OrderingMode.UNORDERED);

        PipelineResult result = pipeline.process(inputs, item -> item, written::addAll, settings);

        assertThat(result.succeeded()).isEqualTo(count);
        assertThat(result.failed()).isZero();
        assertThat(result.succeeded() + result.failed()).isEqualTo(result.submitted());
        assertThat(written).containsExactlyInAnyOrderElementsOf(inputs);
        assertThat(result.ingressHighWatermark()).isLessThanOrEqualTo(settings.queueCapacity());
        assertThat(result.egressHighWatermark()).isLessThanOrEqualTo(settings.queueCapacity());
        assertThat(result.batchesFlushed()).isEqualTo(expectedFlushes(count, 64));
    }

    @Test
    @Timeout(20)
    void orderedModePreservesInputOrderWhenCompletionOrderDoesNot() {
        List<Integer> inputs = IntStream.range(0, 40).boxed().toList();
        List<Integer> written = new ArrayList<>();
        PipelineSettings settings = PipelineSettings.of(8, 16, 10, OrderingMode.ORDERED);

        PipelineResult result = pipeline.process(inputs, item -> {
            Thread.sleep(item == 0 ? 40 : 1L);
            return item;
        }, written::addAll, settings);

        assertThat(result.failed()).isZero();
        assertThat(written).containsExactlyElementsOf(inputs);
        assertThat(result.reorderHighWatermark()).isGreaterThan(0);
    }

    @Test
    @Timeout(20)
    void failureDoesNotStallOrderedCommit() {
        List<Integer> inputs = IntStream.range(0, 6).boxed().toList();
        List<Integer> written = new ArrayList<>();
        PipelineSettings settings = PipelineSettings.of(4, 8, 10, OrderingMode.ORDERED);

        PipelineResult result = pipeline.process(inputs, item -> {
            Thread.sleep(item == 0 ? 30 : 1L);
            if (item == 0) {
                throw new IllegalStateException("bad-0");
            }
            return item;
        }, written::addAll, settings);

        assertThat(result.succeeded()).isEqualTo(5);
        assertThat(result.failed()).isEqualTo(1);
        assertThat(written).containsExactly(1, 2, 3, 4, 5);
        assertThat(result.sampleErrors()).anyMatch(error -> error.contains("bad-0"));
    }

    @Test
    @Timeout(20)
    void matchesSequentialMultisetAndIsolatesFailures() {
        List<Integer> inputs = IntStream.range(0, 100).boxed().toList();
        ItemProcessor<Integer, Integer> work = item -> {
            if (item % 10 == 0) {
                throw new IllegalStateException("bad-" + item);
            }
            return item;
        };
        List<Integer> sequentialOut = new ArrayList<>();
        List<Integer> concurrentOut = new ArrayList<>();
        PipelineResult sequential = new SequentialProcessor<Integer, Integer>()
                .process(inputs, work, sequentialOut::addAll, 32);
        PipelineResult concurrent = pipeline.process(
                inputs, work, concurrentOut::addAll, PipelineSettings.of(8, 16, 32, OrderingMode.UNORDERED));

        assertThat(concurrent.succeeded()).isEqualTo(sequential.succeeded()).isEqualTo(90);
        assertThat(concurrent.failed()).isEqualTo(10);
        assertThat(concurrent.batchesFlushed()).isEqualTo(3);
        assertThat(concurrentOut).containsExactlyInAnyOrderElementsOf(sequentialOut);
    }

    @Test
    @Timeout(10)
    void sinkIsInvokedFromASingleThread() {
        Set<String> threads = ConcurrentHashMap.newKeySet();
        pipeline.process(IntStream.range(0, 200).boxed().toList(), item -> item, batch -> {
            threads.add(Thread.currentThread().getName());
        }, PipelineSettings.of(8, 16, 20, OrderingMode.UNORDERED));

        assertThat(threads).hasSize(1);
        assertThat(threads.iterator().next()).startsWith("pipeline-sink");
    }

    @Test
    void emptyInputDoesNotStartWork() {
        PipelineResult result = pipeline.process(
                List.of(), item -> item, batch -> {
                    throw new AssertionError("sink should not be called");
                }, PipelineSettings.of(4, 4, 4, OrderingMode.UNORDERED));

        assertThat(result.submitted()).isZero();
        assertThat(result.batchesFlushed()).isZero();
        assertThat(result.itemsPerSecond()).isZero();
    }

    @Test
    @Timeout(10)
    void timesOutInsteadOfHangingWhenTheSinkNeverReleases() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        PipelineSettings settings = new PipelineSettings(2, 2, 1, OrderingMode.UNORDERED, Duration.ofMillis(300));
        try {
            assertThatThrownBy(() -> pipeline.process(IntStream.range(0, 20).boxed().toList(), item -> item, batch -> {
                try {
                    release.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }, settings))
                    .isInstanceOf(PipelineException.class)
                    .hasMessageContaining("timed out");
        } finally {
            release.countDown();
        }
        Thread.sleep(200);
        boolean leaked = Thread.getAllStackTraces().keySet().stream()
                .anyMatch(thread -> thread.isAlive() && thread.getName().startsWith("pipeline-"));
        assertThat(leaked).isFalse();
    }

    @Test
    void rejectsInvalidSettings() {
        assertThatThrownBy(() -> new PipelineSettings(0, 1, 1, OrderingMode.UNORDERED, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("workerCount");
        assertThatThrownBy(() -> PipelineSettings.of(2, 0, 1, OrderingMode.UNORDERED))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("queueCapacity");
    }

    private static int expectedFlushes(int items, int batchSize) {
        return (items + batchSize - 1) / batchSize;
    }
}
