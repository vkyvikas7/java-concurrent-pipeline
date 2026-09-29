package dev.pipeline.metasync.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class BackpressureTest {

    @Test
    @Timeout(15)
    void slowSinkBlocksTheProducerAndStillDeliversEveryItem() throws Exception {
        int items = 30;
        int capacity = 2;
        CountDownLatch sinkEntered = new CountDownLatch(1);
        CountDownLatch releaseSink = new CountDownLatch(1);
        AtomicLong ingressBlocks = new AtomicLong();
        PipelineProbe probe = new PipelineProbe() {
            @Override
            public void onIngressBlocked() {
                ingressBlocks.incrementAndGet();
            }
        };
        PipelineSettings settings = new PipelineSettings(2, capacity, 1, OrderingMode.UNORDERED, Duration.ofSeconds(10));
        BoundedWorkerPipeline<Integer, Integer> pipeline = new BoundedWorkerPipeline<>();
        AtomicReference<PipelineResult> resultRef = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Thread runner = new Thread(() -> {
            try {
                resultRef.set(pipeline.process(
                        IntStream.range(0, items).boxed().toList(),
                        item -> item,
                        batch -> {
                            sinkEntered.countDown();
                            try {
                                if (!releaseSink.await(8, TimeUnit.SECONDS)) {
                                    throw new IllegalStateException("sink was not released");
                                }
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException("sink interrupted", interrupted);
                            }
                        },
                        settings,
                        probe
                ));
            } catch (Throwable thrown) {
                failure.set(thrown);
            }
        });
        runner.setName("backpressure-driver");
        runner.start();

        try {
            assertThat(sinkEntered.await(5, TimeUnit.SECONDS)).isTrue();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (ingressBlocks.get() == 0 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertThat(ingressBlocks.get())
                    .as("producer should observe a full ingress queue while the sink is stalled")
                    .isGreaterThan(0);
            assertThat(runner.isAlive()).isTrue();
        } finally {
            releaseSink.countDown();
            runner.join(5_000);
        }

        assertThat(failure.get()).isNull();
        assertThat(runner.isAlive()).isFalse();
        PipelineResult result = resultRef.get();
        assertThat(result).isNotNull();
        assertThat(result.succeeded()).isEqualTo(items);
        assertThat(result.failed()).isZero();
        assertThat(result.batchesFlushed()).isEqualTo(items);
        assertThat(result.blockedIngressSubmissions()).isGreaterThan(0);
        assertThat(result.ingressHighWatermark()).isLessThanOrEqualTo(capacity);
        assertThat(result.egressHighWatermark()).isLessThanOrEqualTo(capacity);
    }
}
