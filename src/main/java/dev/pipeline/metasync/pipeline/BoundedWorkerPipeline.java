package dev.pipeline.metasync.pipeline;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * Fixed worker pool between two bounded queues.
 * <p>
 * Ingress fills until {@link PipelineSettings#queueCapacity()}, then the producer blocks.
 * Workers block when the egress queue is full, which stops them taking more ingress work,
 * so a slow sink pushes back all the way to the caller. Nothing in the pipeline grows with
 * the input size except the caller's own list (and the reorder buffer when ordering is on).
 * <p>
 * Completion uses one poison pill per worker, then a single stop message on the egress
 * queue after every worker has exited. That orders the sink's terminal flush after the
 * last result without a second unbounded buffer.
 */
public final class BoundedWorkerPipeline<I, O> {

    public PipelineResult process(
            List<I> inputs,
            ItemProcessor<I, O> processor,
            BatchConsumer<O> consumer,
            PipelineSettings settings
    ) {
        return process(inputs, processor, consumer, settings, PipelineProbe.noop());
    }

    public PipelineResult process(
            List<I> inputs,
            ItemProcessor<I, O> processor,
            BatchConsumer<O> consumer,
            PipelineSettings settings,
            PipelineProbe probe
    ) {
        Objects.requireNonNull(inputs, "inputs");
        Objects.requireNonNull(processor, "processor");
        Objects.requireNonNull(consumer, "consumer");
        Objects.requireNonNull(settings, "settings");
        Objects.requireNonNull(probe, "probe");

        if (inputs.isEmpty()) {
            return empty(settings);
        }

        BlockingQueue<Inbound<I>> ingress = new ArrayBlockingQueue<>(settings.queueCapacity());
        BlockingQueue<Outbound<O>> egress = new ArrayBlockingQueue<>(settings.queueCapacity());
        AtomicInteger ingressHighWatermark = new AtomicInteger();
        AtomicInteger egressHighWatermark = new AtomicInteger();
        LongAdder blockedIngress = new LongAdder();
        LongAdder blockedEgress = new LongAdder();
        AtomicReference<Throwable> workerError = new AtomicReference<>();
        CountDownLatch workersDone = new CountDownLatch(settings.workerCount());

        ExecutorService workers = Executors.newFixedThreadPool(
                settings.workerCount(), new NamedThreadFactory("pipeline-worker"));
        ExecutorService sinkExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory("pipeline-sink"));
        Future<SinkOutcome> sinkFuture = sinkExecutor.submit(() -> drain(egress, consumer, settings));

        for (int i = 0; i < settings.workerCount(); i++) {
            workers.submit(() -> workerLoop(
                    ingress,
                    egress,
                    processor,
                    settings.runTimeout(),
                    workersDone,
                    workerError,
                    egressHighWatermark,
                    blockedEgress,
                    probe
            ));
        }

        long start = System.nanoTime();
        try {
            for (int i = 0; i < inputs.size(); i++) {
                I item = Objects.requireNonNull(inputs.get(i), "null item at " + i);
                offer(ingress, new Inbound.Event<>(i, item), blockedIngress, probe::onIngressBlocked,
                        settings.runTimeout(), ingressHighWatermark);
            }
            for (int i = 0; i < settings.workerCount(); i++) {
                offer(ingress, new Inbound.Stop<>(), blockedIngress, probe::onIngressBlocked,
                        settings.runTimeout(), ingressHighWatermark);
            }
            if (!workersDone.await(settings.runTimeout().toNanos(), TimeUnit.NANOSECONDS)) {
                throw new PipelineException("workers did not finish within " + settings.runTimeout());
            }
            Throwable workerFailure = workerError.get();
            if (workerFailure != null) {
                throw new PipelineException("worker failed: " + workerFailure.getMessage(), workerFailure);
            }
            offer(egress, new Outbound.Stop<>(), blockedEgress, probe::onEgressBlocked,
                    settings.runTimeout(), egressHighWatermark);
            SinkOutcome outcome = sinkFuture.get(settings.runTimeout().toNanos(), TimeUnit.NANOSECONDS);
            if (outcome.succeeded() + outcome.failed() != inputs.size()) {
                throw new PipelineException("lost items: submitted=" + inputs.size()
                        + " succeeded=" + outcome.succeeded()
                        + " failed=" + outcome.failed());
            }
            long elapsed = System.nanoTime() - start;
            return new PipelineResult(
                    PipelineResult.Engine.CONCURRENT,
                    inputs.size(),
                    outcome.succeeded(),
                    outcome.failed(),
                    outcome.batchesFlushed(),
                    elapsed,
                    Math.round(elapsed / 1_000_000.0),
                    PipelineResult.ratePerSecond(outcome.succeeded(), elapsed),
                    settings.workerCount(),
                    settings.queueCapacity(),
                    settings.batchSize(),
                    settings.ordering(),
                    ingressHighWatermark.get(),
                    egressHighWatermark.get(),
                    blockedIngress.sum(),
                    blockedEgress.sum(),
                    outcome.reorderHighWatermark(),
                    outcome.sampleErrors()
            );
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new PipelineException("pipeline interrupted", interrupted);
        } catch (ExecutionException execution) {
            Throwable cause = execution.getCause() == null ? execution : execution.getCause();
            throw new PipelineException("sink failed: " + cause.getMessage(), cause);
        } catch (java.util.concurrent.TimeoutException timeout) {
            throw new PipelineException("sink did not finish within " + settings.runTimeout(), timeout);
        } finally {
            workers.shutdownNow();
            sinkExecutor.shutdownNow();
        }
    }

    private void workerLoop(
            BlockingQueue<Inbound<I>> ingress,
            BlockingQueue<Outbound<O>> egress,
            ItemProcessor<I, O> processor,
            Duration timeout,
            CountDownLatch workersDone,
            AtomicReference<Throwable> workerError,
            AtomicInteger egressHighWatermark,
            LongAdder blockedEgress,
            PipelineProbe probe
    ) {
        try {
            while (true) {
                Inbound<I> inbound;
                try {
                    inbound = ingress.take();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    workerError.compareAndSet(null, interrupted);
                    return;
                }
                if (inbound instanceof Inbound.Stop<I>) {
                    return;
                }
                if (!(inbound instanceof Inbound.Event<I> event)) {
                    continue;
                }
                try {
                    O value = processor.process(event.item());
                    if (value == null) {
                        publish(egress, new Outbound.Failure<>(event.sequence(),
                                        "seq=" + event.sequence() + " processor returned null"),
                                blockedEgress, probe, timeout, egressHighWatermark);
                        continue;
                    }
                    publish(egress, new Outbound.Success<>(event.sequence(), value),
                            blockedEgress, probe, timeout, egressHighWatermark);
                } catch (PipelineException pipeline) {
                    workerError.compareAndSet(null, pipeline);
                    return;
                } catch (Exception ex) {
                    if (ex instanceof InterruptedException || Thread.currentThread().isInterrupted()) {
                        Thread.currentThread().interrupt();
                        workerError.compareAndSet(null, ex);
                        return;
                    }
                    publish(egress, new Outbound.Failure<>(event.sequence(),
                                    SequentialProcessor.describe(event.sequence(), ex)),
                            blockedEgress, probe, timeout, egressHighWatermark);
                }
            }
        } catch (RuntimeException ex) {
            workerError.compareAndSet(null, ex);
        } finally {
            workersDone.countDown();
        }
    }

    private SinkOutcome drain(
            BlockingQueue<Outbound<O>> egress,
            BatchConsumer<O> consumer,
            PipelineSettings settings
    ) throws InterruptedException {
        BatchAccumulator<O> accumulator = new BatchAccumulator<>(settings.batchSize(), consumer);
        ReorderBuffer<Outbound<O>> reorder = settings.ordering() == OrderingMode.ORDERED
                ? new ReorderBuffer<>()
                : null;
        int succeeded = 0;
        int failed = 0;
        List<String> errors = new ArrayList<>();
        while (true) {
            Outbound<O> message = egress.take();
            if (message instanceof Outbound.Stop<O>) {
                break;
            }
            if (reorder == null) {
                int[] counts = apply(message, accumulator, errors);
                succeeded += counts[0];
                failed += counts[1];
            } else {
                long sequence = sequenceOf(message);
                for (Outbound<O> ready : reorder.push(sequence, message)) {
                    int[] counts = apply(ready, accumulator, errors);
                    succeeded += counts[0];
                    failed += counts[1];
                }
            }
        }
        if (reorder != null && reorder.pending() != 0) {
            throw new PipelineException("ordered sink finished with " + reorder.pending() + " results still buffered");
        }
        int flushes = accumulator.finish();
        return new SinkOutcome(succeeded, failed, flushes, errors, reorder == null ? 0 : reorder.highWatermark());
    }

    private static <O> long sequenceOf(Outbound<O> message) {
        if (message instanceof Outbound.Success<O> success) {
            return success.sequence();
        }
        if (message instanceof Outbound.Failure<O> failure) {
            return failure.sequence();
        }
        throw new PipelineException("stop message has no sequence");
    }

    private static <O> int[] apply(Outbound<O> message, BatchAccumulator<O> accumulator, List<String> errors) {
        if (message instanceof Outbound.Success<O> success) {
            accumulator.add(success.value());
            return new int[]{1, 0};
        }
        if (message instanceof Outbound.Failure<O> failure) {
            SequentialProcessor.remember(errors, failure.description());
            return new int[]{0, 1};
        }
        throw new PipelineException("unexpected sink message " + message);
    }

    private void publish(
            BlockingQueue<Outbound<O>> egress,
            Outbound<O> message,
            LongAdder blockedEgress,
            PipelineProbe probe,
            Duration timeout,
            AtomicInteger egressHighWatermark
    ) {
        try {
            offer(egress, message, blockedEgress, probe::onEgressBlocked, timeout, egressHighWatermark);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new PipelineException("interrupted while publishing a result", interrupted);
        }
    }

    private static <T> void offer(
            BlockingQueue<T> queue,
            T item,
            LongAdder blocked,
            Runnable onBlocked,
            Duration timeout,
            AtomicInteger highWatermark
    ) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        boolean countedBlock = false;
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                throw new PipelineException("timed out after " + timeout.toMillis()
                        + "ms waiting for queue capacity (backpressure)");
            }
            if (!countedBlock && queue.remainingCapacity() == 0) {
                blocked.increment();
                onBlocked.run();
                countedBlock = true;
            }
            long slice = Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(50));
            if (queue.offer(item, slice, TimeUnit.NANOSECONDS)) {
                highWatermark.accumulateAndGet(queue.size(), Math::max);
                return;
            }
        }
    }

    private static PipelineResult empty(PipelineSettings settings) {
        return new PipelineResult(
                PipelineResult.Engine.CONCURRENT,
                0,
                0,
                0,
                0,
                0,
                0,
                0.0,
                settings.workerCount(),
                settings.queueCapacity(),
                settings.batchSize(),
                settings.ordering(),
                0,
                0,
                0,
                0,
                0,
                List.of()
        );
    }

    private record SinkOutcome(
            int succeeded,
            int failed,
            int batchesFlushed,
            List<String> sampleErrors,
            int reorderHighWatermark
    ) {
    }
}
