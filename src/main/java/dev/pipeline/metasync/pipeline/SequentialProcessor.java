package dev.pipeline.metasync.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * One-item-at-a-time baseline. Same processor, same batch size, and same failure
 * isolation as {@link BoundedWorkerPipeline}, so a speedup is attributable to parallelism.
 */
public final class SequentialProcessor<I, O> {

    static final int MAX_SAMPLE_ERRORS = 10;

    public PipelineResult process(
            List<I> inputs,
            ItemProcessor<I, O> processor,
            BatchConsumer<O> consumer,
            int batchSize
    ) {
        Objects.requireNonNull(inputs, "inputs");
        Objects.requireNonNull(processor, "processor");
        Objects.requireNonNull(consumer, "consumer");
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }

        long start = System.nanoTime();
        BatchAccumulator<O> accumulator = new BatchAccumulator<>(batchSize, consumer);
        int failed = 0;
        List<String> errors = new ArrayList<>();
        for (int i = 0; i < inputs.size(); i++) {
            I item = Objects.requireNonNull(inputs.get(i), "null item at " + i);
            try {
                O value = processor.process(item);
                if (value == null) {
                    failed++;
                    remember(errors, "seq=" + i + " processor returned null");
                    continue;
                }
                accumulator.add(value);
            } catch (Exception ex) {
                if (ex instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                    throw new PipelineException("sequential processor interrupted", ex);
                }
                failed++;
                remember(errors, describe(i, ex));
            }
        }
        int flushes = accumulator.finish();
        long elapsed = System.nanoTime() - start;
        int succeeded = inputs.size() - failed;
        return new PipelineResult(
                PipelineResult.Engine.SEQUENTIAL,
                inputs.size(),
                succeeded,
                failed,
                flushes,
                elapsed,
                Math.round(elapsed / 1_000_000.0),
                PipelineResult.ratePerSecond(succeeded, elapsed),
                1,
                0,
                batchSize,
                OrderingMode.ORDERED,
                0,
                0,
                0,
                0,
                0,
                errors
        );
    }

    static void remember(List<String> errors, String description) {
        if (errors.size() < MAX_SAMPLE_ERRORS) {
            errors.add(description);
        }
    }

    static String describe(long sequence, Exception ex) {
        String message = ex.getMessage();
        if (message == null || message.isBlank()) {
            message = ex.getClass().getSimpleName();
        }
        return "seq=" + sequence + " " + message;
    }
}
