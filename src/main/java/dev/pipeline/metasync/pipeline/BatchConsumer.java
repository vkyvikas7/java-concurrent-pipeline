package dev.pipeline.metasync.pipeline;

import java.util.List;

/**
 * Receives non-empty batches from a single thread. Implementations do not need
 * their own synchronization. Throwing fails the run; the pipeline does not retry.
 */
@FunctionalInterface
public interface BatchConsumer<T> {

    void acceptBatch(List<T> batch);
}
