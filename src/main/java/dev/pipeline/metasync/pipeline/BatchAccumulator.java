package dev.pipeline.metasync.pipeline;

import java.util.ArrayList;
import java.util.List;

/**
 * Buffers items and flushes every {@code batchSize} elements, plus a final partial batch.
 * Not thread-safe. Both engines call it from exactly one thread.
 */
final class BatchAccumulator<T> {

    private final int batchSize;
    private final BatchConsumer<T> consumer;
    private final List<T> buffer;
    private int flushes;

    BatchAccumulator(int batchSize, BatchConsumer<T> consumer) {
        this.batchSize = batchSize;
        this.consumer = consumer;
        this.buffer = new ArrayList<>(batchSize);
    }

    void add(T item) {
        buffer.add(item);
        if (buffer.size() == batchSize) {
            flush();
        }
    }

    int finish() {
        flush();
        return flushes;
    }

    private void flush() {
        if (buffer.isEmpty()) {
            return;
        }
        consumer.acceptBatch(List.copyOf(buffer));
        buffer.clear();
        flushes++;
    }
}
