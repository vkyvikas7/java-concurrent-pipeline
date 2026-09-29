package dev.pipeline.metasync.pipeline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds out-of-order completions until the next expected sequence arrives.
 * The caller must push every sequence exactly once, including failures, or the buffer stalls.
 */
final class ReorderBuffer<T> {

    private final Map<Long, T> pending = new HashMap<>();
    private long next;
    private int highWatermark;

    List<T> push(long sequence, T value) {
        if (sequence < next) {
            throw new PipelineException("sequence " + sequence + " is behind the commit point " + next);
        }
        if (pending.containsKey(sequence)) {
            throw new PipelineException("duplicate sequence " + sequence);
        }
        pending.put(sequence, value);
        highWatermark = Math.max(highWatermark, pending.size());
        List<T> ready = new ArrayList<>();
        T head;
        while ((head = pending.remove(next)) != null) {
            ready.add(head);
            next++;
        }
        return ready;
    }

    int pending() {
        return pending.size();
    }

    int highWatermark() {
        return highWatermark;
    }
}
