package dev.pipeline.metasync.pipeline;

/**
 * Per-item work. Implementations must be safe to call from multiple worker threads.
 * A thrown exception fails that item only; the rest of the batch continues.
 */
@FunctionalInterface
public interface ItemProcessor<I, O> {

    O process(I item) throws Exception;
}
