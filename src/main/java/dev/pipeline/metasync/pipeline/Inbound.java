package dev.pipeline.metasync.pipeline;

/**
 * Work handed from the producer to the worker pool. {@link Stop} is a poison pill:
 * the producer enqueues one per worker after the last event.
 */
sealed interface Inbound<I> permits Inbound.Event, Inbound.Stop {

    record Event<I>(long sequence, I item) implements Inbound<I> {
    }

    record Stop<I>() implements Inbound<I> {
    }
}
