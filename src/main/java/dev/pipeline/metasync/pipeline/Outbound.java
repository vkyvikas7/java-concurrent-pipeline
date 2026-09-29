package dev.pipeline.metasync.pipeline;

/**
 * Result handed from a worker to the single-threaded sink.
 * Failures still occupy their sequence so an ordered sink can move past them.
 */
sealed interface Outbound<O> permits Outbound.Success, Outbound.Failure, Outbound.Stop {

    record Success<O>(long sequence, O value) implements Outbound<O> {
    }

    record Failure<O>(long sequence, String description) implements Outbound<O> {
    }

    record Stop<O>() implements Outbound<O> {
    }
}
