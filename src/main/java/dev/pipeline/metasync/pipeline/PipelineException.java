package dev.pipeline.metasync.pipeline;

/**
 * The run could not be completed safely (timeout, interrupt, broken sink, or a lost item).
 * Per-item processor failures are not pipeline failures; they are counted and the run continues.
 */
public class PipelineException extends RuntimeException {

    public PipelineException(String message) {
        super(message);
    }

    public PipelineException(String message, Throwable cause) {
        super(message, cause);
    }
}
