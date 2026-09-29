package dev.pipeline.metasync.service;

/**
 * The requested run was rejected before any worker started (validation or size guard).
 */
public class RejectedRunException extends RuntimeException {

    public RejectedRunException(String message) {
        super(message);
    }
}
