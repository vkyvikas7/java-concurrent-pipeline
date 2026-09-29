package dev.pipeline.metasync.domain;

/**
 * Which engine (or engines) a demo run should execute.
 */
public enum SyncMode {
    SEQUENTIAL,
    CONCURRENT,
    BOTH
}
