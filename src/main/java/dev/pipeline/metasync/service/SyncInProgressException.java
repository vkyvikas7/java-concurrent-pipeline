package dev.pipeline.metasync.service;

/**
 * Only one demo run executes at a time so a second caller cannot oversubscribe the machine.
 */
public class SyncInProgressException extends RuntimeException {

    public SyncInProgressException() {
        super("a metadata sync run is already in progress");
    }
}
