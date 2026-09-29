package dev.pipeline.metasync.enrich;

/**
 * A single asset could not be enriched. The pipeline records the failure and continues.
 */
public class EnrichmentException extends Exception {

    public EnrichmentException(String message) {
        super(message);
    }

    public EnrichmentException(String message, Throwable cause) {
        super(message, cause);
    }
}
