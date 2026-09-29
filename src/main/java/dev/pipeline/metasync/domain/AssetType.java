package dev.pipeline.metasync.domain;

/**
 * Kind of catalog asset carried by a sync event.
 */
public enum AssetType {
    TABLE,
    COLUMN,
    DASHBOARD,
    MODEL
}
