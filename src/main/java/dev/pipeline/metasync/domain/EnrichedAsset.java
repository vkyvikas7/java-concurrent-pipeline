package dev.pipeline.metasync.domain;

/**
 * Asset change after classification, ownership, and lineage lookup.
 */
public record EnrichedAsset(
        String eventId,
        String assetId,
        AssetType assetType,
        ChangeType changeType,
        String classification,
        String owner,
        int lineageDepth
) {
}
