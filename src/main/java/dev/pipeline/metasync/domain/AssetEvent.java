package dev.pipeline.metasync.domain;

import java.util.Map;
import java.util.Objects;

/**
 * One source change to apply during a metadata sync.
 * {@code attributes} is copied so callers cannot mutate an event after it is queued.
 */
public record AssetEvent(
        String eventId,
        String assetId,
        AssetType assetType,
        ChangeType changeType,
        long sourceTimestampMillis,
        Map<String, String> attributes
) {
    public AssetEvent {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(assetId, "assetId");
        Objects.requireNonNull(assetType, "assetType");
        Objects.requireNonNull(changeType, "changeType");
        attributes = Map.copyOf(Objects.requireNonNull(attributes, "attributes"));
    }
}
