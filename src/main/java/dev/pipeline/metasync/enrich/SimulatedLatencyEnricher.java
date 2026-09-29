package dev.pipeline.metasync.enrich;

import dev.pipeline.metasync.domain.AssetEvent;
import dev.pipeline.metasync.domain.AssetType;
import dev.pipeline.metasync.domain.EnrichedAsset;

import java.time.Duration;
import java.util.Objects;

/**
 * Stateless enricher that sleeps for a fixed latency to model a blocking HTTP or gRPC call.
 * The mapping itself is deterministic so two engines can be checked for the same output.
 */
public final class SimulatedLatencyEnricher implements MetadataEnricher {

    private final long latencyMillis;

    public SimulatedLatencyEnricher(Duration latency) {
        Objects.requireNonNull(latency, "latency");
        if (latency.isNegative()) {
            throw new IllegalArgumentException("latency must be >= 0");
        }
        this.latencyMillis = latency.toMillis();
    }

    @Override
    public EnrichedAsset process(AssetEvent event) throws EnrichmentException {
        Objects.requireNonNull(event, "event");
        if (latencyMillis > 0) {
            try {
                Thread.sleep(latencyMillis);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new EnrichmentException("interrupted while enriching " + event.eventId(), interrupted);
            }
        }
        return enrich(event);
    }

    public static EnrichedAsset enrich(AssetEvent event) {
        return new EnrichedAsset(
                event.eventId(),
                event.assetId(),
                event.assetType(),
                event.changeType(),
                classification(event.assetType()),
                owner(event.assetId()),
                lineageDepth(event.assetId())
        );
    }

    public static String classification(AssetType type) {
        return switch (type) {
            case TABLE -> "STRUCTURED";
            case COLUMN -> "ATTRIBUTE";
            case DASHBOARD -> "ANALYTICAL";
            case MODEL -> "PREDICTIVE";
        };
    }

    public static String owner(String assetId) {
        return "owner-" + Math.floorMod(assetId.hashCode(), 8);
    }

    public static int lineageDepth(String assetId) {
        return Math.floorMod(assetId.hashCode(), 4);
    }
}
