package dev.pipeline.metasync.source;

import dev.pipeline.metasync.domain.AssetEvent;
import dev.pipeline.metasync.domain.AssetType;
import dev.pipeline.metasync.domain.ChangeType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Builds a deterministic synthetic batch. The same count always yields the same events,
 * so sequential and concurrent runs can be compared on identical input.
 */
public final class AssetEventFactory {

    private static final long BASE_TIMESTAMP = 1_700_000_000_000L;

    private AssetEventFactory() {
    }

    public static List<AssetEvent> generate(int count) {
        if (count < 0) {
            throw new IllegalArgumentException("count must be >= 0");
        }
        AssetType[] types = AssetType.values();
        ChangeType[] changes = ChangeType.values();
        List<AssetEvent> events = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            events.add(new AssetEvent(
                    "evt-" + i,
                    "asset-" + i,
                    types[i % types.length],
                    changes[i % changes.length],
                    BASE_TIMESTAMP + i,
                    Map.of("name", "asset_" + i)
            ));
        }
        return List.copyOf(events);
    }
}
