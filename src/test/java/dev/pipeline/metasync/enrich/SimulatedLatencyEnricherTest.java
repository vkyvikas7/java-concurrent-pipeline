package dev.pipeline.metasync.enrich;

import dev.pipeline.metasync.domain.AssetEvent;
import dev.pipeline.metasync.domain.AssetType;
import dev.pipeline.metasync.domain.ChangeType;
import dev.pipeline.metasync.domain.EnrichedAsset;
import dev.pipeline.metasync.source.AssetEventFactory;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SimulatedLatencyEnricherTest {

    @Test
    void mapsClassificationOwnerAndLineageDeterministically() throws Exception {
        AssetEvent event = new AssetEvent(
                "evt-1",
                "asset-7",
                AssetType.DASHBOARD,
                ChangeType.UPDATED,
                10L,
                Map.of("name", "revenue")
        );
        SimulatedLatencyEnricher enricher = new SimulatedLatencyEnricher(Duration.ZERO);

        EnrichedAsset first = enricher.process(event);
        EnrichedAsset second = enricher.process(event);

        assertThat(first).isEqualTo(second);
        assertThat(first.classification()).isEqualTo("ANALYTICAL");
        assertThat(first.owner()).isEqualTo(SimulatedLatencyEnricher.owner("asset-7"));
        assertThat(first.lineageDepth()).isBetween(0, 3);
        assertThat(first.changeType()).isEqualTo(ChangeType.UPDATED);
    }

    @Test
    void factoryEventsRoundTripThroughEveryAssetType() throws Exception {
        SimulatedLatencyEnricher enricher = new SimulatedLatencyEnricher(Duration.ZERO);
        List<AssetEvent> events = AssetEventFactory.generate(4);

        assertThat(events).extracting(AssetEvent::eventId).containsExactly("evt-0", "evt-1", "evt-2", "evt-3");
        assertThat(enricher.process(events.get(0)).classification()).isEqualTo("STRUCTURED");
        assertThat(enricher.process(events.get(1)).classification()).isEqualTo("ATTRIBUTE");
        assertThat(enricher.process(events.get(2)).classification()).isEqualTo("ANALYTICAL");
        assertThat(enricher.process(events.get(3)).classification()).isEqualTo("PREDICTIVE");
    }

    @Test
    void sleepIsAtLeastTheRequestedLatency() throws Exception {
        AssetEvent event = AssetEventFactory.generate(1).get(0);
        SimulatedLatencyEnricher enricher = new SimulatedLatencyEnricher(Duration.ofMillis(30));
        long start = System.nanoTime();
        enricher.process(event);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
        assertThat(elapsedMs).isGreaterThanOrEqualTo(25);
    }
}
