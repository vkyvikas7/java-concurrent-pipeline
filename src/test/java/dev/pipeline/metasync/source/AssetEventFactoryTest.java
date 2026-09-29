package dev.pipeline.metasync.source;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AssetEventFactoryTest {

    @Test
    void generationIsStableAndIdsAreUnique() {
        assertThat(AssetEventFactory.generate(50)).isEqualTo(AssetEventFactory.generate(50));
        assertThat(AssetEventFactory.generate(50))
                .extracting(event -> event.eventId())
                .doesNotHaveDuplicates();
    }
}
