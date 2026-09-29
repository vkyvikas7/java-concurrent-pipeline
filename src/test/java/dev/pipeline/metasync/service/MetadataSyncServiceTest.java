package dev.pipeline.metasync.service;

import dev.pipeline.metasync.domain.AssetEvent;
import dev.pipeline.metasync.domain.EnrichedAsset;
import dev.pipeline.metasync.domain.SyncMode;
import dev.pipeline.metasync.enrich.SimulatedLatencyEnricher;
import dev.pipeline.metasync.pipeline.ItemProcessor;
import dev.pipeline.metasync.pipeline.OrderingMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetadataSyncServiceTest {

    private final MetadataSyncService service = new MetadataSyncService();

    @Test
    void bothEnginesEnrichTheSameAssets() {
        SyncRunResponse response = service.execute(new SyncRequest(
                SyncMode.BOTH, 36, 0, 4, 8, 10, OrderingMode.ORDERED));

        assertThat(response.sequential().succeeded()).isEqualTo(36);
        assertThat(response.concurrent().succeeded()).isEqualTo(36);
        assertThat(response.concurrent().failed()).isZero();
        assertThat(response.concurrent().sampleAssets()).isNotEmpty();
        assertThat(response.concurrent().sampleAssets()).hasSizeLessThanOrEqualTo(5);
        assertThat(response.concurrent().sampleAssets().get(0).eventId()).isEqualTo("evt-0");
        assertThat(response.concurrent().sampleAssets().get(0).classification()).isEqualTo("STRUCTURED");
        assertThat(response.summary()).contains("items/sec");
        assertThat(response.sequential().batchesFlushed()).isEqualTo(4);
    }

    @Test
    void rejectsASequentialRunThatWouldSleepTooLong() {
        assertThatThrownBy(() -> service.execute(new SyncRequest(
                SyncMode.SEQUENTIAL, 20_000, 200, 8, 32, 64, OrderingMode.UNORDERED)))
                .isInstanceOf(RejectedRunException.class)
                .hasMessageContaining("sequential");
    }

    @Test
    @Timeout(10)
    void secondCallIsRejectedWhileTheFirstHoldsTheGate() throws Exception {
        CountDownLatch insideProcessor = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ItemProcessor<AssetEvent, EnrichedAsset> blocking = event -> {
            insideProcessor.countDown();
            if (!release.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("not released");
            }
            return SimulatedLatencyEnricher.enrich(event);
        };
        SyncRequest slow = new SyncRequest(SyncMode.SEQUENTIAL, 4, 0, 2, 4, 4, OrderingMode.UNORDERED);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<SyncRunResponse> first = executor.submit(() -> service.execute(slow, blocking));
            assertThat(insideProcessor.await(2, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> service.execute(new SyncRequest(
                    SyncMode.CONCURRENT, 2, 0, 2, 4, 4, OrderingMode.UNORDERED)))
                    .isInstanceOf(SyncInProgressException.class);
            release.countDown();
            assertThat(first.get(5, TimeUnit.SECONDS).sequential().succeeded()).isEqualTo(4);
        } finally {
            release.countDown();
            executor.shutdownNow();
        }

        SyncRunResponse after = service.execute(new SyncRequest(
                SyncMode.CONCURRENT, 3, 0, 2, 4, 4, OrderingMode.UNORDERED));
        assertThat(after.concurrent().succeeded()).isEqualTo(3);
        assertThat(after.sequential()).isNull();
    }
}
