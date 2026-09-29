package dev.pipeline.metasync.api;

import dev.pipeline.metasync.domain.SyncMode;
import dev.pipeline.metasync.pipeline.OrderingMode;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/**
 * HTTP body for {@code POST /api/v1/sync/run}. Null optional fields pick demo defaults.
 */
public record SyncRunRequest(
        @NotNull SyncMode mode,
        @NotNull @Min(1) @Max(20_000) Integer itemCount,
        @Min(0) @Max(200) Integer latencyMillis,
        @Min(1) @Max(64) Integer workerCount,
        @Min(1) @Max(10_000) Integer queueCapacity,
        @Min(1) @Max(5_000) Integer batchSize,
        OrderingMode ordering
) {
}
