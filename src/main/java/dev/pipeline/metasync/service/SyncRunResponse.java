package dev.pipeline.metasync.service;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Result of a demo run. {@code speedup} is sequential wall-clock divided by concurrent wall-clock
 * and is present only when both engines ran.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SyncRunResponse(
        @JsonInclude(JsonInclude.Include.NON_NULL) SyncReport sequential,
        @JsonInclude(JsonInclude.Include.NON_NULL) SyncReport concurrent,
        @JsonInclude(JsonInclude.Include.NON_NULL) Double speedup,
        String summary
) {
}
