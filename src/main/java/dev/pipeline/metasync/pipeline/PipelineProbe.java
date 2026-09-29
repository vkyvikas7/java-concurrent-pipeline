package dev.pipeline.metasync.pipeline;

/**
 * Optional live signals for tests and operators. The counters on {@link PipelineResult}
 * are the source of truth after a run returns; this hook fires while the run is blocked.
 */
public interface PipelineProbe {

    default void onIngressBlocked() {
    }

    default void onEgressBlocked() {
    }

    static PipelineProbe noop() {
        return new PipelineProbe() {
        };
    }
}
