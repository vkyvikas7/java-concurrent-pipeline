package dev.pipeline.metasync.cli;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "pipeline.cli.enabled=true",
        "pipeline.cli.mode=BOTH",
        "pipeline.cli.items=24",
        "pipeline.cli.latency-ms=0",
        "pipeline.cli.workers=4",
        "pipeline.cli.queue-capacity=8",
        "pipeline.cli.batch-size=8",
        "pipeline.cli.ordering=ORDERED"
})
class PipelineCliRunnerTest {

    @Autowired
    private PipelineCliRunner runner;

    @Test
    void runsTheBenchmarkWhenTheCliPropertyIsEnabled() {
        assertThat(runner.lastResponse()).isNotNull();
        assertThat(runner.lastResponse().sequential().succeeded()).isEqualTo(24);
        assertThat(runner.lastResponse().concurrent().succeeded()).isEqualTo(24);
        assertThat(runner.lastResponse().concurrent().sampleAssets().get(0).eventId()).isEqualTo("evt-0");
    }
}
