package dev.pipeline.metasync;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MetadataSyncApplicationTest {

    @Test
    void cliAliasEnablesThePipelineAndSkipsTheWebServer() {
        assertThat(MetadataSyncApplication.isCli(new String[]{"--cli"})).isTrue();
        String[] normalized = MetadataSyncApplication.normalizeArgs(new String[]{"--cli", "--pipeline.cli.items=20"});
        assertThat(normalized).contains("--pipeline.cli.enabled=true", "--cli", "--pipeline.cli.items=20");
        assertThat(MetadataSyncApplication.isCli(normalized)).isTrue();
        assertThat(MetadataSyncApplication.isCli(new String[]{"--server.port=8080"})).isFalse();
    }
}
