package dev.pipeline.metasync.cli;

import dev.pipeline.metasync.service.MetadataSyncService;
import dev.pipeline.metasync.service.SyncRequest;
import dev.pipeline.metasync.service.SyncRunResponse;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "pipeline.cli", name = "enabled", havingValue = "true")
public class PipelineCliRunner implements CommandLineRunner {

    private final MetadataSyncService service;
    private final CliProperties properties;
    private volatile SyncRunResponse lastResponse;

    public PipelineCliRunner(MetadataSyncService service, CliProperties properties) {
        this.service = service;
        this.properties = properties;
    }

    @Override
    public void run(String... args) {
        SyncRequest request = new SyncRequest(
                properties.getMode(),
                properties.getItems(),
                properties.getLatencyMs(),
                properties.getWorkers(),
                properties.getQueueCapacity(),
                properties.getBatchSize(),
                properties.getOrdering()
        );
        lastResponse = service.execute(request);
        CliReportPrinter.print(System.out, lastResponse);
    }

    public SyncRunResponse lastResponse() {
        return lastResponse;
    }
}
