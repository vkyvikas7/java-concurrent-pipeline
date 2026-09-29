package dev.pipeline.metasync.api;

import dev.pipeline.metasync.domain.SyncMode;
import dev.pipeline.metasync.pipeline.OrderingMode;
import dev.pipeline.metasync.service.MetadataSyncService;
import dev.pipeline.metasync.service.SyncRequest;
import dev.pipeline.metasync.service.SyncRunResponse;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/sync")
public class SyncController {

    static final int DEFAULT_LATENCY_MILLIS = 10;
    static final int DEFAULT_WORKERS = 8;
    static final int DEFAULT_BATCH_SIZE = 64;

    private final MetadataSyncService service;

    public SyncController(MetadataSyncService service) {
        this.service = service;
    }

    @GetMapping
    public Map<String, Object> describe() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("service", "metadata-sync");
        body.put("run", "POST /api/v1/sync/run");
        body.put("modes", List.of(SyncMode.SEQUENTIAL.name(), SyncMode.CONCURRENT.name(), SyncMode.BOTH.name()));
        body.put("ordering", List.of(OrderingMode.UNORDERED.name(), OrderingMode.ORDERED.name()));
        body.put("defaults", Map.of(
                "latencyMillis", DEFAULT_LATENCY_MILLIS,
                "workerCount", DEFAULT_WORKERS,
                "queueCapacity", "workerCount * 4",
                "batchSize", DEFAULT_BATCH_SIZE,
                "ordering", OrderingMode.UNORDERED.name()
        ));
        body.put("example", Map.of(
                "mode", "BOTH",
                "itemCount", 400,
                "latencyMillis", 15,
                "workerCount", 8,
                "queueCapacity", 32,
                "batchSize", 50,
                "ordering", "UNORDERED"
        ));
        return body;
    }

    @PostMapping("/run")
    public SyncRunResponse run(@Valid @RequestBody SyncRunRequest request) {
        return service.execute(resolve(request));
    }

    static SyncRequest resolve(SyncRunRequest request) {
        int workers = request.workerCount() == null ? DEFAULT_WORKERS : request.workerCount();
        int batchSize = request.batchSize() == null ? DEFAULT_BATCH_SIZE : request.batchSize();
        int queueCapacity = request.queueCapacity() == null ? workers * 4 : request.queueCapacity();
        int latency = request.latencyMillis() == null ? DEFAULT_LATENCY_MILLIS : request.latencyMillis();
        OrderingMode ordering = request.ordering() == null ? OrderingMode.UNORDERED : request.ordering();
        return new SyncRequest(
                request.mode(),
                request.itemCount(),
                latency,
                workers,
                queueCapacity,
                batchSize,
                ordering
        );
    }
}
