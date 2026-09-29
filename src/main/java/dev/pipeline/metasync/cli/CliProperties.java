package dev.pipeline.metasync.cli;

import dev.pipeline.metasync.domain.SyncMode;
import dev.pipeline.metasync.pipeline.OrderingMode;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "pipeline.cli")
public class CliProperties {

    private boolean enabled;
    private SyncMode mode = SyncMode.BOTH;
    private int items = 400;
    private int latencyMs = 15;
    private int workers = 8;
    private int queueCapacity = 32;
    private int batchSize = 50;
    private OrderingMode ordering = OrderingMode.UNORDERED;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public SyncMode getMode() {
        return mode;
    }

    public void setMode(SyncMode mode) {
        this.mode = mode;
    }

    public int getItems() {
        return items;
    }

    public void setItems(int items) {
        this.items = items;
    }

    public int getLatencyMs() {
        return latencyMs;
    }

    public void setLatencyMs(int latencyMs) {
        this.latencyMs = latencyMs;
    }

    public int getWorkers() {
        return workers;
    }

    public void setWorkers(int workers) {
        this.workers = workers;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public OrderingMode getOrdering() {
        return ordering;
    }

    public void setOrdering(OrderingMode ordering) {
        this.ordering = ordering;
    }
}
