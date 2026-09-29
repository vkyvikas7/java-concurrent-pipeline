package dev.pipeline.metasync.pipeline;

/**
 * Whether the sink emits successful items in input order.
 * <p>
 * {@link #UNORDERED} keeps auxiliary memory bounded by the queues.
 * {@link #ORDERED} adds a reorder buffer that can grow with the number of items
 * completed ahead of a slow earlier item.
 */
public enum OrderingMode {
    UNORDERED,
    ORDERED
}
