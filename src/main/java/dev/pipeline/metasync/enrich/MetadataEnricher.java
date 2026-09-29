package dev.pipeline.metasync.enrich;

import dev.pipeline.metasync.domain.AssetEvent;
import dev.pipeline.metasync.domain.EnrichedAsset;
import dev.pipeline.metasync.pipeline.ItemProcessor;

/**
 * Lookup step that stands in for a remote metadata service (classification, owner, lineage).
 * Implementations are called concurrently and must not share mutable state.
 */
public interface MetadataEnricher extends ItemProcessor<AssetEvent, EnrichedAsset> {
}
